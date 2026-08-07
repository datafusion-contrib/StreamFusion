package tech.streamfusion.operator;

import tech.streamfusion.Native;
import tech.streamfusion.operator.MiniBatchMetrics.FlushReason;
import tech.streamfusion.planner.NativeConfig;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.types.logical.RowType;

/**
 * Columnar eager (push→emit) deduplication: Arrow in, Arrow out. Serves the non-buffered dedup
 * variants — rowtime keep-last ({@code RowTimeDeduplicateFunction}), proctime keep-last ({@code
 * ProcTimeDeduplicateKeepLastRowFunction}), proctime keep-first ({@code
 * ProcTimeDeduplicateKeepFirstRowFunction}), and rowtime keep-first under mini-batch (Flink's
 * bundled {@code RowTimeMiniBatchDeduplicateFunction} with {@code keepLastRow=false} — keep-last's
 * retracting machinery with the comparator flipped). Keep-last keeps the winning row per key and
 * emits a retract changelog eagerly on each input batch ({@code +I} for a key's first row, {@code
 * -U}(previous)/{@code +U}(new) on replacement — the kind rides the {@code $row_kind$} column;
 * with {@code generateUpdateBefore} and {@code generateInsert} both false every emission is a bare
 * {@code +U}, Flink's insert-sensitivity option off under an only-update-after consumer);
 * proctime keep-first emits each key's first row ({@code +I}, insert-only) and drops the rest. A
 * rowtime order keeps the max-rowtime (keep-last) or min-rowtime (keep-first) row; proctime uses
 * arrival order. Insert-only input. Keys are co-located by the columnar shuffle; the per-key stored
 * row and the checkpointed handle state live here. (Rowtime keep-first without mini-batch is
 * watermark-buffered — see {@link NativeColumnarDeduplicateOperator}.)
 */
public class NativeColumnarKeepLastDeduplicateOperator
    extends AbstractNativeStatefulOperator<ArrowBatch>
    implements OneInputStreamOperator<ArrowBatch, ArrowBatch> {

  private final int[] partitionColumns;
  private final int rowtimeColumn;
  private final RowType rowType;
  private final boolean generateUpdateBefore;
  private final boolean generateInsert;
  private final boolean rowtimeOrdered;
  private final boolean keepFirst;
  private final boolean miniBatch;
  private final boolean compactChanges;
  private final long miniBatchSize;
  private final long stateTtlMillis;

  private transient MiniBatchBoundary boundary;
  private transient MiniBatchMetrics miniBatchMetrics;
  private transient BatchCoalescer coalescer;

  public NativeColumnarKeepLastDeduplicateOperator(
      int[] partitionColumns,
      int[] keyTimestampPrecisions,
      int rowtimeColumn,
      RowType rowType,
      boolean generateUpdateBefore,
      boolean generateInsert,
      boolean rowtimeOrdered,
      boolean keepFirst,
      boolean miniBatch,
      boolean compactChanges,
      long miniBatchSize,
      long stateTtlMillis,
      int maxParallelism) {
    super("keep-last deduplicate", keyTimestampPrecisions, maxParallelism);
    this.partitionColumns = partitionColumns;
    this.rowtimeColumn = rowtimeColumn;
    this.rowType = rowType;
    this.generateUpdateBefore = generateUpdateBefore;
    this.generateInsert = generateInsert;
    this.rowtimeOrdered = rowtimeOrdered;
    this.keepFirst = keepFirst;
    // Proctime keep-first emits eagerly even under mini-batch (same insert-only rows either way);
    // rowtime keep-first is the mini-batch bundled retracting shape, so it buffers like keep-last.
    this.miniBatch = miniBatch && (rowtimeOrdered || !keepFirst);
    this.compactChanges = compactChanges;
    this.miniBatchSize = miniBatchSize;
    this.stateTtlMillis = stateTtlMillis;
  }

  @Override
  protected long createHandle() {
    return Native.createKeepLastDeduplicator(
        partitionColumns,
        keyTimestampPrecisions(),
        rowtimeColumn,
        generateUpdateBefore,
        generateInsert,
        rowtimeOrdered,
        keepFirst,
        miniBatch,
        compactChanges,
        stateTtlMillis,
        memoryBudgetBytes());
  }

  @Override
  protected long restoreRawHandle(byte[][] snapshots) {
    return Native.restoreKeepLastDeduplicatorPartitions(
        partitionColumns,
        keyTimestampPrecisions(),
        rowtimeColumn,
        generateUpdateBefore,
        generateInsert,
        rowtimeOrdered,
        keepFirst,
        miniBatch,
        compactChanges,
        stateTtlMillis,
        getProcessingTimeService().getCurrentProcessingTime(),
        snapshots,
        memoryBudgetBytes());
  }

  @Override
  protected byte[][] snapshotRawPartitions() {
    return Native.snapshotKeepLastDeduplicatorPartitions(
        handle, maxParallelism(), keyTimestampPrecisions());
  }

  @Override
  protected void closeHandle() {
    Native.closeKeepLastDeduplicator(handle);
  }

  @Override
  protected long stateBytesHandle() {
    return Native.keepLastDeduplicatorStateBytes(handle);
  }

  @Override
  public void open() throws Exception {
    super.open();
    if (miniBatch) {
      boundary = new MiniBatchBoundary(miniBatchSize);
      miniBatchMetrics = new MiniBatchMetrics(getMetricGroup(), true);
    }
    coalescer = BatchCoalescer.create(getProcessingTimeService(), this::ingest);
  }

  @Override
  public void processElement(StreamRecord<ArrowBatch> element) {
    ColumnarRecordMetrics.countIngested(getMetricGroup(), element.getValue().rowCount());
    VectorSchemaRoot in = element.getValue().root();
    if (coalescer != null) {
      coalescer.add(in);
    } else {
      ingest(in);
    }
  }

  private void ingest(VectorSchemaRoot in) {
    if (!miniBatch) {
      try {
        push(in);
      } finally {
        in.close();
      }
      publishStateBytes();
      return;
    }
    int rows = in.getRowCount();
    miniBatchMetrics.onPhysicalBatch();
    try {
      int offset = 0;
      while (offset < rows) {
        boolean firstContribution = offset == 0 || boundary.bufferedRows() == 0;
        int length = boundary.nextSliceLength(rows - offset);
        if (length < rows - offset) {
          miniBatchMetrics.onPhysicalBatchSplit();
        }
        if (offset == 0 && length == rows) {
          push(in);
        } else {
          try (VectorSchemaRoot slice = in.slice(offset, length)) {
            push(slice);
          }
        }
        miniBatchMetrics.onSlice(length, firstContribution);
        miniBatchMetrics.onCurrentKeys(Native.keepLastDeduplicatorStagedKeys(handle));
        offset += length;
        if (boundary.onSlice(length)) {
          flushBundle(FlushReason.COUNT);
        }
      }
    } finally {
      in.close();
    }
    publishStateBytes();
  }

  private void push(VectorSchemaRoot in) {
    BufferAllocator inAllocator =
        in.getFieldVectors().isEmpty() ? allocator : in.getFieldVectors().get(0).getAllocator();
    try (ArrowArray inArray = ArrowArray.allocateNew(inAllocator);
        ArrowSchema inSchema = ArrowSchema.allocateNew(inAllocator);
        ArrowArray outArray = ArrowArray.allocateNew(allocator);
        ArrowSchema outSchema = ArrowSchema.allocateNew(allocator)) {
      Data.exportVectorSchemaRoot(inAllocator, in, dictionaries, inArray, inSchema);
      // Flink's TtlTimeProvider clock: the processing-time service is System.currentTimeMillis in
      // production and harness-controlled in tests, so expiry is deterministic to test.
      long now = getProcessingTimeService().getCurrentProcessingTime();
      Native.pushKeepLastDeduplicator(
          handle,
          inArray.memoryAddress(),
          inSchema.memoryAddress(),
          now,
          outArray.memoryAddress(),
          outSchema.memoryAddress());
      VectorSchemaRoot out =
          Data.importVectorSchemaRoot(allocator, outArray, outSchema, dictionaries);
      if (out.getRowCount() > 0) {
        ColumnarRecordMetrics.emit(output, getMetricGroup(), new ArrowBatch(out));
      } else {
        out.close();
      }
    }
  }

  @Override
  public void processWatermark(Watermark mark) throws Exception {
    if (coalescer != null) {
      coalescer.flush();
    }
    if (miniBatch) {
      flushBundle(FlushReason.WATERMARK);
      publishStateBytes();
    }
    super.processWatermark(mark);
  }

  @Override
  public void prepareSnapshotPreBarrier(long checkpointId) throws Exception {
    if (coalescer != null) {
      coalescer.flush();
    }
    if (miniBatch) {
      flushBundle(FlushReason.CHECKPOINT);
    }
    super.prepareSnapshotPreBarrier(checkpointId);
  }

  @Override
  public void finish() throws Exception {
    if (coalescer != null) {
      coalescer.flush();
    }
    if (miniBatch) {
      flushBundle(FlushReason.FINISH);
    }
    super.finish();
  }

  private void flushBundle(FlushReason reason) {
    long transientBytes = Native.keepLastDeduplicatorStagingBytes(handle);
    long touchedKeys = Native.keepLastDeduplicatorStagedKeys(handle);
    try (ArrowArray outArray = ArrowArray.allocateNew(allocator);
        ArrowSchema outSchema = ArrowSchema.allocateNew(allocator)) {
      Native.flushKeepLastDeduplicator(
          handle, outArray.memoryAddress(), outSchema.memoryAddress());
      VectorSchemaRoot out =
          Data.importVectorSchemaRoot(allocator, outArray, outSchema, dictionaries);
      int outputRows = out.getRowCount();
      miniBatchMetrics.onFlush(reason, outputRows, touchedKeys, transientBytes);
      if (outputRows > 0) {
        ColumnarRecordMetrics.emit(output, getMetricGroup(), new ArrowBatch(out));
      } else {
        out.close();
      }
    }
    boundary.reset();
  }

  @Override
  public void close() throws Exception {
    if (coalescer != null) {
      coalescer.close();
      coalescer = null;
    }
    super.close();
  }
}
