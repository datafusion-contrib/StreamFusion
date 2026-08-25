package tech.streamfusion.operator;

import tech.streamfusion.Native;
import tech.streamfusion.operator.MiniBatchMetrics.FlushReason;
import tech.streamfusion.planner.NativeConfig;
import tech.streamfusion.state.RocksDBNativeStateSupport;
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
 * Changelog normalization (Flink's {@code ChangelogNormalize}), fed Arrow batches and emitting Arrow
 * batches. Keeps the last full row per unique key and turns an upsert/duplicate-bearing changelog
 * into a regular INSERT/UPDATE_BEFORE/UPDATE_AFTER/DELETE changelog (the row kind read and written on
 * the batch's {@code $row_kind$} column). With mini-batch disabled it emits synchronously per input
 * batch. With mini-batch enabled it emits the first-preimage/final-postimage transition at the
 * logical count/watermark/checkpoint/end boundary. Columnar in and out, so it pays no per-operator
 * transpose; the keyed shuffle stays columnar where the input is a columnar producer.
 */
public class NativeColumnarChangelogNormalizeOperator
    extends AbstractNativeStatefulOperator<ArrowBatch>
    implements OneInputStreamOperator<ArrowBatch, ArrowBatch> {

  private final int[] keyColumns;
  private final RowType rowType;
  private final boolean generateUpdateBefore;
  private final boolean miniBatch;
  private final long miniBatchSize;
  private final long stateTtlMillis;

  private transient MiniBatchBoundary boundary;
  private transient MiniBatchMetrics miniBatchMetrics;
  private transient BatchCoalescer coalescer;

  public NativeColumnarChangelogNormalizeOperator(
      int[] keyColumns,
      int[] keyTimestampPrecisions,
      RowType rowType,
      boolean generateUpdateBefore,
      boolean miniBatch,
      long miniBatchSize,
      long stateTtlMillis,
      int maxParallelism) {
    super("changelog normalize", keyTimestampPrecisions, maxParallelism);
    this.keyColumns = keyColumns;
    this.rowType = rowType;
    this.generateUpdateBefore = generateUpdateBefore;
    this.miniBatch = miniBatch;
    this.miniBatchSize = miniBatchSize;
    this.stateTtlMillis = stateTtlMillis;
  }

  @Override
  protected boolean usesDirectRocksDBState() {
    return true;
  }

  @Override
  protected RocksDBNativeStateSupport resolveRocksDBState(boolean rawStateRestored) {
    return resolveRocksDB(rawStateRestored, () -> true, stateTtlMillis);
  }

  @Override
  protected long createRocksDBHandle(RocksDBNativeStateSupport rocksdb) {
    return Native.createRocksDBChangelogNormalizer(
        keyColumns, keyTimestampPrecisions(), generateUpdateBefore, miniBatch, stateTtlMillis,
        getProcessingTimeService().getCurrentProcessingTime(), memoryBudgetBytes(),
        rocksdb.tableDirectory(), maxParallelism(), rocksdb.optionsJson(),
        rocksdb.sharedResourcesHandle(), rocksdb.sourceDirectories(),
        rocksdb.sourceSnapshotTokens(), rocksdb.keyGroupStart(), rocksdb.keyGroupEnd(),
        rocksdb.aligned());
  }

  @Override
  protected String[] checkpointRocksDBHandle(String snapshotDirectory) {
    return directRocksDBState()
        ? Native.checkpointRocksDBChangelogNormalizer(handle, snapshotDirectory)
        : super.checkpointRocksDBHandle(snapshotDirectory);
  }

  @Override
  protected long createHandle() {
    return Native.createChangelogNormalizer(
        keyColumns,
        keyTimestampPrecisions(),
        generateUpdateBefore,
        miniBatch,
        stateTtlMillis,
        memoryBudgetBytes());
  }

  @Override
  protected long restoreRawHandle(byte[][] snapshots) {
    return Native.restoreChangelogNormalizerPartitions(
        keyColumns,
        keyTimestampPrecisions(),
        generateUpdateBefore,
        miniBatch,
        stateTtlMillis,
        getProcessingTimeService().getCurrentProcessingTime(),
        snapshots,
        memoryBudgetBytes());
  }

  @Override
  protected byte[][] snapshotRawPartitions() {
    return Native.snapshotChangelogNormalizerPartitions(
        handle, maxParallelism(), keyTimestampPrecisions());
  }

  @Override
  protected byte[][] snapshotCanonicalPartitions() {
    return directRocksDBState()
        ? Native.snapshotRocksDBChangelogNormalizerPartitions(handle)
        : snapshotRawPartitions();
  }

  @Override
  protected void closeHandle() {
    if (directRocksDBState()) {
      Native.closeRocksDBChangelogNormalizer(handle);
    } else {
      Native.closeChangelogNormalizer(handle);
    }
  }

  @Override
  protected long stateBytesHandle() {
    return directRocksDBState()
        ? Native.rocksdbChangelogNormalizerStateBytes(handle)
        : Native.changelogNormalizerStateBytes(handle);
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
      if (rows == 0) {
        push(in);
      } else {
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
          miniBatchMetrics.onCurrentKeys(
              directRocksDBState()
                  ? Native.rocksdbChangelogNormalizerStagedKeys(handle)
                  : Native.changelogNormalizerStagedKeys(handle));
          offset += length;
          if (boundary.onSlice(length)) {
            flushBundle(FlushReason.COUNT);
          }
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
      if (directRocksDBState()) {
        Native.pushRocksDBChangelogNormalizer(
            handle,
            inArray.memoryAddress(),
            inSchema.memoryAddress(),
            now,
            outArray.memoryAddress(),
            outSchema.memoryAddress());
      } else {
        Native.pushChangelogNormalizer(
            handle,
            inArray.memoryAddress(),
            inSchema.memoryAddress(),
            now,
            outArray.memoryAddress(),
            outSchema.memoryAddress());
      }
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
    long transientBytes =
        directRocksDBState()
            ? Native.rocksdbChangelogNormalizerStagingBytes(handle)
            : Native.changelogNormalizerStagingBytes(handle);
    long touchedKeys =
        directRocksDBState()
            ? Native.rocksdbChangelogNormalizerStagedKeys(handle)
            : Native.changelogNormalizerStagedKeys(handle);
    try (ArrowArray outArray = ArrowArray.allocateNew(allocator);
        ArrowSchema outSchema = ArrowSchema.allocateNew(allocator)) {
      if (directRocksDBState()) {
        Native.flushRocksDBChangelogNormalizer(
            handle, outArray.memoryAddress(), outSchema.memoryAddress());
      } else {
        Native.flushChangelogNormalizer(
            handle, outArray.memoryAddress(), outSchema.memoryAddress());
      }
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
