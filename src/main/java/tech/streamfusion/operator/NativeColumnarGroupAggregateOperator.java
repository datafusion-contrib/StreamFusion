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

/**
 * Non-windowed {@code GROUP BY} aggregation, fed Arrow batches and emitting Arrow batches (the
 * native kernel reads/writes the row kind on the batch's {@code $row_kind$} column). A native
 * changelog chain pays no per-operator transpose; the row↔Arrow conversion happens only at the host
 * edges (inserted by the transition pass), and each keyed shuffle stays columnar where the input is a
 * columnar producer.
 */
public class NativeColumnarGroupAggregateOperator
    extends AbstractNativeStatefulOperator<ArrowBatch>
    implements OneInputStreamOperator<ArrowBatch, ArrowBatch> {

  private final int[] aggregateKinds;
  private final int[] valueTypes;
  private final int[] valueColumns;
  private final int[] keyColumns;
  private final int[] filterColumns;
  private final int[] countColumns;
  private final int[] distinctViewColumns;
  private final int recordCountColumn;
  private final boolean generateUpdateBefore;
  private final boolean miniBatch;
  private final long miniBatchSize;
  private final long stateTtlMillis;

  private transient MiniBatchBoundary boundary;
  private transient MiniBatchMetrics miniBatchMetrics;
  private transient BatchCoalescer coalescer;

  public NativeColumnarGroupAggregateOperator(
      int[] aggregateKinds,
      int[] valueTypes,
      int[] valueColumns,
      int[] keyColumns,
      int[] filterColumns,
      int[] countColumns,
      int[] distinctViewColumns,
      int recordCountColumn,
      boolean generateUpdateBefore,
      boolean miniBatch,
      long miniBatchSize,
      long stateTtlMillis,
      int[] keyTimestampPrecisions,
      int maxParallelism) {
    super("group aggregate", keyTimestampPrecisions, maxParallelism);
    this.aggregateKinds = aggregateKinds;
    this.valueTypes = valueTypes;
    this.valueColumns = valueColumns;
    this.keyColumns = keyColumns;
    this.filterColumns = filterColumns;
    this.countColumns = countColumns;
    this.distinctViewColumns = distinctViewColumns;
    this.recordCountColumn = recordCountColumn;
    this.generateUpdateBefore = generateUpdateBefore;
    this.miniBatch = miniBatch;
    this.miniBatchSize = miniBatchSize;
    this.stateTtlMillis = stateTtlMillis;
  }

  @Override
  protected boolean usesDirectRocksDBState() {
    return Native.rocksdbGroupAggregatorSupported(aggregateKinds, valueTypes);
  }

  @Override
  protected RocksDBNativeStateSupport resolveRocksDBState(boolean rawStateRestored) {
    return resolveRocksDB(
        rawStateRestored,
        () -> true,
        stateTtlMillis);
  }

  @Override
  protected long createRocksDBHandle(RocksDBNativeStateSupport rocksdb) {
    return Native.createRocksDBGroupAggregator(
        aggregateKinds, valueTypes, valueColumns, keyColumns, keyTimestampPrecisions(),
        filterColumns, countColumns, distinctViewColumns, recordCountColumn,
        generateUpdateBefore, miniBatch, stateTtlMillis,
        getProcessingTimeService().getCurrentProcessingTime(), memoryBudgetBytes(),
        rocksdb.tableDirectory(), maxParallelism(), rocksdb.optionsJson(),
        rocksdb.sourceDirectories(), rocksdb.sourceSnapshotTokens(),
        rocksdb.keyGroupStart(), rocksdb.keyGroupEnd(), rocksdb.aligned());
  }

  @Override
  protected String[] checkpointRocksDBHandle(String snapshotDirectory) {
    return directRocksDBState()
        ? Native.checkpointRocksDBGroupAggregator(handle, snapshotDirectory)
        : super.checkpointRocksDBHandle(snapshotDirectory);
  }

  @Override
  protected long createHandle() {
    return Native.createGroupAggregator(
        aggregateKinds, valueTypes, valueColumns, keyColumns, keyTimestampPrecisions(),
        filterColumns, countColumns, distinctViewColumns, recordCountColumn,
        generateUpdateBefore, miniBatch, stateTtlMillis, memoryBudgetBytes());
  }

  @Override
  protected long restoreRawHandle(byte[][] snapshots) {
    return Native.restoreGroupAggregatorPartitions(
        aggregateKinds, valueTypes, valueColumns, keyColumns, keyTimestampPrecisions(),
        filterColumns, countColumns, distinctViewColumns, recordCountColumn, generateUpdateBefore,
        miniBatch, stateTtlMillis, getProcessingTimeService().getCurrentProcessingTime(),
        snapshots, memoryBudgetBytes());
  }

  @Override
  protected byte[][] snapshotRawPartitions() {
    return Native.snapshotGroupAggregatorPartitions(
        handle, maxParallelism(), keyTimestampPrecisions());
  }

  @Override
  protected void closeHandle() {
    if (directRocksDBState()) {
      Native.closeRocksDBGroupAggregator(handle);
    } else {
      Native.closeGroupAggregator(handle);
    }
  }

  @Override
  protected long stateBytesHandle() {
    return directRocksDBState()
        ? Native.rocksdbGroupAggregatorStateBytes(handle)
        : Native.groupAggregatorStateBytes(handle);
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
        update(in);
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
        update(in);
      } else {
        int offset = 0;
        while (offset < rows) {
          boolean firstContribution = offset == 0 || boundary.bufferedRows() == 0;
          int length = boundary.nextSliceLength(rows - offset);
          if (length < rows - offset) {
            miniBatchMetrics.onPhysicalBatchSplit();
          }
          if (offset == 0 && length == rows) {
            update(in);
          } else {
            try (VectorSchemaRoot slice = in.slice(offset, length)) {
              update(slice);
            }
          }
          miniBatchMetrics.onSlice(length, firstContribution);
          miniBatchMetrics.onCurrentKeys(
              directRocksDBState()
                  ? Native.rocksdbGroupAggregatorStagedKeys(handle)
                  : Native.groupAggregatorStagedKeys(handle));
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

  private void update(VectorSchemaRoot in) {
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
        Native.updateRocksDBGroupAggregator(
            handle,
            inArray.memoryAddress(),
            inSchema.memoryAddress(),
            now,
            outArray.memoryAddress(),
            outSchema.memoryAddress());
      } else {
        Native.updateGroupAggregator(
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
            ? Native.rocksdbGroupAggregatorStagingBytes(handle)
            : Native.groupAggregatorStagingBytes(handle);
    long touchedKeys =
        directRocksDBState()
            ? Native.rocksdbGroupAggregatorStagedKeys(handle)
            : Native.groupAggregatorStagedKeys(handle);
    try (ArrowArray outArray = ArrowArray.allocateNew(allocator);
        ArrowSchema outSchema = ArrowSchema.allocateNew(allocator)) {
      if (directRocksDBState()) {
        Native.flushRocksDBGroupAggregator(
            handle, outArray.memoryAddress(), outSchema.memoryAddress());
      } else {
        Native.flushGroupAggregator(handle, outArray.memoryAddress(), outSchema.memoryAddress());
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
