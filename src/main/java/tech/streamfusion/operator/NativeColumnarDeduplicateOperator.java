package tech.streamfusion.operator;

import tech.streamfusion.Native;
import tech.streamfusion.planner.NativeConfig;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.runtime.operators.over.AbstractRowTimeUnboundedPrecedingOver;
import org.apache.flink.table.types.logical.RowType;

/**
 * Columnar append-only keep-first deduplication (`ROW_NUMBER() OVER (PARTITION BY … ORDER BY rowtime
 * ASC) = 1`): Arrow in, Arrow out. The Arrow-batch analog of the host's insert-only {@code
 * RowTimeDeduplicateKeepFirstRowFunction}. Each input batch is buffered natively; on a watermark the
 * deduplicator emits each key's minimum-rowtime row once the watermark reaches that rowtime, and
 * drops every later row for the key. Insert-only — the watermark guarantees no smaller-rowtime row
 * can still arrive once a key's row fires. Keys are co-located by the columnar shuffle; the per-key
 * candidate state and the late-data drop live in the native deduplicator, and this layer moves
 * batches across the bridge and owns the handle's checkpointed state. On the RocksDB backend the
 * candidates and fired markers live in the persistent store (write buffer + disk table) and the
 * watermark firing is a range read over both; memory state travels as raw keyed-state blobs.
 */
public class NativeColumnarDeduplicateOperator extends AbstractNativeStatefulOperator<ArrowBatch>
    implements OneInputStreamOperator<ArrowBatch, ArrowBatch> {

  private final int[] partitionColumns;
  private final int rowtimeColumn;
  private final RowType rowType;
  private final long stateTtlMillis;

  private transient Counter numLateRecordsDropped;
  private transient long reportedLateDrops;

  public NativeColumnarDeduplicateOperator(
      int[] partitionColumns,
      int[] keyTimestampPrecisions,
      int rowtimeColumn,
      RowType rowType,
      long stateTtlMillis,
      int maxParallelism) {
    super("keep-first deduplicate", keyTimestampPrecisions, maxParallelism);
    this.partitionColumns = partitionColumns;
    this.rowtimeColumn = rowtimeColumn;
    this.rowType = rowType;
    this.stateTtlMillis = stateTtlMillis;
  }

  @Override
  protected long createHandle() {
    return Native.createKeepFirstDeduplicator(
        partitionColumns,
        keyTimestampPrecisions(),
        rowtimeColumn,
        stateTtlMillis,
        memoryBudgetBytes());
  }

  @Override
  protected long restoreRawHandle(byte[][] snapshots) {
    return Native.restoreKeepFirstDeduplicatorPartitions(
        partitionColumns,
        keyTimestampPrecisions(),
        rowtimeColumn,
        stateTtlMillis,
        getProcessingTimeService().getCurrentProcessingTime(),
        snapshots,
        memoryBudgetBytes());
  }

  @Override
  protected byte[][] snapshotRawPartitions() {
    return Native.snapshotKeepFirstDeduplicatorPartitions(
        handle, maxParallelism(), keyTimestampPrecisions());
  }

  @Override
  protected void closeHandle() {
    Native.closeKeepFirstDeduplicator(handle);
  }

  @Override
  protected long stateBytesHandle() {
    return Native.keepFirstDeduplicatorStateBytes(handle);
  }

  @Override
  public void open() throws Exception {
    super.open();
    // Flink's RowTimeDeduplicateKeepFirstRowFunction counts every late-dropped row under this
    // exact name; the native late filter accumulates the total and each push syncs the delta.
    numLateRecordsDropped =
        getMetricGroup()
            .counter(AbstractRowTimeUnboundedPrecedingOver.LATE_ELEMENTS_DROPPED_METRIC_NAME);
    reportedLateDrops = 0;
  }

  @Override
  public void processElement(StreamRecord<ArrowBatch> element) {
    ColumnarRecordMetrics.countIngested(getMetricGroup(), element.getValue().rowCount());
    VectorSchemaRoot in = element.getValue().root();
    BufferAllocator inAllocator =
        in.getFieldVectors().isEmpty() ? allocator : in.getFieldVectors().get(0).getAllocator();
    try (ArrowArray array = ArrowArray.allocateNew(inAllocator);
        ArrowSchema schema = ArrowSchema.allocateNew(inAllocator)) {
      Data.exportVectorSchemaRoot(inAllocator, in, dictionaries, array, schema);
      // Flink's TtlTimeProvider clock: the processing-time service is System.currentTimeMillis
      // in production and harness-controlled in tests, so expiry is deterministic to test.
      Native.pushKeepFirstDeduplicator(
          handle,
          array.memoryAddress(),
          schema.memoryAddress(),
          getProcessingTimeService().getCurrentProcessingTime());
    } finally {
      in.close();
    }
    long lateDrops = Native.keepFirstDeduplicatorLateDrops(handle);
    numLateRecordsDropped.inc(lateDrops - reportedLateDrops);
    reportedLateDrops = lateDrops;
    publishStateBytes();
  }

  Counter numLateRecordsDropped() {
    return numLateRecordsDropped;
  }

  @Override
  public void processWatermark(Watermark mark) throws Exception {
    try (ArrowArray array = ArrowArray.allocateNew(allocator);
        ArrowSchema schema = ArrowSchema.allocateNew(allocator)) {
      Native.flushKeepFirstDeduplicator(
          handle,
          mark.getTimestamp(),
          getProcessingTimeService().getCurrentProcessingTime(),
          array.memoryAddress(),
          schema.memoryAddress());
      VectorSchemaRoot out = Data.importVectorSchemaRoot(allocator, array, schema, dictionaries);
      if (out.getRowCount() > 0) {
        ColumnarRecordMetrics.emit(output, getMetricGroup(), new ArrowBatch(out));
      } else {
        out.close(); // no key's first row was completed by this watermark
      }
    }
    publishStateBytes();
    super.processWatermark(mark);
  }

}
