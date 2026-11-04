package tech.streamfusion.operator;

import tech.streamfusion.Native;
import tech.streamfusion.planner.NativeConfig;
import tech.streamfusion.state.RocksDBNativeStateSupport;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.types.logical.RowType;

/**
 * Columnar global half of two-phase window aggregation: the same merge as {@link
 * NativeGlobalWindowAggregateOperator}, but fed the partial-state Arrow batches the columnar local
 * half emits directly — no row→Arrow rebuild — and emitting the final per-window results as Arrow
 * ({@code [key?, agg…, window_start, window_end]}). Arrow → Arrow; a rowwise sink is
 * reached through the dedicated {@code ArrowToRowDataOperator} at the island perimeter.
 */
public class NativeColumnarGlobalWindowAggregateOperator extends NativeRowWindowOperatorCore
    implements OneInputStreamOperator<ArrowBatch, ArrowBatch> {

  private final int[] keyTypes;
  private final boolean cumulative;
  private boolean stockPartialBuffer;
  private transient long bufferProgress;
  private transient long nextBufferProgress;

  public void useStockGlobalBuffering() {
    if (handle != 0)
      throw new IllegalStateException("Buffer mode must be bound before initialization");
    stockPartialBuffer = true;
  }

  @Override
  protected void configureStateHandle() {
    super.configureStateHandle();
    Native.setStockWindowPartialBuffer(handle, stockPartialBuffer);
  }

  @Override
  public void open() throws Exception {
    super.open();
    bufferProgress = canonicalWatermark();
    nextBufferProgress = Long.MIN_VALUE;
  }



  public NativeColumnarGlobalWindowAggregateOperator(
      long windowMillis,
      long slideMillis,
      boolean cumulative,
      int[] keyTypes,
      int[] valueTypes,
      int[] aggregateKinds,
      String timeZoneId,
      RowType outputType,
      int[] keyTimestampPrecisions,
      int maxParallelism) {
    this(
        windowMillis, slideMillis, cumulative, keyTypes, valueTypes, aggregateKinds, timeZoneId,
        !"UTC".equals(timeZoneId), timeZoneId, outputType, keyTimestampPrecisions, maxParallelism);
  }

  public NativeColumnarGlobalWindowAggregateOperator(
      long windowMillis,
      long slideMillis,
      boolean cumulative,
      int[] keyTypes,
      int[] valueTypes,
      int[] aggregateKinds,
      String timeZoneId,
      boolean timestampLtz,
      String sessionTimeZoneId,
      RowType outputType,
      int[] keyTimestampPrecisions,
      int maxParallelism) {
    super(
        "global window aggregate",
        windowMillis,
        slideMillis,
        valueTypes,
        aggregateKinds,
        timeZoneId,
        timestampLtz,
        sessionTimeZoneId,
        outputType,
        keyTimestampPrecisions,
        maxParallelism);
    this.cumulative = cumulative;
    this.keyTypes = keyTypes;
  }

  @Override
  protected boolean usesDirectRocksDBState() {
    return Native.rocksdbWindowAggregatorSupported(valueTypes, aggregateKinds, keyTypes);
  }

  @Override
  protected long createRocksDBHandle(
      RocksDBNativeStateSupport rocksdb, byte[][] restoredPartitions) {
    return createRocksDBWindowAggregatorHandle(rocksdb, cumulative, keyTypes, restoredPartitions);
  }

  @Override
  protected long createHandle() {
    return cumulative
        ? Native.createCumulativeAggregator(
            windowMillis, slideMillis, valueTypes, aggregateKinds, memoryBudgetBytes())
        : super.createHandle();
  }

  @Override
  protected long restoreHandle(byte[] snapshot) {
    return cumulative
        ? Native.restoreCumulativeAggregator(
            windowMillis, slideMillis, valueTypes, aggregateKinds, snapshot, memoryBudgetBytes())
        : super.restoreHandle(snapshot);
  }

  @Override
  protected long restoreRawHandle(byte[][] snapshots) {
    return Native.restoreTumblingAggregatorPartitions(
        windowMillis,
        slideMillis,
        cumulative,
        valueTypes,
        aggregateKinds,
        snapshots,
        memoryBudgetBytes());
  }

  @Override
  public void processElement(StreamRecord<ArrowBatch> element) {
    ColumnarRecordMetrics.countIngested(getMetricGroup(), element.getValue().rowCount());
    VectorSchemaRoot in = element.getValue().root();
    // The partial batch's buffers belong to the upstream allocator; export with that allocator (C
    // Data buffers associate only within one allocator root), then fold it into the aggregator.
    BufferAllocator inAllocator =
        in.getFieldVectors().isEmpty() ? allocator : in.getFieldVectors().get(0).getAllocator();
    try (ArrowArray array = ArrowArray.allocateNew(inAllocator);
        ArrowSchema schema = ArrowSchema.allocateNew(inAllocator)) {
      Data.exportVectorSchemaRoot(inAllocator, in, dictionaries, array, schema);
      if (directRocksDBState()) {
        Native.pushPartialRocksDBWindowAggregator(
            handle, array.memoryAddress(), schema.memoryAddress());
      } else {
        Native.updatePartialTumblingAggregator(
            handle, array.memoryAddress(), schema.memoryAddress());
      }
    } finally {
      in.close(); // the partial batch is consumed
    }
    publishStateBytes();
  }

  @Override
  protected void flushPending() {
    if (stockPartialBuffer) Native.flushStockWindowPartials(handle, Long.MIN_VALUE, true);
  }

  @Override
  public void prepareSnapshotPreBarrier(long checkpointId) throws Exception {
    flushPending();
    super.prepareSnapshotPreBarrier(checkpointId);
  }

  @Override
  protected void flushBeforeWatermark(long watermark) {
    if (!stockPartialBuffer) {
      super.flushBeforeWatermark(watermark);
      return;
    }
    if (watermark <= bufferProgress) return;
    bufferProgress = watermark;
    if (watermark < nextBufferProgress) return;
    Native.flushStockWindowPartials(handle, watermark, false);
    nextBufferProgress =
        org.apache.flink.table.runtime.util.TimeWindowUtil.getNextTriggerWatermark(
            watermark,
            slideMillis,
            windowShiftZone(),
            java.util.TimeZone.getTimeZone(windowShiftZone()).useDaylightTime());
  }

  @Override
  protected void emitClosedWindows(long watermark) {
    emitFinal(watermark, keyTypes);
  }
}
