package tech.streamfusion.operator;

import tech.streamfusion.Native;
import tech.streamfusion.planner.NativeConfig;
import tech.streamfusion.state.PaimonNativeStateSupport;
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
  protected PaimonNativeStateSupport resolvePaimonState(
      boolean rawStateRestored) {
    return resolvePaimon(
        rawStateRestored,
        () -> Native.paimonWindowAggStateSupported(valueTypes, aggregateKinds, keyTypes));
  }

  @Override
  protected long createPaimonHandle(
      PaimonNativeStateSupport paimon) {
    return Native.createPaimonTumblingAggregator(
        windowMillis,
        slideMillis,
        cumulative,
        valueTypes,
        aggregateKinds,
        keyTypes,
        keyTimestampPrecisions(),
        memoryBudgetBytes(),
        paimon.tableDirectory(),
        maxParallelism(),
        NativeConfig.paimonBuckets(),
        NativeConfig.paimonFileFormat(),
        NativeConfig.paimonFileCompression(),
        paimon.sourceDirectories(),
        paimon.sourceSnapshotTokens(),
        paimon.keyGroupStart(),
        paimon.keyGroupEnd(),
        paimon.aligned());
  }

  @Override
  protected String[] checkpointPaimonHandle() {
    return Native.checkpointPaimonTumblingAggregator(handle);
  }

  // Cumulative globals merge each slice into the nested windows of its bucket; see the row-fed
  // operator. The native side switches fan-out on the cumulative flag set here.
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
      Native.updatePartialTumblingAggregator(handle, array.memoryAddress(), schema.memoryAddress());
    } finally {
      in.close(); // the partial batch is consumed
    }
    publishStateBytes();
  }

  @Override
  protected void flushPending() {
    // Each partial batch is folded into the aggregator as it arrives; nothing is buffered here.
  }

  @Override
  protected void emitClosedWindows(long watermark) {
    emitFinal(watermark, keyTypes);
  }
}
