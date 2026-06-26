package io.github.jordepic.streamfusion.operator;

import io.github.jordepic.streamfusion.Native;
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
 * ({@code [key?, agg…, window_start, window_end]}). Arrow → Arrow (ticket 36); a rowwise sink is
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
      RowType outputType) {
    super(
        "streamfusion-global-window-state",
        windowMillis,
        slideMillis,
        valueTypes,
        aggregateKinds,
        timeZoneId,
        outputType);
    this.cumulative = cumulative;
    this.keyTypes = keyTypes;
  }

  // Cumulative globals merge each slice into the nested windows of its bucket; see the row-fed
  // operator. The native side switches fan-out on the cumulative flag set here.
  @Override
  protected long createHandle() {
    return cumulative
        ? Native.createCumulativeAggregator(windowMillis, slideMillis, valueTypes, aggregateKinds)
        : super.createHandle();
  }

  @Override
  protected long restoreHandle(byte[] snapshot) {
    return cumulative
        ? Native.restoreCumulativeAggregator(
            windowMillis, slideMillis, valueTypes, aggregateKinds, snapshot)
        : super.restoreHandle(snapshot);
  }

  @Override
  public void processElement(StreamRecord<ArrowBatch> element) {
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
