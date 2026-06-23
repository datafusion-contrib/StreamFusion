package io.github.jordepic.streamfusion.operator;

import io.github.jordepic.streamfusion.Native;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.RowData;

/**
 * Columnar global half of two-phase window aggregation: the same merge as {@link
 * NativeGlobalWindowAggregateOperator}, but fed the partial-state Arrow batches the columnar local
 * half emits directly — no row→Arrow rebuild. Each incoming batch ({@code [key?, partial0..,
 * slice_end]}) is folded straight into the aggregator; on a watermark the final per-window rows are
 * emitted, matching the host.
 */
public class NativeColumnarGlobalWindowAggregateOperator extends NativeRowWindowOperatorCore
    implements OneInputStreamOperator<ArrowBatch, RowData> {

  private final int[] keyTypes;
  private final boolean cumulative;

  public NativeColumnarGlobalWindowAggregateOperator(
      long windowMillis,
      long slideMillis,
      boolean cumulative,
      int[] keyTypes,
      int[] valueTypes,
      int[] aggregateKinds,
      String timeZoneId) {
    super(
        "streamfusion-global-window-state",
        windowMillis,
        slideMillis,
        valueTypes,
        aggregateKinds,
        timeZoneId);
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
