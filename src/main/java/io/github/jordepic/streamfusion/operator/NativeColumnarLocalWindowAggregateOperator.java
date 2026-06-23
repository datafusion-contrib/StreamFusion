package io.github.jordepic.streamfusion.operator;

import io.github.jordepic.streamfusion.Native;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

/**
 * Columnar local half of two-phase window aggregation: the same per-slice pre-aggregate as {@link
 * NativeLocalWindowAggregateOperator}, but fed Arrow batches directly and emitting each closed
 * slice's partial state as an Arrow batch ({@code [key?, partial0.., slice_end]}) rather than rows.
 * That partial batch is exactly what the columnar global half consumes, so the shuffle between them
 * stays columnar — no row transpose on either side of the exchange.
 */
public class NativeColumnarLocalWindowAggregateOperator extends NativeWindowOperatorCore<ArrowBatch>
    implements OneInputStreamOperator<ArrowBatch, ArrowBatch> {

  private final int timeColumn;
  private final int[] valueColumns;
  private final int[] keyColumns;
  private final int[] keyTypes;

  public NativeColumnarLocalWindowAggregateOperator(
      long windowMillis,
      long slideMillis,
      int timeColumn,
      int[] valueColumns,
      int[] keyColumns,
      int[] keyTypes,
      int[] valueTypes,
      int[] aggregateKinds,
      String timeZoneId) {
    super(
        "streamfusion-local-window-state",
        windowMillis,
        slideMillis,
        valueTypes,
        aggregateKinds,
        timeZoneId);
    this.timeColumn = timeColumn;
    this.valueColumns = valueColumns;
    this.keyColumns = keyColumns;
    this.keyTypes = keyTypes;
  }

  @Override
  public void processElement(StreamRecord<ArrowBatch> element) {
    try (VectorSchemaRoot in = element.getValue().root()) {
      updateColumnar(in, timeColumn, valueColumns, keyColumns, keyTypes);
    }
  }

  @Override
  protected void flushPending() {
    // Each batch is folded into the aggregator as it arrives; nothing is buffered here.
  }

  @Override
  protected void emitClosedWindows(long watermark) {
    try (ArrowArray array = ArrowArray.allocateNew(allocator);
        ArrowSchema schema = ArrowSchema.allocateNew(allocator)) {
      Native.flushPartialTumblingAggregator(
          handle, watermark, array.memoryAddress(), schema.memoryAddress());
      VectorSchemaRoot partial = Data.importVectorSchemaRoot(allocator, array, schema, dictionaries);
      if (partial.getRowCount() > 0) {
        output.collect(new StreamRecord<>(new ArrowBatch(partial)));
      } else {
        partial.close(); // nothing closed this watermark; release the empty batch
      }
    }
  }
}
