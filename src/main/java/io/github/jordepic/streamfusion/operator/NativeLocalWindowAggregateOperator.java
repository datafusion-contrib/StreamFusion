package io.github.jordepic.streamfusion.operator;

import io.github.jordepic.streamfusion.Native;
import java.util.List;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;

/**
 * Local half of two-phase window aggregation: pre-aggregates raw rows per window and, on a
 * watermark, emits each closed window's partial state as {@code [key?, partial, slice_end]},
 * matching the host's local-aggregate output so the downstream shuffle and global aggregate are
 * unaffected. The global half merges these partials.
 */
public class NativeLocalWindowAggregateOperator extends NativeWindowOperatorBase {

  private final int timeColumn;
  private final int valueColumn;
  private final int keyColumn;

  public NativeLocalWindowAggregateOperator(
      long windowMillis,
      int timeColumn,
      int valueColumn,
      int keyColumn,
      int aggregateKind,
      String timeZoneId,
      int batchSize) {
    super("streamfusion-local-window-state", windowMillis, aggregateKind, timeZoneId, batchSize);
    this.timeColumn = timeColumn;
    this.valueColumn = valueColumn;
    this.keyColumn = keyColumn;
  }

  @Override
  protected void pushBatch(List<RowData> rows) {
    updateRaw(rows, timeColumn, valueColumn, keyColumn);
  }

  @Override
  protected void emitClosedWindows(long watermark) {
    boolean keyed = keyColumn >= 0;
    try (ArrowArray array = ArrowArray.allocateNew(allocator);
        ArrowSchema schema = ArrowSchema.allocateNew(allocator)) {
      Native.flushPartialTumblingAggregator(
          handle, watermark, array.memoryAddress(), schema.memoryAddress());
      try (VectorSchemaRoot result =
          Data.importVectorSchemaRoot(allocator, array, schema, dictionaries)) {
        BigIntVector key = (BigIntVector) result.getVector("key");
        BigIntVector partial = (BigIntVector) result.getVector("partial");
        BigIntVector sliceEnd = (BigIntVector) result.getVector("slice_end");
        for (int i = 0; i < result.getRowCount(); i++) {
          // Local output column order follows the host: [key?, partial, slice_end].
          GenericRowData row;
          if (keyed) {
            row = new GenericRowData(3);
            row.setField(0, key.get(i));
            row.setField(1, partial.get(i));
            row.setField(2, sliceEnd.get(i));
          } else {
            row = new GenericRowData(2);
            row.setField(0, partial.get(i));
            row.setField(1, sliceEnd.get(i));
          }
          output.collect(new StreamRecord<>(row, sliceEnd.get(i)));
        }
      }
    }
  }
}
