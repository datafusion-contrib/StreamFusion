package io.github.jordepic.streamfusion.operator;

import io.github.jordepic.streamfusion.Native;
import java.util.List;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
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
  private final int[] keyColumns;
  private final int[] keyTypes;

  public NativeLocalWindowAggregateOperator(
      long windowMillis,
      long slideMillis,
      int timeColumn,
      int valueColumn,
      int[] keyColumns,
      int[] keyTypes,
      int valueType,
      int[] aggregateKinds,
      String timeZoneId,
      int batchSize) {
    super(
        "streamfusion-local-window-state",
        windowMillis,
        slideMillis,
        valueType,
        aggregateKinds,
        timeZoneId,
        batchSize);
    this.timeColumn = timeColumn;
    this.valueColumn = valueColumn;
    this.keyColumns = keyColumns;
    this.keyTypes = keyTypes;
  }

  @Override
  protected void pushBatch(List<RowData> rows) {
    updateRaw(rows, timeColumn, valueColumn, keyColumns, keyTypes);
  }

  @Override
  protected void emitClosedWindows(long watermark) {
    int keyCount = keyColumns.length;
    int aggregates = aggregateCount();
    try (ArrowArray array = ArrowArray.allocateNew(allocator);
        ArrowSchema schema = ArrowSchema.allocateNew(allocator)) {
      Native.flushPartialTumblingAggregator(
          handle, watermark, array.memoryAddress(), schema.memoryAddress());
      try (VectorSchemaRoot result =
          Data.importVectorSchemaRoot(allocator, array, schema, dictionaries)) {
        FieldVector[] keys = new FieldVector[keyCount];
        for (int j = 0; j < keyCount; j++) {
          keys[j] = (FieldVector) result.getVector("key" + j);
        }
        BigIntVector sliceEnd = (BigIntVector) result.getVector("slice_end");
        FieldVector[] partials = new FieldVector[aggregates];
        for (int a = 0; a < aggregates; a++) {
          partials[a] = (FieldVector) result.getVector("partial" + a);
        }
        for (int i = 0; i < result.getRowCount(); i++) {
          // Local output column order follows the host: [key0..key{n-1}, partial0.., slice_end].
          GenericRowData row = new GenericRowData(keyCount + aggregates + 1);
          int field = 0;
          for (int j = 0; j < keyCount; j++) {
            row.setField(field++, boxKey(keys[j], i, keyTypes[j]));
          }
          for (int a = 0; a < aggregates; a++) {
            row.setField(field++, readScalar(partials[a], i));
          }
          row.setField(field, sliceEnd.get(i));
          output.collect(new StreamRecord<>(row, sliceEnd.get(i)));
        }
      }
    }
  }
}
