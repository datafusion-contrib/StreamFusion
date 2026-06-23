package io.github.jordepic.streamfusion.operator;

import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;

/**
 * The {@link RowData}-emitting layer of the window operator core: adds {@link #emitFinal}, which
 * fetches the windows a watermark has closed and emits each as a result row. Window operators that
 * produce final per-window rows (single-phase, global, session) extend this; the partial-emitting
 * local operator extends the output-agnostic {@link NativeWindowOperatorCore} directly (it emits
 * Arrow partial batches, not rows).
 */
public abstract class NativeRowWindowOperatorCore extends NativeWindowOperatorCore<RowData> {

  protected NativeRowWindowOperatorCore(
      String stateName,
      long windowMillis,
      long slideMillis,
      int[] valueTypes,
      int[] aggregateKinds,
      String timeZoneId) {
    super(stateName, windowMillis, slideMillis, valueTypes, aggregateKinds, timeZoneId);
  }

  /**
   * Emits the final per-window rows the watermark has closed:
   * {@code [key?, agg0..aggN-1, window_start, window_end]} — the host's column order. The flush
   * carries the window end explicitly (windows of the same start can differ by it), so every window
   * operator — single-phase, global, and session — shares this path.
   */
  protected final void emitFinal(long watermark, int[] keyTypes) {
    int keyCount = keyTypes.length;
    int aggregates = aggregateCount();
    try (ArrowArray array = ArrowArray.allocateNew(allocator);
        ArrowSchema schema = ArrowSchema.allocateNew(allocator)) {
      flushHandle(watermark, array.memoryAddress(), schema.memoryAddress());
      try (VectorSchemaRoot result =
          Data.importVectorSchemaRoot(allocator, array, schema, dictionaries)) {
        FieldVector[] keys = new FieldVector[keyCount];
        for (int j = 0; j < keyCount; j++) {
          keys[j] = (FieldVector) result.getVector("key" + j);
        }
        BigIntVector windowStart = (BigIntVector) result.getVector("window_start");
        BigIntVector windowEnd = (BigIntVector) result.getVector("window_end");
        FieldVector[] results = new FieldVector[aggregates];
        for (int a = 0; a < aggregates; a++) {
          results[a] = (FieldVector) result.getVector("result" + a);
        }
        for (int i = 0; i < result.getRowCount(); i++) {
          long start = windowStart.get(i);
          long end = windowEnd.get(i);
          GenericRowData row = new GenericRowData(keyCount + aggregates + 2);
          int field = 0;
          for (int j = 0; j < keyCount; j++) {
            row.setField(field++, boxKey(keys[j], i, keyTypes[j]));
          }
          for (int a = 0; a < aggregates; a++) {
            row.setField(field++, readScalar(results[a], i));
          }
          row.setField(field++, TimestampData.fromLocalDateTime(toLocal(start)));
          row.setField(field, TimestampData.fromLocalDateTime(toLocal(end)));
          output.collect(new StreamRecord<>(row, start));
        }
      }
    }
  }
}
