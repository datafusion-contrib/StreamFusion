package io.github.jordepic.streamfusion.bridge;

import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;

/**
 * Converts between the host engine's row-at-a-time representation and the columnar layout native
 * operators consume. The engine plans over rows, so a buffered run of rows must be transposed into
 * a column before it crosses to native code and transposed back on the way out.
 *
 * <p>Scoped to a single int32 column, the shape the first native operators accept; wider schemas
 * and more types extend the same transpose.
 */
public final class IntColumnBridge {

  private IntColumnBridge() {}

  /** Transposes one int field across the buffered rows into a column native code can read. */
  public static IntVector toArrow(List<RowData> rows, int field, BufferAllocator allocator) {
    IntVector vector = new IntVector("c" + field, allocator);
    vector.allocateNew(rows.size());
    for (int i = 0; i < rows.size(); i++) {
      vector.set(i, rows.get(i).getInt(field));
    }
    vector.setValueCount(rows.size());
    return vector;
  }

  /** Transposes a column produced by native code back into single-field rows. */
  public static List<RowData> fromArrow(IntVector vector) {
    List<RowData> rows = new ArrayList<>(vector.getValueCount());
    for (int i = 0; i < vector.getValueCount(); i++) {
      GenericRowData row = new GenericRowData(1);
      row.setField(0, vector.get(i));
      rows.add(row);
    }
    return rows;
  }
}
