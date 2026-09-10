package tech.streamfusion.arrow;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Collections;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntervalDayVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.IntervalUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.flink.table.types.logical.DayTimeIntervalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.Test;

/**
 * A day-time INTERVAL reaches the boundary either as the Int64 millis StreamFusion canonicalises on,
 * or as Arrow's own {@code Interval(DAY_TIME)} when the native result type is an interval rather than
 * a timestamp. Both must read back as Flink's internal millisecond long.
 */
class ArrowIntervalDayColumnVectorTest {

  private static final RowType SCHEMA =
      RowType.of(
          new LogicalType[] {
            new DayTimeIntervalType(DayTimeIntervalType.DayTimeResolution.SECOND)
          },
          new String[] {"i"});

  @Test
  void readsArrowDayTimeIntervalAsMillis() {
    Field field =
        new Field(
            "i",
            FieldType.nullable(new ArrowType.Interval(IntervalUnit.DAY_TIME)),
            Collections.emptyList());
    try (BufferAllocator allocator = new RootAllocator();
        VectorSchemaRoot root =
            VectorSchemaRoot.create(new Schema(Collections.singletonList(field)), allocator)) {
      IntervalDayVector vector = (IntervalDayVector) root.getVector("i");
      vector.allocateNew(6);
      vector.set(0, 0, 6_000);
      // The days component must be folded in, not dropped.
      vector.set(1, 2, 500);
      vector.setNull(2);
      // Both components share a sign when the native side splits a negative interval.
      vector.set(3, -1, -3_600_000);
      // A mixed-sign pair still folds to the sum rather than to either component's sign.
      vector.set(4, 1, -500);
      vector.set(5, 0, 0);
      vector.setValueCount(6);
      root.setRowCount(6);

      ArrowReader reader = ArrowConversion.createArrowReader(root, SCHEMA);

      assertEquals(6_000L, reader.read(0).getLong(0));
      assertEquals(2 * 86_400_000L + 500L, reader.read(1).getLong(0));
      assertTrue(reader.read(2).isNullAt(0));
      assertEquals(-86_400_000L - 3_600_000L, reader.read(3).getLong(0));
      assertEquals(86_400_000L - 500L, reader.read(4).getLong(0));
      assertEquals(0L, reader.read(5).getLong(0));
    }
  }
}
