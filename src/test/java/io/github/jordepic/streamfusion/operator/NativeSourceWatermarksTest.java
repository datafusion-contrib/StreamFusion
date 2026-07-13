package io.github.jordepic.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.TimeStampNanoVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;

/** Pins {@link NativeSourceWatermarks#maxRowtimeMillis} to the native {@code max_rowtime_millis}
 * semantics: nulls skipped, nanoseconds floored toward the earlier millisecond (pre-epoch included),
 * bigint epoch millis read verbatim, {@code Long.MIN_VALUE} when every value is null. */
class NativeSourceWatermarksTest {

  @Test
  void maxRowtimeSkipsNullsAndFloorsNanosToMillis() {
    try (BufferAllocator allocator = new RootAllocator();
        VectorSchemaRoot root = nanoRoot(allocator)) {
      TimeStampNanoVector rowtimes = (TimeStampNanoVector) root.getVector(0);
      rowtimes.setSafe(0, 999_999_999L); // floors to 999ms
      rowtimes.setNull(1);
      rowtimes.setSafe(2, 1_000_000_001L); // floors to 1000ms — the max
      root.setRowCount(3);
      assertEquals(1000, NativeSourceWatermarks.maxRowtimeMillis(root, 0));
    }
  }

  @Test
  void maxRowtimeFloorsPreEpochAndSignalsAllNull() {
    try (BufferAllocator allocator = new RootAllocator();
        VectorSchemaRoot root = nanoRoot(allocator)) {
      TimeStampNanoVector rowtimes = (TimeStampNanoVector) root.getVector(0);
      rowtimes.setSafe(0, -1L); // pre-epoch: floors to -1ms, not 0
      root.setRowCount(1);
      assertEquals(-1, NativeSourceWatermarks.maxRowtimeMillis(root, 0));

      rowtimes.setNull(0);
      root.setRowCount(1);
      assertEquals(Long.MIN_VALUE, NativeSourceWatermarks.maxRowtimeMillis(root, 0));
    }
  }

  @Test
  void maxRowtimeReadsEpochMillisBigintVerbatim() {
    try (BufferAllocator allocator = new RootAllocator();
        VectorSchemaRoot root =
            VectorSchemaRoot.create(
                new Schema(
                    List.of(
                        new Field(
                            "dateTime",
                            FieldType.nullable(new ArrowType.Int(64, true)),
                            List.of()))),
                allocator)) {
      root.allocateNew();
      BigIntVector rowtimes = (BigIntVector) root.getVector(0);
      rowtimes.setSafe(0, 60_000L);
      rowtimes.setNull(1);
      rowtimes.setSafe(2, 90_000L);
      root.setRowCount(3);
      assertEquals(90_000, NativeSourceWatermarks.maxRowtimeMillis(root, 0));
    }
  }

  private static VectorSchemaRoot nanoRoot(BufferAllocator allocator) {
    VectorSchemaRoot root =
        VectorSchemaRoot.create(
            new Schema(
                List.of(
                    new Field(
                        "ts",
                        FieldType.nullable(new ArrowType.Timestamp(TimeUnit.NANOSECOND, null)),
                        List.of()))),
            allocator);
    root.allocateNew();
    return root;
  }
}
