package tech.streamfusion.arrow;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.Instant;
import java.util.List;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.TimeStampVector;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.flink.table.data.TimestampData;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.api.Test;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.flink.table.data.GenericRowData;
import tech.streamfusion.arrow.writers.TimestampWriter;
import tech.streamfusion.arrow.vectors.ArrowTimestampColumnVector;

class TimestampAccessorTest {
  @Test
  void componentWriterPreservesRangeAndHiddenFractions() {
    try (BufferAllocator allocator = new RootAllocator();
        StructVector vector = (StructVector) TimestampAccessor.field("ts", true).createVector(allocator)) {
      vector.allocateNew();
      var writer = TimestampWriter.forRow(vector, 3);
      TimestampData[] values = {
          TimestampData.fromEpochMillis(Long.MIN_VALUE, 999999), null,
          TimestampData.fromEpochMillis(-62135596800000L, 123456),
          TimestampData.fromEpochMillis(-1, 999999),
          TimestampData.fromEpochMillis(253402300799999L, 999999),
          TimestampData.fromEpochMillis(Long.MAX_VALUE, 999999)};
      for (TimestampData value : values) writer.write(GenericRowData.of(value), 0);
      writer.finish();
      var reader = new TimestampAccessor(vector);
      for (int i = 0; i < values.length; i++) {
        assertEquals(values[i] == null, reader.isNull(i));
        if (values[i] != null) assertEquals(values[i], reader.getTimestamp(i));
      }
      assertEquals(Long.MAX_VALUE, reader.maxMillis(values.length));
      writer.reset();
      writer.write(GenericRowData.of((Object) null), 0);
      writer.finish();
      assertNull(reader.maxMillis(1));
    }
  }

  @Test
  void cachedTimestampWriterSurvivesGrowthAndReset() {
    try (BufferAllocator allocator = new RootAllocator();
        StructVector vector = (StructVector) TimestampAccessor.field("ts", true).createVector(allocator)) {
      vector.setInitialCapacity(2);
      vector.allocateNew();
      var writer = TimestampWriter.forArray(vector, 9);
      var reader = new TimestampAccessor(vector);
      for (int pass = 0; pass < 2; pass++) {
        for (int row = 0; row < 8193; row++) {
          TimestampData value = row % 7 == 0 ? null : TimestampData.fromEpochMillis(-row, row % 1_000_000);
          writer.write(new org.apache.flink.table.data.GenericArrayData(new TimestampData[] {value}), 0);
        }
        writer.finish();
        for (int row = 0; row < 8193; row++) {
          assertEquals(row % 7 == 0, reader.isNull(row));
          if (row % 7 != 0) assertEquals(TimestampData.fromEpochMillis(-row, row % 1_000_000), reader.getTimestamp(row));
        }
        writer.reset();
      }
    }
  }

  @Test
  void primitiveWriterReportsOverflowInsteadOfWrapping() {
    try (BufferAllocator allocator = new RootAllocator();
        TimeStampVector vector = (TimeStampVector) new Field("ts",
            FieldType.nullable(new ArrowType.Timestamp(TimeUnit.NANOSECOND, null)), List.of()).createVector(allocator)) {
      vector.allocateNew();
      TimestampData value = TimestampData.fromEpochMillis(-62135596800000L, 123456);
      assertThrows(ArithmeticException.class, () -> TimestampWriter.forRow(vector, 9).write(GenericRowData.of(value), 0));
      for (long nanos : new long[] {Long.MIN_VALUE, -1, 0, Long.MAX_VALUE}) {
        TimestampData expected = TimestampData.fromEpochMillis(Math.floorDiv(nanos, 1000000), (int) Math.floorMod(nanos, 1000000));
        TimestampAccessor.set(vector, 0, expected);
        assertEquals(nanos, vector.get(0));
      }
    }
  }

  @ParameterizedTest
  @EnumSource(TimeUnit.class)
  void readsFlinkValueWithoutNarrowingToNanoseconds(TimeUnit unit) {
    Field field =
        new Field("ts", FieldType.nullable(new ArrowType.Timestamp(unit, "UTC")), List.of());
    long perSecond =
        switch (unit) {
          case SECOND -> 1;
          case MILLISECOND -> 1000;
          case MICROSECOND -> 1_000_000;
          case NANOSECOND -> 1_000_000_000;
        };
    long[] values =
        unit == TimeUnit.SECOND
            ? new long[] {Long.MIN_VALUE / 1000, -1, 0, 1, Long.MAX_VALUE / 1000}
            : new long[] {Long.MIN_VALUE, -1_000_001, -1, 0, 1, Long.MAX_VALUE};
    try (BufferAllocator allocator = new RootAllocator();
        TimeStampVector vector = (TimeStampVector) field.createVector(allocator)) {
      vector.allocateNew();
      TimestampAccessor accessor = new TimestampAccessor(vector);
      ArrowTimestampColumnVector rowView = new ArrowTimestampColumnVector(vector);
      for (long value : values) {
        vector.setSafe(0, value);
        vector.setNull(1);
        vector.setValueCount(2);
        TimestampData expected =
            TimestampData.fromInstant(
                Instant.ofEpochSecond(
                    value / perSecond, value % perSecond * (1_000_000_000 / perSecond)));
        assertEquals(expected, accessor.getTimestamp(0));
        assertEquals(expected, rowView.getTimestamp(0, 9));
        assertEquals(expected.getMillisecond(), accessor.maxMillis(2));
      }
      vector.setNull(0);
      assertNull(accessor.maxMillis(2));
      assertNull(accessor.maxMillis(0));
      if (unit == TimeUnit.SECOND) {
        vector.setSafe(0, Long.MAX_VALUE);
        assertThrows(ArithmeticException.class, () -> accessor.getMillis(0));
      }
    }
  }
}
