package tech.streamfusion.arrow;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.flink.table.data.TimestampData;
import org.junit.jupiter.api.Test;

class TimestampConversionTest {
  @Test
  void preservesNanosecondBoundariesAndNegativeFractions() {
    assertEquals(
        Long.MIN_VALUE,
        TimestampConversion.toNanos(TimestampData.fromEpochMillis(-9_223_372_036_855L, 224_192)));
    assertEquals(
        Long.MAX_VALUE,
        TimestampConversion.toNanos(TimestampData.fromEpochMillis(9_223_372_036_854L, 775_807)));
    assertEquals(-1, TimestampConversion.toNanos(TimestampData.fromEpochMillis(-1, 999_999)));
    assertEquals(0, TimestampConversion.toNanos(TimestampData.fromEpochMillis(0)));
  }

  @Test
  void rejectsOverflowOnEitherSideOfTheRange() {
    assertThrows(
        ArithmeticException.class,
        () ->
            TimestampConversion.toNanos(
                TimestampData.fromEpochMillis(-9_223_372_036_855L, 224_191)));
    assertThrows(
        ArithmeticException.class,
        () ->
            TimestampConversion.toNanos(
                TimestampData.fromEpochMillis(9_223_372_036_854L, 775_808)));
  }
}
