package tech.streamfusion.arrow;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.DecimalVector;
import org.apache.flink.table.data.DecimalData;
import org.junit.jupiter.api.Test;
import tech.streamfusion.arrow.vectors.ArrowDecimalColumnVector;

class DecimalAccessorTest {
  @Test
  void wideReconstructionPreservesUnscaledValueAndOverflowContract() {
    for (String text : java.util.List.of("0.000", "-9999999999999999999.25", "123456789012345678901234567890.12345678")) {
      BigDecimal value = new BigDecimal(text);
      for (int scale : new int[] {0, 2, 18}) {
        for (int precision : new int[] {19, 38}) {
          assertEquals(
              DecimalData.fromUnscaledBytes(value.unscaledValue().toByteArray(), precision, scale),
              DecimalAccessor.fromInternalValue(value, precision, scale));
        }
      }
    }
  }

  @Test
  void compactRoundingCarrySurvivesArrowAndInternalReconstruction() {
    try (var allocator = new RootAllocator();
        var vector = new DecimalVector("d", allocator, 5, 2)) {
      vector.allocateNew(3);
      DecimalAccessor.set(vector, 0, DecimalData.fromUnscaledLong(100000, 5, 2));
      DecimalAccessor.set(vector, 1, DecimalData.fromUnscaledLong(-100000, 5, 2));
      DecimalAccessor.set(vector, 2, null);
      vector.setValueCount(3);
      var column = new ArrowDecimalColumnVector(vector);
      assertEquals(new BigDecimal("1000.00"), column.getDecimal(0, 5, 2).toBigDecimal());
      assertEquals(new BigDecimal("-1000.00"), column.getDecimal(1, 5, 2).toBigDecimal());
      assertTrue(column.isNullAt(2));
    }
  }

  @Test
  void wideDecimalsKeepTheirValues() {
    try (var allocator = new RootAllocator();
        var vector = new DecimalVector("d", allocator, 38, 18)) {
      vector.allocateNew(1);
      BigDecimal value = new BigDecimal("99999999999999999999.999999999999999999");
      DecimalAccessor.set(vector, 0, DecimalData.fromBigDecimal(value, 38, 18));
      vector.setValueCount(1);
      assertEquals(
          value, new ArrowDecimalColumnVector(vector).getDecimal(0, 38, 18).toBigDecimal());
    }
  }

  @Test
  void differingScalesStillConvertAndCheckPrecision() {
    try (var allocator = new RootAllocator();
        var vector = new DecimalVector("d", allocator, 5, 2)) {
      vector.allocateNew(2);
      DecimalAccessor.set(vector, 0, DecimalData.fromBigDecimal(new BigDecimal("1.255"), 6, 3));
      DecimalAccessor.set(vector, 1, DecimalData.fromBigDecimal(new BigDecimal("999.995"), 6, 3));
      vector.setValueCount(2);
      assertEquals(new BigDecimal("1.26"), vector.getObject(0));
      assertTrue(vector.isNull(1));
    }
  }
}
