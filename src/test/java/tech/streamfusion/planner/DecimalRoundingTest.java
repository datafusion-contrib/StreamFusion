package tech.streamfusion.planner;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.math.BigDecimal;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.runtime.functions.SqlFunctionUtils;
import org.junit.jupiter.api.Test;

class DecimalRoundingTest {
  @Test
  void floatingTruncatePreservesBitsAndReleasedFailures() {
    var random = new java.util.Random(268);
    for (int i = 0; i < 1000; i++) {
      double value = i < 6 ? new double[] {0.0, -0.0, Double.NaN, Double.POSITIVE_INFINITY,
          Double.NEGATIVE_INFINITY, Double.MAX_VALUE}[i] : Double.longBitsToDouble(random.nextLong());
      for (int position : new int[] {Integer.MIN_VALUE, -38, -3, 0, 2, 18, Integer.MAX_VALUE}) {
        Double expected = null;
        RuntimeException failure = null;
        try { expected = SqlFunctionUtils.struncate(value, position); }
        catch (RuntimeException error) { failure = error; }
        if (failure == null) {
          assertEquals(Double.doubleToRawLongBits(expected),
              Double.doubleToRawLongBits(DecimalRounding.truncate(value, position)));
        } else {
          var actual = org.junit.jupiter.api.Assertions.assertThrows(failure.getClass(),
              () -> DecimalRounding.truncate(value, position));
          // HotSpot can omit helpful NPE messages after the same throw site becomes hot.
          if (!(failure instanceof NullPointerException)) assertEquals(failure.getMessage(), actual.getMessage());
        }
      }
    }
  }

  @Test
  void valuesAndRuntimeMetadataMatchReleasedRounding() {
    var random = new java.util.Random(265);
    for (int precision : new int[] {12, 18, 19, 20, 38}) {
      for (int scale : new int[] {0, 3, 9}) {
        for (int i = 0; i < 500; i++) {
          var unscaled = new java.math.BigInteger(120, random).mod(java.math.BigInteger.TEN.pow(precision));
          if (i % 2 == 0) unscaled = unscaled.negate();
          var value = DecimalData.fromBigDecimal(new BigDecimal(unscaled, scale), precision, scale);
          for (int position : new int[] {-38, -12, -3, 0, 2, 9, Integer.MAX_VALUE}) {
            for (boolean truncate : new boolean[] {false, true}) {
              var expected = truncate ? SqlFunctionUtils.struncate(value, position) : SqlFunctionUtils.sround(value, position);
              var actual = DecimalRounding.round(value, position, truncate);
              assertEquals(expected, actual);
              if (expected != null) {
                assertEquals(expected.precision(), actual.precision());
                assertEquals(expected.scale(), actual.scale());
                assertEquals(expected.toString(), actual.toString());
              }
            }
          }
        }
      }
    }
  }
}
