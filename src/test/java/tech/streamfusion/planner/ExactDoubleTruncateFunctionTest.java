package tech.streamfusion.planner;

import static org.junit.jupiter.api.Assertions.*;

import java.util.SplittableRandom;
import org.apache.flink.table.runtime.functions.SqlFunctionUtils;
import org.junit.jupiter.api.Test;

class ExactDoubleTruncateFunctionTest {
  @Test
  void matchesReleasedDecimalConversionAndTruncation() {
    var random = new SplittableRandom(917);
    for (int i = 0; i < 30000; i++) {
      check(random.nextDouble(-1e9, 1e9), random.nextInt(-7, 8));
      check(Double.longBitsToDouble(random.nextLong()), random.nextInt(-7, 8));
    }
    for (int scale = -6; scale <= 6; scale++) {
      for (int i = 0; i < 1000; i++) {
        double value = random.nextInt(-1000000, 1000001) / Math.pow(10, scale);
        check(Math.nextDown(value), scale);
        check(value, scale);
        check(Math.nextUp(value), scale);
      }
    }
    for (double value :
        new double[] {
          -0.0,
          0.0,
          1,
          -1,
          1e9,
          -1e9,
          Math.nextDown(1.0),
          Math.nextUp(1e9),
          Double.NaN,
          Double.POSITIVE_INFINITY,
          Double.NEGATIVE_INFINITY,
          Double.MIN_VALUE,
          -Double.MIN_VALUE,
          Double.MAX_VALUE
        }) {
      for (int scale : new int[] {Integer.MIN_VALUE, Integer.MAX_VALUE, -100, 100, -6, 6, 0})
        check(value, scale);
    }
  }

  @Test
  void preservesSqlNulls() {
    var function = new ExactDoubleTruncateFunction();
    assertNull(function.eval(null, 1));
    assertNull(function.eval(1.5, null));
  }

  private static void check(double value, int scale) {
    double expected;
    try {
      expected = SqlFunctionUtils.struncate(value, scale);
    } catch (RuntimeException failure) {
      var actual =
          assertThrows(
              failure.getClass(), () -> ExactDoubleTruncateFunction.truncate(value, scale));
      assertEquals(failure.getMessage(), actual.getMessage());
      return;
    }
    assertEquals(
        Double.doubleToLongBits(expected),
        Double.doubleToLongBits(ExactDoubleTruncateFunction.truncate(value, scale)),
        () -> value + " at scale " + scale);
  }
}
