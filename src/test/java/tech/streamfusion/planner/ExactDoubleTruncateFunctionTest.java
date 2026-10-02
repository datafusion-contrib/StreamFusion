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
  void dyadicBoundariesAndTheirNeighborsMatchReleasedDecimalRounding() {
    var random = new SplittableRandom(219);
    for (int bits = 0; bits <= 6; bits++) {
      for (int i = 0; i < 1000; i++) {
        double value = random.nextInt(-1000000000, 1000000001) / (double) (1 << bits);
        for (int scale = 0; scale <= 6; scale++) {
          check(Math.nextDown(value), scale);
          check(value, scale);
          check(Math.nextUp(value), scale);
        }
      }
    }
    for (double value : new double[] {1000.1, -1000.1, 1e9, -1e9}) {
      for (int scale = 0; scale <= 6; scale++) check(value, scale);
    }
  }

  @Test
  void smallValuesIncludeTheReleasedScaleEighteenRoundingMargin() {
    for (double value : new double[] {
        -0.0, 0.0, Double.MIN_VALUE, -Double.MIN_VALUE,
        0.5e-18, -0.5e-18, 1.5e-18, -1.5e-18,
        0.46, -0.46, 0.1, -0.1, 1e-6, -1e-6, 1.0, -1.0}) {
      for (int scale = -6; scale <= 6; scale++) {
        check(Math.nextDown(value), scale);
        check(value, scale);
        check(Math.nextUp(value), scale);
      }
    }
  }

  @Test
  void decimalTextBoundariesPreserveRoundingCarryBeforeTruncation() {
    for (int scale = 0; scale <= 6; scale++) {
      for (int coefficient = -20; coefficient <= 20; coefficient++) {
        double value = coefficient / Math.pow(10, scale);
        check(Math.nextDown(value), scale);
        check(value, scale);
        check(Math.nextUp(value), scale);
      }
    }
  }

  @Test
  void decimalGridIdentitiesAndAdjacentDoublesMatchReleasedFlink() {
    var random = new SplittableRandom(518);
    for (int scale = 0; scale <= 6; scale++) {
      long factor = (long) Math.pow(10, scale);
      long limit = 1_000_000_000L * factor;
      for (int sample = 0; sample < 2500; sample++) {
        long coefficient = sample < 10 ? limit - sample : random.nextLong(-limit, limit);
        double value = coefficient / (double) factor;
        check(Math.nextDown(value), scale);
        check(value, scale);
        check(Math.nextUp(value), scale);
        check(Math.nextDown(-value), scale);
        check(-value, scale);
        check(Math.nextUp(-value), scale);
      }
    }
  }

  @Test
  void scaleDependentMagnitudeBoundsAndLargeDecimalNeighborsMatchReleasedFlink() {
    var random = new SplittableRandom(20261002);
    for (int scale = -6; scale <= 6; scale++) {
      double factor = Math.pow(10, Math.max(scale, 0));
      double limit = 1e15 / factor;
      for (double sign : new double[] {-1, 1}) {
        for (double magnitude : new double[] {limit, 1e12 + 0.12345, 1e14 + 0.25}) {
          double value = sign * magnitude;
          check(Math.nextDown(value), scale);
          check(value, scale);
          check(Math.nextUp(value), scale);
        }
      }
      for (int i = 0; i < 2500; i++) {
        double value = random.nextLong(-1_000_000_000_000_000L, 1_000_000_000_000_001L) / factor;
        check(Math.nextDown(value), scale);
        check(value, scale);
        check(Math.nextUp(value), scale);
        check(random.nextDouble(-limit, limit), scale);
      }
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
    if (Double.isFinite(value) && scale >= -6 && scale <= 6
        && Math.abs(value) <= 1e15 / Math.pow(10, Math.max(scale, 0))) {
      assertEquals(Double.doubleToLongBits(expected),
          Double.doubleToLongBits(ExactDoubleTruncateFunction.truncateDecimalText(value, scale)),
          () -> "decimal text " + value + " at scale " + scale);
    }
    assertEquals(
        Double.doubleToLongBits(expected),
        Double.doubleToLongBits(ExactDoubleTruncateFunction.truncate(value, scale)),
        () -> value + " at scale " + scale);
  }
}
