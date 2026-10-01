package tech.streamfusion.planner;

import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.runtime.functions.SqlFunctionUtils;

/** A proved bounded shortcut; ambiguous decimal boundaries retain released Flink evaluation. */
public final class ExactDoubleTruncateFunction extends ScalarFunction {
  private static final long serialVersionUID = 1L;
  private static final double[] POWERS = {1, 10, 100, 1000, 10000, 100000, 1000000};

  public Double eval(Double value, Integer scale) {
    if (value == null || scale == null) return null;
    return truncate(value, scale);
  }

  public static double truncate(double value, int scale) {
    double magnitude = Math.abs(value);
    if (magnitude >= 1 && magnitude <= 1e9 && scale >= -6 && scale <= 6) {
      // At this magnitude, Flink's scale-18 decimal rounding stays between adjacent doubles.
      // Outward-rounded scaled bounds must truncate to one integer before using the shortcut.
      // The domain keeps that integer and each power of ten exactly representable as doubles.
      double factor = POWERS[Math.abs(scale)];
      double lower =
          Math.nextDown(scale >= 0 ? Math.nextDown(value) * factor : Math.nextDown(value) / factor);
      double upper =
          Math.nextUp(scale >= 0 ? Math.nextUp(value) * factor : Math.nextUp(value) / factor);
      long low = (long) lower;
      long high = (long) upper;
      if (low == high) {
        return low == 0 ? 0.0 : scale >= 0 ? low / factor : low * factor;
      }
    }
    return SqlFunctionUtils.struncate(value, scale);
  }
}
