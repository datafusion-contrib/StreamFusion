package tech.streamfusion.planner;

import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.runtime.functions.SqlFunctionUtils;

/** Exact bounded shortcuts; other domains retain released Flink evaluation. */
public final class ExactDoubleTruncateFunction extends ScalarFunction {
  private static final long serialVersionUID = 1L;
  private static final double[] POWERS = {1, 10, 100, 1000, 10000, 100000, 1000000};

  public Double eval(Double value, Integer scale) {
    if (value == null || scale == null) return null;
    return truncate(value, scale);
  }

  public static double truncate(double value, int scale) {
    double magnitude = Math.abs(value);
    if (magnitude <= 1e9 && scale >= -6 && scale <= 6) {
      if (value != 0 && scale >= 0) {
        double scaledByTwo = value * (1 << scale);
        // An integral power-of-two scale proves at most scale fractional decimal digits.
        // Both the multiplication and integer comparison are exact in this bounded domain.
        if (scaledByTwo == (long) scaledByTwo) return value;
      }
      double lowerInput = Math.nextDown(value);
      double upperInput = Math.nextUp(value);
      if (magnitude < 1) {
        // Flink rounds its decimal to scale 18; include half a decimal unit before scaling.
        lowerInput = Math.nextDown(lowerInput - 0.5e-18);
        upperInput = Math.nextUp(upperInput + 0.5e-18);
      }
      double factor = POWERS[Math.abs(scale)];
      double lower = Math.nextDown(scale >= 0 ? lowerInput * factor : lowerInput / factor);
      double upper = Math.nextUp(scale >= 0 ? upperInput * factor : upperInput / factor);
      long low = (long) lower;
      long high = (long) upper;
      if (low == high) {
        return low == 0 ? 0.0 : scale >= 0 ? low / factor : low * factor;
      }
      return truncateDecimalText(value, scale);
    }
    return SqlFunctionUtils.struncate(value, scale);
  }
  static double truncateDecimalText(double value, int scale) {
    String text = Double.toString(value);
    int start = text.charAt(0) == '-' ? 1 : 0;
    int exponentAt = text.indexOf('E');
    int end = exponentAt < 0 ? text.length() : exponentAt;
    int pointAt = text.indexOf('.');
    int integerDigits = pointAt < 0 ? end - start : pointAt - start;
    int digitCount = end - start - (pointAt < 0 ? 0 : 1);
    int point = integerDigits + (exponentAt < 0 ? 0
        : Integer.parseInt(text, exponentAt + 1, text.length(), 10));
    long coefficient = 0;
    for (int digit = 0; digit < point + scale; digit++) {
      coefficient = coefficient * 10 + decimalDigit(text, start, integerDigits, digitCount, digit);
    }
    // Flink applies HALF_UP at scale 18 before truncation. Only a carry through all omitted
    // nines can change the retained coefficient; its bounded value is at most 1e15.
    boolean carry = true;
    for (int place = scale + 1; place <= 18; place++) {
      if (decimalDigit(text, start, integerDigits, digitCount, point + place - 1) != 9) {
        carry = false;
        break;
      }
    }
    if (carry && decimalDigit(text, start, integerDigits, digitCount, point + 18) >= 5) {
      coefficient++;
    }
    if (coefficient == 0) return 0.0;
    double result = scale >= 0 ? coefficient / POWERS[scale] : coefficient * POWERS[-scale];
    return value < 0 ? -result : result;
  }

  private static int decimalDigit(String text, int start, int integerDigits, int digitCount, int digit) {
    if (digit < 0 || digit >= digitCount) return 0;
    return text.charAt(start + digit + (digit >= integerDigits ? 1 : 0)) - '0';
  }

}
