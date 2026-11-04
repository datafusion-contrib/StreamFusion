package tech.streamfusion.planner;

import java.util.Arrays;
import java.util.Formatter;
import java.util.IllegalFormatException;
import java.util.Locale;
import java.util.regex.Pattern;

/** Literal formats whose output depends only on verified primitive formatting and ASCII. */
final class IntegerPrintfFormat {
  private static final Pattern CONVERSION =
      Pattern.compile("%(?:([0-9]+)\\$)?([-0+ <]*)([0-9]+)?(?:\\.([0-9]+))?([sSdxXobB])");

  private IntegerPrintfFormat() {}

  static boolean supports(String format, int arguments) {
    return supports(format, arguments, null);
  }

  static boolean supports(String format, boolean[] booleanArguments) {
    return supports(format, booleanArguments.length, booleanArguments);
  }

  private static boolean supports(String format, int arguments, boolean[] booleanArguments) {
    if (format.length() > 512 || format.chars().anyMatch(c -> c > 127)) return false;
    var matcher = CONVERSION.matcher(format);
    for (int offset = 0; offset < format.length(); ) {
      int percent = format.indexOf('%', offset);
      if (percent < 0) break;
      if (percent + 1 < format.length() && format.charAt(percent + 1) == '%') {
        offset = percent + 2;
        continue;
      }
      if (!matcher.region(percent, format.length()).lookingAt()
          || matcher.group(1) != null && matcher.group(2).contains("<")) return false;
      try {
        if (matcher.group(1) != null) {
          int index = Integer.parseInt(matcher.group(1));
          // JDK 11 accepts index zero, unlike later JDKs and the Arrow formatter.
          if (index < 1 || index > arguments) return false;
        }
        if (matcher.group(3) != null && Integer.parseInt(matcher.group(3)) > 256
            || matcher.group(4) != null && Integer.parseInt(matcher.group(4)) > 256) return false;
      } catch (NumberFormatException ignored) {
        return false;
      }
      offset = matcher.end();
    }
    // Validate the running JDK's flags/indexing during planning, never per input row.
    Object[] zeros = new Object[arguments];
    Arrays.fill(zeros, 0);
    if (booleanArguments != null) {
      for (int i = 0; i < arguments; i++) {
        if (booleanArguments[i]) zeros[i] = Boolean.FALSE;
      }
    }
    try (var formatter = new Formatter(Locale.US)) {
      formatter.format(format, zeros);
      return true;
    } catch (IllegalFormatException ignored) {
      return false;
    }
  }
}
