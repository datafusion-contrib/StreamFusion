package tech.streamfusion.planner;


/** Pure temporal unit, zone and formatting compatibility rules. */
final class TemporalExpressionSemantics {
  private TemporalExpressionSemantics() {}

  /**
   * The chrono field name for a Calcite {@code TimeUnitRange}, or null if not a supported integer
   * field.
   */
  static String extractField(String unit) {
    if (unit == null) {
      return null;
    }
    switch (unit) {
      case "YEAR":
        return "year";
      case "MONTH":
        return "month";
      case "DAY":
        return "day";
      case "HOUR":
        return "hour";
      case "MINUTE":
        return "minute";
      case "SECOND":
        return "second";
      default:
        return null;
    }
  }

  /**
   * Whether the native {@code chrono-tz} path accepts this session zone: IANA names ({@code
   * Region/City}), {@code UTC}, and fixed offsets ({@code +HH:MM}). Legacy JVM forms ({@code
   * GMT+1}, {@code PST}) that {@code chrono-tz} rejects fall back, so we never diverge silently on
   * an unparseable zone.
   */
  static boolean nativeZoneSupported(String zoneId) {
    return "UTC".equals(zoneId) || zoneId.indexOf('/') >= 0 || zoneId.matches("[+-]\\d{2}:\\d{2}");
  }

  /**
   * Translates a Java {@code DateTimeFormatter} pattern to restricted strftime tokens, or null if
   * it uses any field the translation can't reproduce byte-for-byte. Only zero-padded numeric
   * fields (the unambiguous ones) and literal separators are admitted; text fields, fractional
   * seconds, am/pm, zones, and single-letter (non-padded) fields fall back. Plain timestamps use a
   * Java-compatible numeric renderer; the opt-in LTZ path uses Chrono.
   */
  static String toChronoFormat(String pattern) {
    if (pattern == null || pattern.isEmpty()) {
      return null;
    }
    StringBuilder out = new StringBuilder();
    int i = 0;
    boolean quoted = false;
    while (i < pattern.length()) {
      char c = pattern.charAt(i);
      if (Character.isSurrogate(c)) {
        return null;
      }
      if (c == '\'') {
        if (i + 1 < pattern.length() && pattern.charAt(i + 1) == '\'') {
          out.append('\'');
          i += 2;
        } else {
          quoted = !quoted;
          i++;
        }
      } else if (quoted) {
        out.append(c == '%' ? "%%" : String.valueOf(c));
        i++;
      } else if (c == '[' || c == ']' || c == '{' || c == '}' || c == '#') {
        return null;
      } else if (Character.isLetter(c)) {
        int j = i;
        while (j < pattern.length() && pattern.charAt(j) == c) {
          j++;
        }
        String token;
        switch (pattern.substring(i, j)) {
          case "yyyy":
            token = "%Y";
            break;
          case "yy":
            token = "%y";
            break;
          case "MM":
            token = "%m";
            break;
          case "dd":
            token = "%d";
            break;
          case "HH":
            token = "%H";
            break;
          case "mm":
            token = "%M";
            break;
          case "ss":
            token = "%S";
            break;
          default:
            return null;
        }
        out.append(token);
        i = j;
      } else if (c == '%') {
        out.append("%%"); // a literal percent — chrono's escape
        i++;
      } else {
        out.append(c); // separator / literal passes through
        i++;
      }
    }
    return quoted ? null : out.toString();
  }
}
