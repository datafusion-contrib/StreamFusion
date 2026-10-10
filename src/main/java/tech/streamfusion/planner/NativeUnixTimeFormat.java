package tech.streamfusion.planner;

import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.time.ZoneId;
import java.util.TimeZone;

/** Admission for the numeric, fixed-offset subset of Flink's SimpleDateFormat contract. */
final class NativeUnixTimeFormat {
  private NativeUnixTimeFormat() {}

  static String encode(String pattern, String zoneId) {
    if (pattern == null || zoneId == null || !ZoneId.of(zoneId).getRules().isFixedOffset()) {
      return null;
    }
    if (pattern.codePoints().anyMatch(c -> c >= 0xd800 && c <= 0xdfff)) return null;
    boolean quoted = false;
    for (int i = 0; i < pattern.length(); ) {
      char c = pattern.charAt(i++);
      if (c == '\'') {
        if (i < pattern.length() && pattern.charAt(i) == '\'') i++;
        else quoted = !quoted;
      } else if (!quoted && (c >= 'A' && c <= 'Z' || c >= 'a' && c <= 'z')) {
        int width = 1;
        while (i < pattern.length() && pattern.charAt(i) == c) {
          i++;
          width++;
        }
        if (!(c == 'y' && width == 4 || "MdHms".indexOf(c) >= 0 && width == 2)) return null;
      }
    }
    if (quoted) return null;
    var format = new SimpleDateFormat(pattern);
    Object numbersCandidate;
    if (!format.getCalendar().getCalendarType().equals("gregory")
        || !((numbersCandidate = format.getNumberFormat()) instanceof DecimalFormat)
        || ((DecimalFormat) numbersCandidate).getDecimalFormatSymbols().getZeroDigit() != '0')
      return null;

    // Use the same ZoneId -> TimeZone conversion and serialized zone ID as Flink code generation.
    var zone = TimeZone.getTimeZone(TimeZone.getTimeZone(ZoneId.of(zoneId)).getID());
    return zone.getRawOffset() + "|" + pattern;
  }
}
