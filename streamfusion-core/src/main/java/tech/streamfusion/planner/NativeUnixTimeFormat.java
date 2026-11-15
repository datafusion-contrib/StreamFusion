package tech.streamfusion.planner;

import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.time.ZoneId;
import java.util.TimeZone;
import java.util.TreeSet;

/** Compiles numeric patterns and the running JDK's finite TimeZone offset history. */
final class NativeUnixTimeFormat {
  private static final long JDK_HISTORY_START = -2208988800000L; // 1900-01-01 UTC
  private static final long JDK_HISTORY_END = 2145916799000L; // 2037-12-31 23:59:59 UTC
  private static final long JDK_FINAL_STANDARD_YEAR = 2114380800000L; // 2037-01-01

  private NativeUnixTimeFormat() {}

  static String encode(String pattern, String zoneId) {
    if (pattern == null || zoneId == null) {
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
    var rules = ZoneId.of(zoneId).getRules();
    if (!rules.getTransitionRules().isEmpty() || zone.useDaylightTime()) return null;
    if (!rules.isFixedOffset() && !zone.getClass().getName().equals("sun.util.calendar.ZoneInfo"))
      return null;
    var boundaries = new TreeSet<Long>();
    // java.util.TimeZone starts its historical table at 1900, uses the final raw offset
    // before that, and may append a final standard-time entry after finite history. ZoneRules alone
    // would use local mean time before 1900 and produce different Flink results.
    boundaries.add(JDK_HISTORY_START);
    boundaries.add(JDK_FINAL_STANDARD_YEAR - zone.getRawOffset());
    for (var transition : rules.getTransitions()) {
      long millis = transition.getInstant().toEpochMilli();
      if (millis > JDK_HISTORY_END) return null;
      if (millis >= JDK_HISTORY_START) boundaries.add(millis);
    }
    // ZoneInfo appends a raw-offset boundary after finite history. Its year varies
    // by JDK (2037 or 2100); locate it through the public API instead of assuming a year.
    long last = boundaries.last();
    int lastOffset = zone.getOffset(last);
    if (lastOffset != zone.getOffset(Long.MAX_VALUE)) {
      long end = Long.MAX_VALUE;
      while (end - last > 1) {
        long middle = last + (end - last) / 2;
        if (zone.getOffset(middle) == lastOffset) last = middle;
        else end = middle;
      }
      boundaries.add(end);
    }
    int offset = zone.getOffset(Long.MIN_VALUE);
    var encoded = new StringBuilder().append(offset);
    for (long millis : boundaries) {
      int nextOffset = zone.getOffset(millis);
      if (nextOffset != offset) {
        encoded.append(',').append(millis).append(':').append(nextOffset);
        offset = nextOffset;
      }
    }
    return encoded.append('|').append(pattern).toString();
  }
}
