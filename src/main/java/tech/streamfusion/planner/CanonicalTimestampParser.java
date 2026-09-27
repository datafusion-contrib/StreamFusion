package tech.streamfusion.planner;

import java.time.DateTimeException;
import java.time.LocalDateTime;
import java.util.TimeZone;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;

/** A strict subset of Flink's timestamp parser; null requests the released parser's fallback. */
public final class CanonicalTimestampParser {
  private static final int[] POWERS_OF_TEN = {
    1, 10, 100, 1_000, 10_000, 100_000, 1_000_000, 10_000_000, 100_000_000, 1_000_000_000
  };

  private CanonicalTimestampParser() {}

  public static TimestampData parse(StringData input, int precision, TimeZone zone) {
    String text = input.toString();
    int length = text.length();
    if ((length != 19 && (length < 21 || length > 29))
        || precision < 0 || precision > 9
        || text.charAt(4) != '-' || text.charAt(7) != '-'
        || text.charAt(10) != ' ' || text.charAt(13) != ':' || text.charAt(16) != ':'
        || (length > 19 && text.charAt(19) != '.')) {
      return null;
    }
    int year = digits(text, 0, 4);
    // Flink's formatter uses year-of-era, so year zero must go through its own resolver.
    if (year <= 0) return null;
    int nanos = 0;
    if (length > 19) {
      nanos = digits(text, 20, length);
      if (nanos < 0) return null;
      nanos *= POWERS_OF_TEN[29 - length];
      int quantum = POWERS_OF_TEN[9 - precision];
      nanos = nanos / quantum * quantum;
    }
    try {
      var local = LocalDateTime.of(year, digits(text, 5, 7), digits(text, 8, 10),
          digits(text, 11, 13), digits(text, 14, 16), digits(text, 17, 19), nanos);
      return zone == null ? TimestampData.fromLocalDateTime(local)
          : TimestampData.fromInstant(local.atZone(zone.toZoneId()).toInstant());
    } catch (DateTimeException unsupported) {
      // Flink's SMART resolver also accepts some noncanonical dates and times.
      return null;
    }
  }

  private static int digits(String text, int start, int end) {
    int value = 0;
    for (int i = start; i < end; i++) {
      int digit = text.charAt(i) - '0';
      if (digit < 0 || digit > 9) return -1;
      value = value * 10 + digit;
    }
    return value;
  }
}
