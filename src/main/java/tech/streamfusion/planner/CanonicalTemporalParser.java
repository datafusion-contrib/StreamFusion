package tech.streamfusion.planner;

import java.time.DateTimeException;
import java.time.LocalDateTime;
import java.time.LocalDate;
import java.util.TimeZone;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;

/** Verified fast paths for Flink's temporal parsers, with released-parser fallback. */
public final class CanonicalTemporalParser {
  private static final int[] POWERS_OF_TEN = {
    1, 10, 100, 1_000, 10_000, 100_000, 1_000_000, 10_000_000, 100_000_000, 1_000_000_000
  };

  private CanonicalTemporalParser() {}

  public static TimestampData tryTimestamp(StringData input, int precision, TimeZone zone) {
    try {
      TimestampData value = parse(input, precision, zone);
      if (value != null) return value;
      return zone == null
          ? org.apache.flink.table.utils.DateTimeUtils.parseTimestampData(input.toString(), precision)
          : org.apache.flink.table.utils.DateTimeUtils.parseTimestampData(input.toString(), precision, zone);
    } catch (RuntimeException invalid) {
      return null;
    }
  }

  public static Integer tryDate(StringData input) {
    try {
      Integer value = parseDate(input);
      return value != null ? value : org.apache.flink.table.utils.DateTimeUtils.parseDate(input.toString());
    } catch (RuntimeException invalid) {
      return null;
    }
  }

  public static Integer tryTime(StringData input, int precision) {
    try {
      Integer value = parseTimeMillis(input);
      if (value == null) value = org.apache.flink.table.utils.DateTimeUtils.parseTime(input.toString());
      return value == null ? null : tech.streamfusion.compat.FlinkCompat.applyParsedTimePrecision(value, precision);
    } catch (RuntimeException invalid) {
      return null;
    }
  }

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
    boolean zeroFraction = true;
    if (length > 19) {
      nanos = digits(text, 20, length);
      if (nanos < 0) return null;
      zeroFraction = nanos == 0;
      nanos *= POWERS_OF_TEN[29 - length];
      int quantum = POWERS_OF_TEN[9 - precision];
      nanos = nanos / quantum * quantum;
    }
    try {
      int month = digits(text, 5, 7);
      int day = digits(text, 8, 10);
      int hour = digits(text, 11, 13);
      int minute = digits(text, 14, 16);
      int second = digits(text, 17, 19);
      if (month < 1 || month > 12 || day < 1 || day > 31
          || hour < 0 || hour > 24 || minute < 0 || minute > 59 || second < 0 || second > 59) return null;
      day = Math.min(day, daysInMonth(year, month));
      LocalDateTime local;
      if (hour == 24) {
        if (minute != 0 || second != 0 || !zeroFraction) return null;
        local = LocalDate.of(year, month, day).plusDays(1).atStartOfDay();
      } else {
        local = LocalDateTime.of(year, month, day, hour, minute, second, nanos);
      }
      return zone == null ? TimestampData.fromLocalDateTime(local)
          : TimestampData.fromInstant(local.atZone(zone.toZoneId()).toInstant());
    } catch (DateTimeException unsupported) {
      return null;
    }
  }

  public static Integer parseDate(StringData input) {
    String text = input.toString();
    int length = text.length();
    if (length < 8 || length > 10 || text.charAt(4) != '-') return null;
    int separator = text.indexOf('-', 5);
    if (separator < 6 || separator > 7 || length - separator < 2 || length - separator > 3) return null;
    int year = digits(text, 0, 4);
    if (year <= 0) return null;
    try {
      int month = digits(text, 5, separator);
      int day = digits(text, separator + 1, length);
      if (month < 1 || month > 12 || day < 1 || day > daysInMonth(year, month)) return null;
      return (int) LocalDate.of(year, month, day).toEpochDay();
    } catch (DateTimeException unsupported) {
      return null;
    }
  }

  public static Integer parseTimeMillis(StringData input) {
    String text = input.toString();
    int length = text.length();
    if ((length != 8 && (length < 10 || length > 12)) || text.charAt(2) != ':' || text.charAt(5) != ':'
        || (length > 8 && text.charAt(8) != '.')) return null;
    int hour = digits(text, 0, 2);
    int minute = digits(text, 3, 5);
    int second = digits(text, 6, 8);
    int millis = length > 8 ? digits(text, 9, length) : 0;
    if (hour < 0 || hour > 23 || minute < 0 || minute > 59
        || second < 0 || second > 59 || millis < 0) return null;
    if (length > 8) millis *= POWERS_OF_TEN[12 - length];
    return ((hour * 60 + minute) * 60 + second) * 1_000 + millis;
  }

  private static int daysInMonth(int year, int month) {
    return switch (month) {
      case 2 -> java.time.Year.isLeap(year) ? 29 : 28;
      case 4, 6, 9, 11 -> 30;
      default -> 31;
    };
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
