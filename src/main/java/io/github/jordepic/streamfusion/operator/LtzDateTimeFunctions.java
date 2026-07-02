package io.github.jordepic.streamfusion.operator;

import java.util.TimeZone;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.utils.DateTimeUtils;

/**
 * The byte-parity path for {@code DATE_FORMAT}/{@code EXTRACT} over a {@code TIMESTAMP_LTZ}. A local-zoned
 * timestamp's calendar fields depend on the session time zone (Flink renders the instant in {@code
 * table.local-time-zone} first), which the native formatter — having no time-zone database — cannot
 * reproduce. Rather than re-derive Flink's zone handling in Rust, these route the LTZ case through
 * Flink's <em>own</em> {@link DateTimeUtils} via the columnar JVM upcall ({@link NativeUdf}), exactly as
 * {@code REGEXP_EXTRACT}/{@code UPPER}/{@code LOWER} do — so the result is byte-identical by construction.
 * The pattern/field and the session zone are baked into the function; the only column argument is the
 * timestamp, marshalled as epoch millis ({@link NativeUdf#TYPE_TIMESTAMP}). The faster pure-native
 * {@code chrono-tz} path is the opt-in alternative behind {@code allowIncompatible}.
 */
public final class LtzDateTimeFunctions {

  private LtzDateTimeFunctions() {}

  /** {@code DATE_FORMAT(ltz, pattern)} = Flink's {@code formatTimestamp(ts, pattern, sessionZone)}. */
  public static final class DateFormat extends ScalarFunction {
    private static final long serialVersionUID = 1L;
    private final String pattern;
    private final String zoneId;

    public DateFormat(String pattern, String zoneId) {
      this.pattern = pattern;
      this.zoneId = zoneId;
    }

    public String eval(Long epochMillis) {
      if (epochMillis == null) {
        return null;
      }
      return DateTimeUtils.formatTimestamp(
          TimestampData.fromEpochMillis(epochMillis), pattern, TimeZone.getTimeZone(zoneId));
    }
  }

  /**
   * {@code EXTRACT(field FROM ltz)} = Flink's {@code extractFromTimestamp(field, ts, sessionZone)}. The
   * field is carried as its name (e.g. {@code "HOUR"}) — Flink's {@link DateTimeUtils.TimeUnitRange}
   * shares Calcite's enum names — so the function holds only strings (no Calcite type at runtime).
   */
  public static final class Extract extends ScalarFunction {
    private static final long serialVersionUID = 1L;
    private final String field;
    private final String zoneId;

    public Extract(String field, String zoneId) {
      this.field = field;
      this.zoneId = zoneId;
    }

    public Long eval(Long epochMillis) {
      if (epochMillis == null) {
        return null;
      }
      return DateTimeUtils.extractFromTimestamp(
          DateTimeUtils.TimeUnitRange.valueOf(field),
          TimestampData.fromEpochMillis(epochMillis),
          TimeZone.getTimeZone(zoneId));
    }
  }
}
