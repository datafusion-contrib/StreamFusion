package tech.streamfusion;

import static org.apache.flink.table.api.DataTypes.*;
import static org.junit.jupiter.api.Assertions.*;

import java.time.Instant;
import java.time.LocalDateTime;
import java.util.Arrays;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.types.Row;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.planner.NativePlanner;

class FlinkNativeCalendarSqlHarnessTest {
  @ParameterizedTest
  @ValueSource(strings = {"UTC", "Asia/Shanghai", "GMT+05:30"})
  void extractionUsesTheRunningJdkHistoryWithoutJvmChildren(String zone) throws Exception {
    String sql =
        "SELECT YEAR(d), MONTH(d), DAYOFMONTH(d), YEAR(ts), MONTH(ts), DAYOFMONTH(ts), "
            + "HOUR(ts), MINUTE(ts), SECOND(ts), HOUR(tm), MINUTE(tm), SECOND(tm), "
            + "YEAR(ltz), MONTH(ltz), DAYOFMONTH(ltz), QUARTER(ltz), WEEK(ltz), DAYOFYEAR(ltz), "
            + "DAYOFWEEK(ltz), HOUR(ltz), MINUTE(ltz), SECOND(ltz) FROM src";
    BuiltinFunctionParity.assertParity(() -> environment(zone), sql);
    var tables = environment(zone);
    var scan = NativePlanner.install(tables);
    tables.explainSql(sql);
    assertTrue(scan.fallbackReasons().isEmpty(), scan.explainSummary());
    assertTrue(
        scan.functionExecutions().stream().allMatch(s -> s.contains("jvm=[]")),
        scan.explainSummary());
  }

  @ParameterizedTest
  @ValueSource(strings = {"UTC", "America/New_York"})
  void calendarRoundingAndIntervalsPreserveLeapDaysNegativeEpochAndNanoseconds(String zone)
      throws Exception {
    BuiltinFunctionParity.assertParity(
        () -> environment(zone),
        "SELECT FLOOR(ts TO MONTH), CEIL(ts TO YEAR), FLOOR(d TO MONTH), CEIL(d TO YEAR), "
            + "ts + INTERVAL '1' MONTH, ts - INTERVAL '1' DAY, "
            + "ltz + INTERVAL '1' MONTH, ltz - INTERVAL '1' DAY, "
            + "d + INTERVAL '1' MONTH, d - INTERVAL '1' DAY, tm - INTERVAL '25' HOUR, "
            + "GREATEST(ts,ts + INTERVAL '1' DAY), LEAST(ltz,ltz - INTERVAL '1' MONTH), "
            + "GREATEST(d,d + INTERVAL '1' MONTH), LEAST(tm,tm + INTERVAL '1' HOUR) FROM src");
  }

  private TableEnvironment environment(String zone) {
    var type =
        ROW(
            FIELD("d", DATE()),
            FIELD("tm", TIME(3)),
            FIELD("ts", TIMESTAMP(9)),
            FIELD("ltz", TIMESTAMP_LTZ(9)));
    var dates =
        Arrays.asList(
            "0001-01-01T00:00:00",
            "1582-10-04T12:34:56",
            "1899-12-31T23:59:59",
            "1900-01-01T00:00:00",
            "1969-12-31T23:59:59.999999999",
            "2000-02-29T12:34:56.123456789",
            "2024-01-31T00:00:00",
            "2024-03-10T02:30:00",
            "9999-12-31T23:59:59.999999999");
    var rows = new java.util.ArrayList<Row>();
    for (String text : dates) {
      var value = LocalDateTime.parse(text);
      rows.add(
          Row.of(
              value.toLocalDate(),
              value.toLocalTime(),
              value,
              value.toInstant(java.time.ZoneOffset.UTC)));
    }
    rows.add(Row.of(null, null, null, (Instant) null));
    return BuiltinFunctionParity.environment(type, rows, zone);
  }
}
