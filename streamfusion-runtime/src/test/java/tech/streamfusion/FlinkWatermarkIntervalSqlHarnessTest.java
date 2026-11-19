package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static tech.streamfusion.compat.FlinkTestSources.fromData;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.List;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.planner.NativePlanner;

/** Calendar delays must retain Flink's watermark arithmetic before downstream windows run. */
class FlinkWatermarkIntervalSqlHarnessTest {

  private static final String WINDOW_QUERY =
      "SELECT window_start, COUNT(*) FROM TABLE(TUMBLE(TABLE t, DESCRIPTOR(rt), "
          + "INTERVAL '1' DAY)) GROUP BY window_start, window_end";

  @ParameterizedTest
  @ValueSource(
      strings = {"INTERVAL '1' MONTH", "INTERVAL '1' YEAR", "INTERVAL '1-1' YEAR TO MONTH"})
  void calendarDelaysStayNativeWithoutDroppingEarlierWindows(String interval) throws Exception {
    assertNativeAssigner(environment(interval), WINDOW_QUERY);
    NativeParity.assertKindedParity(
        () -> environment(interval),
        WINDOW_QUERY,
        List.of(
            List.of("+I", LocalDateTime.of(2024, 1, 20, 0, 0), 1L),
            List.of("+I", LocalDateTime.of(2024, 2, 1, 0, 0), 1L)));
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "INTERVAL '1' MONTH - INTERVAL '1' DAY",
        "INTERVAL '1' DAY - INTERVAL '1' MONTH",
        "INTERVAL '1' MONTH - INTERVAL '1' MONTH"
      })
  void composedWatermarkExpressionsStayNativeIntoWindows(String intervals) throws Exception {
    assertNativeAssigner(environment(intervals), WINDOW_QUERY);
    NativeParity.assertParity(() -> environment(intervals), WINDOW_QUERY);
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "INTERVAL '31' DAY",
        "INTERVAL '1' HOUR",
        "INTERVAL '0' SECOND",
        "INTERVAL '1 02:03:04.005' DAY TO SECOND"
      })
  void dayTimeDelaysStayNativeAndMatchFlink(String interval) throws Exception {
    assertNativeAssigner(environment(interval), WINDOW_QUERY);
    NativeParity.assertParity(() -> environment(interval), WINDOW_QUERY);
  }

  @ParameterizedTest
  @ValueSource(strings = {"UTC", "Asia/Shanghai", "America/New_York"})
  void calendarWatermarksOnLocalZonedTimestampsMatchFlink(String zone) throws Exception {
    String interval = "INTERVAL '1' MONTH";
    // Non-UTC LTZ windows have a separate admission gate; exercise the assigner without that
    // window.
    String query = "SELECT rt FROM t";
    assertNativeAssigner(environment(interval, zone, true, originalRows()), query);
    NativeParity.assertParity(() -> environment(interval, zone, true, originalRows()), query);
  }

  @ParameterizedTest
  @ValueSource(strings = {"2023", "2024"})
  void monthEndClampingKeepsTheMaximumCandidateBeforeLateRows(String year) throws Exception {
    int y = Integer.parseInt(year);
    String query = WINDOW_QUERY.replace("INTERVAL '1' DAY", "INTERVAL '1' HOUR");
    LocalDateTime[] rows = {
      LocalDateTime.of(y, 3, 30, 23, 0),
      LocalDateTime.of(y, 3, 31, 0, 0),
      LocalDateTime.of(y, 3, 1, 12, 0).minusDays(1)
    };
    NativeParity.assertKindedParity(
        () -> environment("INTERVAL '1' MONTH", "UTC", false, rows),
        query,
        List.of(List.of("+I", rows[0], 1L), List.of("+I", rows[1], 1L)));
  }

  @ParameterizedTest
  @ValueSource(strings = {"INTERVAL '1' MONTH", "INTERVAL '1' SECOND"})
  void currentWatermarkAdvancesBetweenSortedRows(String interval) throws Exception {
    LocalDateTime[] rows = {
      LocalDateTime.of(2024, 3, 1, 0, 0), LocalDateTime.of(2024, 3, 2, 0, 0)
    };
    String query = "SELECT rt, CURRENT_WATERMARK(rt) FROM t";
    assertNativeAssigner(environment(interval, "UTC", false, rows), query);
    NativeParity.assertParity(() -> environment(interval, "UTC", false, rows), query);
  }

  private static void assertNativeAssigner(TableEnvironment table, String query) {
    NativePlanner.install(table);
    String plan = table.explainSql(query);
    assertTrue(plan.contains("NativeWatermarkAssigner"), plan);
  }

  private static TableEnvironment environment(String interval) {
    return environment(interval, "UTC", false, originalRows());
  }

  private static LocalDateTime[] originalRows() {
    return new LocalDateTime[] {
      LocalDateTime.of(2024, 2, 1, 0, 0), LocalDateTime.of(2024, 1, 20, 0, 0)
    };
  }

  private static TableEnvironment environment(
      String interval, String zone, boolean localZoned, LocalDateTime[] values) {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    env.getConfig().setAutoWatermarkInterval(200);
    StreamTableEnvironment table = StreamTableEnvironment.create(env);
    table.getConfig().setLocalTimeZone(ZoneId.of(zone));
    table.getConfig().set("table.optimizer.agg-phase-strategy", "ONE_PHASE");
    // Both rows survive a month of lateness; interpreting one month as 1 ms drops January.
    table.createTemporaryView(
        "t",
        fromData(
            env,
            Types.ROW_NAMED(
                new String[] {"rt"}, localZoned ? Types.INSTANT : Types.LOCAL_DATE_TIME),
            Arrays.stream(values)
                .map(value -> Row.of(localZoned ? value.toInstant(ZoneOffset.UTC) : value))
                .toArray(Row[]::new)),
        Schema.newBuilder()
            .column("rt", localZoned ? DataTypes.TIMESTAMP_LTZ(3) : DataTypes.TIMESTAMP(3))
            .watermark("rt", "rt - " + interval)
            .build());
    return table;
  }
}
