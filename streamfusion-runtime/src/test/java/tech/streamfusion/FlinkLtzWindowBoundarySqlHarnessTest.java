package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static tech.streamfusion.compat.FlinkTestSources.fromData;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.stream.Stream;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.types.Row;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.planner.NativePlanner;

class FlinkLtzWindowBoundarySqlHarnessTest {
  static Stream<Arguments> shapesAndZones() {
    return Stream.of("TUMBLE", "HOP", "CUMULATE")
        .flatMap(
            shape ->
                Stream.of("UTC", "GMT+08:00", "GMT-05:30").map(zone -> Arguments.of(shape, zone)));
  }

  private static String tvf(String shape) {
    return "TABLE("
        + shape
        + "(TABLE src, DESCRIPTOR(ts), "
        + (shape.equals("TUMBLE")
            ? "INTERVAL '10' SECOND"
            : "INTERVAL '5' SECOND, INTERVAL '10' SECOND")
        + "))";
  }

  @ParameterizedTest
  @MethodSource("shapesAndZones")
  void standaloneLtzHasLocalBoundsAndInstantRowtime(String shape, String zone) throws Exception {
    String sql = "SELECT id, ts, window_start, window_end, window_time FROM " + tvf(shape);
    var types = environment(zone, true).sqlQuery(sql).getResolvedSchema().getColumnDataTypes();
    assertEquals(
        LogicalTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE,
        types.get(1).getLogicalType().getTypeRoot());
    assertEquals(
        LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE, types.get(2).getLogicalType().getTypeRoot());
    assertEquals(
        LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE, types.get(3).getLogicalType().getTypeRoot());
    assertEquals(
        LogicalTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE,
        types.get(4).getLogicalType().getTypeRoot());
    parity(zone, true, sql, "NativeWindowTableFunction");
  }

  @ParameterizedTest
  @MethodSource("shapesAndZones")
  void rankPreservesLocalBoundaries(String shape, String zone) throws Exception {
    parity(
        zone,
        true,
        "SELECT id, window_start, window_end, window_time FROM (SELECT *, ROW_NUMBER() OVER"
            + " (PARTITION BY window_start, window_end, k ORDER BY id DESC) rn FROM "
            + tvf(shape)
            + ") WHERE rn <= 2",
        "NativeWindowRank");
  }

  @ParameterizedTest
  @MethodSource("shapesAndZones")
  void dedupPreservesLocalBoundaries(String shape, String zone) throws Exception {
    parity(
        zone,
        true,
        "SELECT id, window_start, window_end, window_time FROM (SELECT *, ROW_NUMBER() OVER"
            + " (PARTITION BY window_start, window_end, k ORDER BY window_time ASC) rn FROM "
            + tvf(shape)
            + ") WHERE rn = 1",
        "NativeWindowRank");
  }

  @ParameterizedTest
  @MethodSource("shapesAndZones")
  void joinPreservesLocalBoundaries(String shape, String zone) throws Exception {
    parity(
        zone,
        true,
        "SELECT a.id, a.window_start, a.window_end, b.window_time FROM (SELECT * FROM "
            + tvf(shape)
            + ") a JOIN (SELECT * FROM "
            + tvf(shape)
            + ") b ON a.id=b.id "
            + "AND a.window_start=b.window_start AND a.window_end=b.window_end",
        "NativeWindowJoin");
  }

  @ParameterizedTest
  @MethodSource("shapesAndZones")
  void fusedAggregateStillMatchesHost(String shape, String zone) throws Exception {
    parity(
        zone,
        false,
        "SELECT k, window_start, window_end, COUNT(*), SUM(id) FROM "
            + tvf(shape)
            + " GROUP BY k, window_start, window_end",
        "NativeColumnarGlobalWindowAggregate");
  }

  @ParameterizedTest
  @ValueSource(strings = {"UTC", "GMT+08:00", "GMT-05:30"})
  void boundaryExpressionsObserveLocalValues(String zone) throws Exception {
    parity(
        zone,
        true,
        "SELECT id, CAST(window_start AS STRING), EXTRACT(HOUR FROM window_end) FROM "
            + tvf("TUMBLE")
            + " WHERE window_start >= TIMESTAMP '1970-01-01 00:00:00'",
        "NativeWindowTableFunction");
  }

  @ParameterizedTest
  @ValueSource(strings = {"Asia/Kolkata", "America/Los_Angeles"})
  void historicalOrRecurringOffsetChangesMatchHost(String zone) throws Exception {
    parity(zone, true, "SELECT * FROM " + tvf("TUMBLE"), "NativeWindowTableFunction");
  }

  @ParameterizedTest
  @ValueSource(strings = {"UTC", "GMT+08:00", "GMT-05:30"})
  void rankConsumesAlreadyRenderedAggregateBounds(String zone) throws Exception {
    parity(
        zone,
        false,
        "SELECT k, n, window_start, window_end FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY"
            + " window_start, window_end ORDER BY n DESC, k ASC) rn FROM (SELECT k, window_start,"
            + " window_end, window_time, COUNT(*) n FROM "
            + tvf("TUMBLE")
            + " GROUP BY k, window_start, window_end, window_time)) WHERE rn <= 1",
        "NativeWindowRank");
  }

  private static void parity(String zone, boolean includeNull, String sql, String operator)
      throws Exception {
    String plan = NativePlanner.explain(environment(zone, includeNull), sql);
    assertTrue(plan.contains(operator), () -> "Missing " + operator + ": " + plan);
    NativeParity.assertParity(() -> environment(zone, includeNull), sql);
  }

  private static TableEnvironment environment(String zone, boolean includeNull) {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    StreamTableEnvironment table = StreamTableEnvironment.create(env);
    table.getConfig().set("table.local-time-zone", zone);
    List<Row> rows = new java.util.ArrayList<>();
    String[] samples = {
      "0001-01-01T00:00:00.123456789Z",
      "1969-12-31T23:59:59.999999999Z",
      "1970-01-01T00:00:00Z",
      "1970-01-01T00:00:04.999999999Z",
      "1970-01-01T00:00:05Z",
      "1970-01-01T00:00:09.999999999Z",
      "1970-01-01T00:00:10Z",
      "2021-03-14T02:30:00Z",
      "9999-12-31T23:59:49.999999999Z"
    };
    for (int i = 0; i < samples.length; i++) {
      rows.add(Row.of(i, (long) (i % 2), Instant.parse(samples[i])));
    }
    if (includeNull) rows.add(Row.of(9, 1L, null));
    table.createTemporaryView(
        "src",
        fromData(
                env,
                rows,
                Types.ROW_NAMED(
                    new String[] {"id", "k", "ts"}, Types.INT, Types.LONG, Types.INSTANT))
            .assignTimestampsAndWatermarks(
                WatermarkStrategy.<Row>forBoundedOutOfOrderness(Duration.ofDays(1))
                    .withTimestampAssigner(
                        (row, previous) ->
                            row.getField(2) == null
                                ? 0
                                : ((Instant) row.getField(2)).toEpochMilli())),
        Schema.newBuilder()
            .column("id", DataTypes.INT())
            .column("k", DataTypes.BIGINT())
            .column("ts", DataTypes.TIMESTAMP_LTZ(3))
            .watermark("ts", "SOURCE_WATERMARK()")
            .build());
    return table;
  }
}
