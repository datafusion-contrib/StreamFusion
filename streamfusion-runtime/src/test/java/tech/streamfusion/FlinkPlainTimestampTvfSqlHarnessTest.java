package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static tech.streamfusion.compat.FlinkTestSources.fromData;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Stream;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.planner.NativePlanner;

class FlinkPlainTimestampTvfSqlHarnessTest {
  static Stream<Arguments> shapesAndZones() {
    return Stream.of("TUMBLE", "HOP", "CUMULATE")
        .flatMap(
            shape ->
                Stream.of("UTC", "Asia/Shanghai", "America/Los_Angeles")
                    .map(zone -> Arguments.of(shape, zone)));
  }

  private static String tvf(String shape) {
    String intervals =
        shape.equals("TUMBLE")
            ? "INTERVAL '10' SECOND"
            : "INTERVAL '5' SECOND, INTERVAL '10' SECOND";
    return "TABLE(" + shape + "(TABLE src, DESCRIPTOR(ts), " + intervals + "))";
  }

  @ParameterizedTest
  @MethodSource("shapesAndZones")
  void standalonePlainTimestampPreservesWallClock(String shape, String zone) throws Exception {
    String sql = "SELECT id, ts, window_start, window_end, window_time FROM " + tvf(shape);
    var types = environment(zone, false).sqlQuery(sql).getResolvedSchema().getColumnDataTypes();
    for (int i = 1; i < types.size(); i++) {
      assertEquals(
          LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE, types.get(i).getLogicalType().getTypeRoot());
      assertEquals(3, ((TimestampType) types.get(i).getLogicalType()).getPrecision());
    }
    assertNativeParity(() -> environment(zone, false), sql, "NativeWindowTableFunction");
  }

  @Test
  void localZonedUtcMatchesHostBoundaries() throws Exception {
    assertNativeParity(
        () -> environment("UTC", true),
        "SELECT id, ts, window_start, window_end, window_time FROM " + tvf("TUMBLE"),
        "NativeWindowTableFunction");
  }

  @ParameterizedTest
  @MethodSource("shapesAndZones")
  void downstreamRankConsumesPlainTimestampWindows(String shape, String zone) throws Exception {
    assertNativeParity(
        () -> environment(zone, false),
        "SELECT id, k, window_start, window_end FROM (SELECT *, ROW_NUMBER() OVER "
            + "(PARTITION BY window_start, window_end, k ORDER BY id DESC) AS rn FROM "
            + tvf(shape)
            + ") WHERE rn <= 2",
        "NativeWindowRank");
  }

  @ParameterizedTest
  @MethodSource("shapesAndZones")
  void downstreamDedupConsumesPlainTimestampWindows(String shape, String zone) throws Exception {
    for (String direction : List.of("ASC", "DESC")) {
      assertNativeParity(
          () -> environment(zone, false),
          "SELECT id, k, window_start, window_end FROM (SELECT *, ROW_NUMBER() OVER "
              + "(PARTITION BY window_start, window_end, k ORDER BY window_time "
              + direction
              + ") AS rn FROM "
              + tvf(shape)
              + ") WHERE rn = 1",
          "NativeWindowRank");
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"TUMBLE", "HOP"})
  void nonzeroOffsetFallsBack(String shape) throws Exception {
    String table = tvf(shape);
    table = table.substring(0, table.length() - 2) + ", INTERVAL '1' SECOND))";
    NativeParity.assertFallbackReasonContains(
        () -> environment("UTC", false), "SELECT * FROM " + table, "zero offset");
  }

  @ParameterizedTest
  @MethodSource("shapesAndZones")
  void downstreamAggregateConsumesPlainTimestampWindows(String shape, String zone)
      throws Exception {
    assertNativeParity(
        () -> environment(zone, false, false),
        "SELECT k, window_start, window_end, COUNT(*), SUM(id) FROM "
            + tvf(shape)
            + " GROUP BY k, window_start, window_end",
        "NativeColumnarGlobalWindowAggregate");
  }

  @Test
  void downstreamJoinConsumesPlainTimestampWindows() throws Exception {
    assertNativeParity(
        () -> environment("America/Los_Angeles", false),
        "SELECT a.id, a.window_start, a.window_end FROM (SELECT * FROM "
            + tvf("TUMBLE")
            + ") a JOIN (SELECT * FROM "
            + tvf("TUMBLE")
            + ") b ON a.id = b.id "
            + "AND a.window_start = b.window_start AND a.window_end = b.window_end",
        "NativeWindowJoin");
  }

  private static void assertNativeParity(
      Supplier<TableEnvironment> environment, String sql, String operator) throws Exception {
    String plan = NativePlanner.explain(environment.get(), sql);
    assertTrue(plan.contains(operator), () -> "Missing " + operator + ": " + plan);
    NativeParity.assertParity(environment, sql);
  }

  private static TableEnvironment environment(String zone, boolean ltz) {
    return environment(zone, ltz, true);
  }

  private static TableEnvironment environment(String zone, boolean ltz, boolean includeNull) {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    StreamTableEnvironment table = StreamTableEnvironment.create(env);
    table.getConfig().set("table.local-time-zone", zone);
    String[] samples = {
      "0001-01-01T00:00:00.123456789",
      "1969-12-31T23:59:59.999999999",
      "1970-01-01T00:00:00",
      "1970-01-01T00:00:04.999999999",
      "1970-01-01T00:00:05",
      "1970-01-01T00:00:09.999999999",
      "1970-01-01T00:00:10",
      "2021-03-14T02:30:00",
      "9999-12-31T23:59:49.999999999",
      null
    };
    List<Row> rows = new ArrayList<>();
    for (int i = 0; i < samples.length; i++) {
      if (samples[i] == null && !includeNull) {
        continue;
      }
      LocalDateTime ts = samples[i] == null ? null : LocalDateTime.parse(samples[i]);
      rows.add(
          Row.of(i, (long) (i % 2), ts == null ? null : ltz ? ts.toInstant(ZoneOffset.UTC) : ts));
    }
    table.createTemporaryView(
        "src",
        fromData(
                env,
                rows,
                Types.ROW_NAMED(
                    new String[] {"id", "k", "ts"},
                    Types.INT,
                    Types.LONG,
                    ltz ? Types.INSTANT : Types.LOCAL_DATE_TIME))
            .assignTimestampsAndWatermarks(
                WatermarkStrategy.<Row>forBoundedOutOfOrderness(Duration.ofDays(1))
                    .withTimestampAssigner(
                        (row, previous) -> {
                          Object value = row.getField(2);
                          return value == null
                              ? 0
                              : ltz
                                  ? ((Instant) value).toEpochMilli()
                                  : ((LocalDateTime) value)
                                      .toInstant(ZoneOffset.UTC)
                                      .toEpochMilli();
                        })),
        Schema.newBuilder()
            .column("id", DataTypes.INT())
            .column("k", DataTypes.BIGINT())
            .column("ts", ltz ? DataTypes.TIMESTAMP_LTZ(3) : DataTypes.TIMESTAMP(3))
            .watermark("ts", "SOURCE_WATERMARK()")
            .build());
    return table;
  }
}
