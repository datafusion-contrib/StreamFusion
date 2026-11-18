package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;
import java.time.ZoneId;
import java.util.List;
import java.util.stream.Stream;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import tech.streamfusion.compat.FlinkTestSources;

class FlinkCalendarWindowSqlHarnessTest {
  static Stream<Arguments> cases() {
    return Stream.of(
            System.getProperty("calendar.test.zones", "GMT+05:45,America/Los_Angeles,Asia/Shanghai")
                .split(","))
        .flatMap(
            zone ->
                Stream.of("TUMBLE", "HOP", "CUMULATE")
                    .flatMap(
                        shape ->
                            Stream.of(
                                    System.getProperty(
                                            "calendar.test.consumers",
                                            "tvf,aggregate,two_phase,join,rank,dedup")
                                        .split(","))
                                .map(consumer -> Arguments.of(zone, shape, consumer))));
  }

  @ParameterizedTest
  @MethodSource("cases")
  void calendarConsumersExecuteNativelyWithExactReleasedResults(
      String zone, String shape, String consumer) {
    String window =
        shape
            + "(TABLE src, DESCRIPTOR(rt), "
            + (shape.equals("TUMBLE") ? "INTERVAL '1' DAY" : "INTERVAL '6' HOUR, INTERVAL '1' DAY")
            + ")";
    String input = "TABLE(" + window + ")";
    String sql;
    if (consumer.equals("tvf"))
      sql = "SELECT k, v, rt, window_start, window_end, window_time FROM " + input;
    else if (consumer.equals("aggregate") || consumer.equals("two_phase"))
      sql =
          "SELECT k, window_start, window_end, SUM(v), COUNT(*) FROM "
              + input
              + " GROUP BY k, window_start, window_end";
    else if (consumer.equals("join"))
      sql =
          "SELECT l.k, l.v, r.v, l.window_start, l.window_end FROM "
              + "(SELECT * FROM "
              + input
              + ")"
              + " l JOIN "
              + "(SELECT * FROM "
              + input.replace("TABLE src, DESCRIPTOR(rt)", "TABLE rhs, DESCRIPTOR(right_rt)")
              + ")"
              + " r ON l.k = r.k AND l.window_start = r.window_start AND l.window_end ="
              + " r.window_end";
    else
      sql =
          "SELECT k, v, window_start, window_end, rn FROM (SELECT *, ROW_NUMBER() OVER (PARTITION"
              + " BY k, window_start, window_end ORDER BY "
              + (consumer.equals("dedup") ? "rt DESC" : "v DESC")
              + ") rn FROM "
              + input
              + ") WHERE rn <= "
              + (consumer.equals("dedup") ? "1" : "2");
    var runs =
        NativeFailureParity.run(
            () -> environment(zone, consumer.equals("two_phase"), consumer.equals("join")), sql);
    runs.assertSuccess(NativeFailureParity.Route.NATIVE);
    assertEquals(runs.host().resultTypes(), runs.nativeRun().resultTypes());
    String required =
        consumer.equals("join")
            ? "NativeWindowJoin"
            : consumer.equals("rank") || consumer.equals("dedup")
                ? "NativeWindowRank"
                : consumer.equals("tvf")
                    ? "NativeWindowTableFunction"
                    : "NativeColumnarWindowAggregate";
    if (consumer.equals("two_phase")) required = "NativeColumnarGlobalWindowAggregate";
    String expectedOperator = required;
    assertTrue(
        runs.nativeRun().plan().contains(expectedOperator),
        () -> "Missing " + expectedOperator + ": " + runs.nativeRun().plan());
  }

  @ParameterizedTest
  @org.junit.jupiter.params.provider.ValueSource(
      strings = {"GMT+05:45", "America/Los_Angeles", "Asia/Shanghai"})
  void sessionConnectivityUsesTheRegionalAssignmentDomain(String zone) {
    var runs =
        NativeFailureParity.run(
            () -> environment(zone, false, false),
            "SELECT k, SESSION_START(rt, INTERVAL '1' HOUR) AS window_start, SESSION_END(rt,"
                + " INTERVAL '1' HOUR) AS window_end, SUM(v), COUNT(*) FROM src GROUP BY k,"
                + " SESSION(rt, INTERVAL '1' HOUR)");
    runs.assertSuccess(NativeFailureParity.Route.NATIVE);
    assertEquals(runs.host().resultTypes(), runs.nativeRun().resultTypes());
    assertTrue(
        runs.nativeRun().plan().contains("NativeColumnarSessionWindowAggregate"),
        runs.nativeRun().plan());
  }

  @ParameterizedTest
  @org.junit.jupiter.params.provider.ValueSource(strings = {"HOP", "CUMULATE"})
  void sharedCalendarMergesPreserveDistinctAndBuiltinState(String shape) {
    for (boolean twoPhase : new boolean[] {false, true}) {
      var runs =
          NativeFailureParity.run(
              () -> environment("America/Los_Angeles", twoPhase, false),
              "SELECT k, window_start, window_end, SUM(v), COUNT(*), MIN(v), MAX(v),"
                  + " COUNT(DISTINCT CAST(v AS STRING)) FROM TABLE("
                  + shape
                  + "(TABLE src, DESCRIPTOR(rt), INTERVAL '6' HOUR, INTERVAL '1' DAY))"
                  + " GROUP BY k, window_start, window_end");
      runs.assertSuccess(NativeFailureParity.Route.NATIVE);
      assertEquals(runs.host().resultTypes(), runs.nativeRun().resultTypes());
      assertTrue(
          runs.nativeRun()
              .plan()
              .contains(
                  twoPhase
                      ? "NativeColumnarGlobalWindowAggregate"
                      : "NativeColumnarWindowAggregate"),
          runs.nativeRun().plan());
    }
  }

  @ParameterizedTest
  @org.junit.jupiter.params.provider.ValueSource(strings = {"GMT+05:45", "America/Los_Angeles"})
  void processingTimeCalendarPlansCarryTheSameNativeAssignment(String zone) {
    for (String shape : List.of("TUMBLE", "HOP", "CUMULATE")) {
      var table = environment(zone, false, false);
      table.executeSql(
          "CREATE TEMPORARY VIEW processing_input AS SELECT *, PROCTIME() AS pt FROM src");
      String input =
          "TABLE("
              + shape
              + "(TABLE processing_input, DESCRIPTOR(pt), "
              + (shape.equals("TUMBLE")
                  ? "INTERVAL '1' DAY"
                  : "INTERVAL '6' HOUR, INTERVAL '1' DAY")
              + "))";
      String assignment =
          tech.streamfusion.planner.NativePlanner.explain(
              table, "SELECT k, window_start, window_end FROM " + input);
      assertTrue(assignment.contains("NativeWindowTableFunction"), assignment);
      String aggregate =
          tech.streamfusion.planner.NativePlanner.explain(
              table,
              "SELECT k, window_start, window_end, SUM(v), COUNT(*) FROM "
                  + input
                  + " GROUP BY k, window_start, window_end");
      assertTrue(aggregate.contains("NativeColumnarWindowAggregate"), aggregate);
      String rank =
          tech.streamfusion.planner.NativePlanner.explain(
              table,
              "SELECT k, v, window_start, window_end, rn FROM (SELECT *, ROW_NUMBER() OVER"
                  + " (PARTITION BY k, window_start, window_end ORDER BY v DESC) rn FROM "
                  + input
                  + ") WHERE rn <= 2");
      assertTrue(rank.contains("NativeWindowRank"), rank);
    }
  }

  static Stream<Arguments> recoveryCases() {
    return Stream.of("TUMBLE", "HOP", "CUMULATE")
        .flatMap(shape -> Stream.of(false, true).map(rocks -> Arguments.of(shape, rocks)));
  }

  @ParameterizedTest
  @MethodSource("recoveryCases")
  void regionalCalendarWindowsSurviveTwoCompletedCheckpoints(String shape, boolean rocks) {
    var rows =
        java.util.stream.IntStream.range(0, 12)
            .mapToObj(
                i -> Row.of(i % 3, (long) i, Instant.ofEpochMilli(1710060600000L + i * 600000L)))
            .collect(java.util.stream.Collectors.toList());
    var schema =
        Schema.newBuilder()
            .column("k", DataTypes.INT())
            .column("v", DataTypes.BIGINT())
            .column("rt", DataTypes.TIMESTAMP_LTZ(3))
            .watermark("rt", "SOURCE_WATERMARK()")
            .build();
    String backend = rocks ? "tech.streamfusion.state.RocksDBNativeStateBackendFactory" : "hashmap";
    try (var recovery =
        new PortableSqlRecovery(
                backend,
                rows,
                Types.ROW_NAMED(
                    new String[] {"k", "v", "rt"}, Types.INT, Types.LONG, Types.INSTANT),
                schema,
                false,
                4,
                8)
            .withExecutionMetrics()
            .withWatermarks(java.util.Map.of(10, 1710136800000L, 12, 1710223200000L))) {
      java.util.function.UnaryOperator<org.apache.flink.table.api.TableEnvironment> configure =
          table -> {
            table.getConfig().setLocalTimeZone(ZoneId.of("America/Los_Angeles"));
            table.getConfig().set("table.optimizer.agg-phase-strategy", "ONE_PHASE");
            table.getConfig().set("streamfusion.transpose.batchRows", "2");
            return table;
          };
      String window =
          shape
              + "(TABLE recovery_input, DESCRIPTOR(rt), "
              + (shape.equals("TUMBLE")
                  ? "INTERVAL '1' DAY"
                  : "INTERVAL '6' HOUR, INTERVAL '1' DAY")
              + ")";
      var runs =
          NativeFailureParity.runRecovery(
              () -> configure.apply(recovery.uninterrupted()),
              () -> configure.apply(recovery.get()),
              "SELECT k, window_start, window_end, SUM(v), COUNT(*) FROM TABLE("
                  + window
                  + ") GROUP BY k, window_start, window_end");
      runs.assertSuccess(NativeFailureParity.Route.NATIVE);
      recovery.verifyRepeatedRecovery();
      RecoveryExecutionMetrics.assertConsumed(
          recovery.nativeExecution(), "NativeColumnarWindowAggregate");
    }
  }

  private static StreamTableEnvironment environment(String zone, boolean twoPhase, boolean join) {
    var env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    var table = StreamTableEnvironment.create(env);
    table.getConfig().setLocalTimeZone(ZoneId.of(zone));
    table
        .getConfig()
        .set("table.optimizer.agg-phase-strategy", twoPhase ? "TWO_PHASE" : "ONE_PHASE");
    table.getConfig().set("table.exec.mini-batch.enabled", join ? "false" : "true");
    table.getConfig().set("table.exec.mini-batch.allow-latency", "10 ms");
    table.getConfig().set("table.exec.mini-batch.size", "2");
    table.getConfig().set("streamfusion.transpose.batchRows", "2");
    var rows =
        List.of(
            Row.of(1, 10L, Instant.parse("1969-12-31T23:30:00Z")),
            Row.of(1, 20L, Instant.parse("2024-03-10T09:50:00Z")),
            Row.of(1, 30L, Instant.parse("2024-03-10T10:10:00Z")),
            Row.of(2, 40L, Instant.parse("2024-11-03T08:30:00Z")),
            Row.of(2, 50L, Instant.parse("2024-11-03T09:30:00Z")));
    table.createTemporaryView(
        "src",
        FlinkTestSources.fromData(
            env,
            rows,
            Types.ROW_NAMED(new String[] {"k", "v", "rt"}, Types.INT, Types.LONG, Types.INSTANT)),
        Schema.newBuilder()
            .column("k", DataTypes.INT())
            .column("v", DataTypes.BIGINT())
            .column("rt", DataTypes.TIMESTAMP_LTZ(3))
            .watermark("rt", "rt - INTERVAL '1' DAY")
            .build());
    table.createTemporaryView(
        "rhs",
        FlinkTestSources.fromData(
            env,
            rows,
            Types.ROW_NAMED(
                new String[] {"k", "v", "right_rt"}, Types.INT, Types.LONG, Types.INSTANT)),
        Schema.newBuilder()
            .column("k", DataTypes.INT())
            .column("v", DataTypes.BIGINT())
            .column("right_rt", DataTypes.TIMESTAMP_LTZ(3))
            .watermark("right_rt", "right_rt - INTERVAL '1' DAY")
            .build());
    return table;
  }
}
