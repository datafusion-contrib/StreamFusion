package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class FlinkWindowAssignmentOffsetSqlHarnessTest {
  static Stream<Arguments> fixtures() {
    return Stream.of("ONE_PHASE", "TWO_PHASE")
        .flatMap(
            phase ->
                Stream.of("TUMBLE", "HOP", "CUMULATE")
                    .flatMap(
                        shape ->
                            Stream.of("UTC", "Asia/Kathmandu", "America/New_York", "PLAIN")
                                .map(
                                    zone ->
                                        Arguments.of(
                                            phase,
                                            shape,
                                            zone,
                                            zone.equals("Asia/Kathmandu") ? -1 : 1))));
  }

  @ParameterizedTest
  @MethodSource("fixtures")
  void offsetsKeepAssignmentSeparateFromBoundaryRendering(
      String phase, String shape, String zone, int offset) {
    try (var metrics = new RecoveryExecutionMetrics()) {
      var runs =
          NativeFailureParity.run(
              () -> environment(metrics, phase, zone), query(shape, offset, "src"));
      runs.assertSuccess(NativeFailureParity.Route.NATIVE);
      assertEquals(runs.host().resultTypes(), runs.nativeRun().resultTypes());
      assertWork(metrics.nativeOperators(), phase);
    }
  }

  private static StreamTableEnvironment environment(
      RecoveryExecutionMetrics metrics, String phase, String zone) {
    var env = metrics.environment(new Configuration(), 1, 0);
    var table = StreamTableEnvironment.create(env);
    configure(table, phase, zone);
    var input =
        env.fromSequence(0, 95)
            .map(
                i ->
                    Row.of(
                        i % 4 == 0 ? null : (int) (i % 4),
                        i % 7 == 0 ? null : i % 13 - 6,
                        i % 5 == 0 ? null : "v" + i % 3,
                        -12000L + i * 400L,
                        java.time.LocalDateTime.ofInstant(
                            Instant.ofEpochMilli(-12000L + i * 400L), java.time.ZoneOffset.UTC)))
            .returns(
                Types.ROW_NAMED(
                    new String[] {"k", "v", "s", "ts", zone.equals("PLAIN") ? "rt" : "wall"},
                    Types.INT,
                    Types.LONG,
                    Types.STRING,
                    Types.LONG,
                    Types.LOCAL_DATE_TIME))
            .assignTimestampsAndWatermarks(
                WatermarkStrategy.<Row>forBoundedOutOfOrderness(Duration.ofDays(1))
                    .withTimestampAssigner((r, previous) -> (Long) r.getField(3)));
    var schema =
        Schema.newBuilder()
            .column("k", DataTypes.INT())
            .column("v", DataTypes.BIGINT())
            .column("s", DataTypes.STRING())
            .column("ts", DataTypes.BIGINT())
            .column(zone.equals("PLAIN") ? "rt" : "wall", DataTypes.TIMESTAMP(3));
    if (!zone.equals("PLAIN")) schema.columnByMetadata("rt", DataTypes.TIMESTAMP_LTZ(3), "rowtime");
    table.createTemporaryView("src", input, schema.watermark("rt", "SOURCE_WATERMARK()").build());
    return table;
  }

  @ParameterizedTest
  @org.junit.jupiter.params.provider.CsvSource({
    "TUMBLE,UTC",
    "TUMBLE,PLAIN",
    "HOP,Asia/Kathmandu",
    "HOP,PLAIN",
    "CUMULATE,America/New_York",
    "CUMULATE,PLAIN"
  })
  void standaloneOffsetKeepsPayloadAndWindowTime(String shape, String zone) {
    try (var metrics = new RecoveryExecutionMetrics()) {
      String sql =
          "SELECT k, v, s, rt, window_start, window_end, window_time FROM TABLE("
              + window(shape, -1, "src")
              + ")";
      var runs = NativeFailureParity.run(() -> environment(metrics, "ONE_PHASE", zone), sql);
      runs.assertSuccess(NativeFailureParity.Route.NATIVE);
      assertEquals(runs.host().resultTypes(), runs.nativeRun().resultTypes());
      RecoveryExecutionMetrics.assertConsumed(
          metrics.nativeOperators(), "NativeWindowTableFunction");
    }
  }

  static Stream<Arguments> recoveryFixtures() {
    return Stream.of("ONE_PHASE", "TWO_PHASE")
        .flatMap(
            phase ->
                Stream.of("hashmap", "tech.streamfusion.state.RocksDBNativeStateBackendFactory")
                    .map(backend -> Arguments.of(phase, backend)));
  }

  @ParameterizedTest
  @MethodSource("recoveryFixtures")
  void offsetStateSurvivesTwoRestores(String phase, String backend) throws Exception {
    recover(phase, backend, false);
  }

  @ParameterizedTest
  @MethodSource("recoveryFixtures")
  void offsetAttachedFiltersSurviveTwoRestores(String phase, String backend) throws Exception {
    recover(phase, backend, true);
  }

  private static void recover(String phase, String backend, boolean attached) throws Exception {
    var rows =
        java.util.stream.IntStream.range(0, 48)
            .mapToObj(
                i ->
                    Row.of(
                        i % 3,
                        i % 7 == 0 ? null : (long) (i % 13 - 6),
                        i % 5 == 0 ? null : "v" + i % 3,
                        Instant.ofEpochMilli(
                            Instant.parse("2024-11-03T05:59:50Z").toEpochMilli() + i * 400L)))
            .collect(java.util.stream.Collectors.toList());
    var schema =
        Schema.newBuilder()
            .column("k", DataTypes.INT())
            .column("v", DataTypes.BIGINT())
            .column("s", DataTypes.STRING())
            .column("rt", DataTypes.TIMESTAMP_LTZ(3))
            .watermark("rt", "SOURCE_WATERMARK()")
            .build();
    try (var recovery =
        new PortableSqlRecovery(
                backend,
                rows,
                Types.ROW_NAMED(
                    new String[] {"k", "v", "s", "rt"},
                    Types.INT,
                    Types.LONG,
                    Types.STRING,
                    Types.INSTANT),
                schema,
                false,
                16,
                32)
            .withExecutionMetrics()
            .withWatermarks(Map.of(48, Long.MAX_VALUE))) {
      String sql = query("CUMULATE", -1, "recovery_input");
      if (attached)
        sql =
            "SELECT window_start, window_end, MAX(vsum) FILTER (WHERE vsum > 0),"
                + " COUNT(*) FILTER (WHERE k > 9), COUNT(vsum) FILTER (WHERE k < 2) FROM ("
                + sql
                + ") counts GROUP BY window_start, window_end";
      var runs =
          NativeFailureParity.runRecovery(
              () -> configure(recovery.uninterrupted(), phase, "America/New_York"),
              () -> configure(recovery.get(), phase, "America/New_York"),
              sql);
      runs.assertSuccess(NativeFailureParity.Route.NATIVE);
      assertEquals(runs.host().resultTypes(), runs.nativeRun().resultTypes());
      recovery.verifyRepeatedRecovery();
      assertWork(recovery.nativeExecution(), phase);
    }
  }

  private static TableEnvironment configure(TableEnvironment table, String phase, String zone) {
    table.getConfig().setLocalTimeZone(ZoneId.of(zone.equals("PLAIN") ? "Asia/Kathmandu" : zone));
    table.getConfig().set("table.optimizer.agg-phase-strategy", phase);
    table.getConfig().set("streamfusion.transpose.batchRows", "7");
    return table;
  }

  private static String query(String shape, int offset, String source) {
    return "SELECT k, window_start, window_end, SUM(v) AS vsum, AVG(v), COUNT(DISTINCT s),"
        + " MIN(s) FILTER (WHERE v > 0), COUNT(*) FROM TABLE("
        + window(shape, offset, source)
        + ") GROUP BY k, window_start, window_end";
  }

  private static String window(String shape, int offset, String source) {
    String sizes =
        shape.equals("TUMBLE") ? "INTERVAL '9' SECOND" : "INTERVAL '3' SECOND, INTERVAL '9' SECOND";
    return shape
        + "(TABLE "
        + source
        + ", DESCRIPTOR(rt), "
        + sizes
        + ", INTERVAL '"
        + offset
        + "' SECOND)";
  }

  private static void assertWork(java.util.List<Map<String, Object>> operators, String phase) {
    RecoveryExecutionMetrics.assertConsumed(
        operators,
        phase.equals("TWO_PHASE")
            ? "NativeColumnarGlobalWindowAggregate"
            : "NativeColumnarWindowAggregate");
    if (phase.equals("TWO_PHASE")) {
      RecoveryExecutionMetrics.assertConsumed(operators, "NativeColumnarLocalWindowAggregate");
    }
  }
}
