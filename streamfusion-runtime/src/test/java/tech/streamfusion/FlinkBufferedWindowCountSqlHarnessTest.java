package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.*;
import static tech.streamfusion.compat.FlinkTestSources.fromData;

import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
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

class FlinkBufferedWindowCountSqlHarnessTest {
  private static final long START = Instant.parse("2020-01-01T00:00:00Z").toEpochMilli();

  static Stream<Arguments> countCases() {
    return Stream.of("ONE_PHASE", "TWO_PHASE")
        .flatMap(
            phase ->
                Stream.of("TUMBLE", "HOP", "CUMULATE")
                    .flatMap(
                        shape ->
                            Stream.of(false, true)
                                .flatMap(
                                    rocks ->
                                        Stream.of(10, 30)
                                            .flatMap(
                                                seconds -> {
                                                  var overrides =
                                                      phase.equals("ONE_PHASE")
                                                              && !shape.equals("TUMBLE")
                                                              && seconds == 30
                                                          ? Stream.of(false, true)
                                                          : Stream.of(false);
                                                  return overrides.map(
                                                      optIn ->
                                                          Arguments.of(
                                                              phase, shape, rocks, seconds, optIn));
                                                }))));
  }

  @ParameterizedTest
  @MethodSource("countCases")
  void countCascadesMatchTypedResultsAcrossBatchesAndBackends(
      String phase, String shape, boolean rocks, int seconds, boolean optIn) {
    List<Row> input =
        IntStream.range(0, 4099)
            .mapToObj(
                i ->
                    Row.of(
                        i % 11 == 0 ? null : i % 67,
                        i % 7 == 0 ? null : (long) i,
                        Instant.ofEpochMilli(START + i * 25L)))
            .collect(Collectors.toList());
    try (var metrics = new RecoveryExecutionMetrics()) {
      var runs =
          NativeFailureParity.run(
              () -> {
                var table = environment(metrics, input, phase, rocks);
                if (optIn)
                  table
                      .getConfig()
                      .set("streamfusion.window.attached.single-phase.enabled", "true");
                return table;
              },
              query("src", shape, seconds));
      runs.assertSuccess(NativeFailureParity.Route.NATIVE);
      assertEquals(runs.host().resultTypes(), runs.nativeRun().resultTypes());
      assertFalse(runs.host().rows().isEmpty());
      assertWindowsConsumed(metrics.nativeOperators());
    }
  }

  static Stream<Arguments> recoveryCases() {
    return Stream.of("ONE_PHASE", "TWO_PHASE")
        .flatMap(
            phase ->
                Stream.of("TUMBLE", "HOP", "CUMULATE")
                    .flatMap(
                        shape ->
                            Stream.of(false, true)
                                .map(rocks -> Arguments.of(phase, shape, rocks))));
  }

  static Stream<Arguments> nestedCases() {
    return Stream.of("ONE_PHASE", "TWO_PHASE")
        .flatMap(phase -> Stream.of(false, true).map(rocks -> Arguments.of(phase, rocks)));
  }

  @ParameterizedTest
  @MethodSource("nestedCases")
  void threeWindowStagesPreserveCountsAndConsumeInput(String phase, boolean rocks) {
    List<Row> input =
        IntStream.range(0, 1031)
            .mapToObj(
                i ->
                    Row.of(
                        i % 13,
                        i % 7 == 0 ? null : (long) i,
                        Instant.ofEpochMilli(START + i * 25L)))
            .collect(Collectors.toList());
    String sql =
        "SELECT MAX(n), SUM(n), AVG(n), COUNT(*), wstart, wend FROM ("
            + "SELECT MOD(k, 3) bucket, COUNT(CASE WHEN first_count > 2 THEN NULL"
            + " ELSE first_count END) n, wstart, wend FROM ("
            + "SELECT k, COUNT(v) first_count, window_start wstart, window_end wend FROM TABLE("
            + "HOP(TABLE src, DESCRIPTOR(rt), INTERVAL '2' SECOND, INTERVAL '10' SECOND))"
            + " GROUP BY k, window_start, window_end) first_stage"
            + " GROUP BY MOD(k, 3), wstart, wend) second_stage GROUP BY wstart, wend";
    try (var metrics = new RecoveryExecutionMetrics()) {
      var runs = NativeFailureParity.run(() -> environment(metrics, input, phase, rocks), sql);
      runs.assertSuccess(NativeFailureParity.Route.NATIVE);
      assertEquals(runs.host().resultTypes(), runs.nativeRun().resultTypes());
      assertFalse(runs.host().rows().isEmpty());
      long executed =
          metrics.nativeOperators().stream()
              .filter(
                  row ->
                      row.get("operator").toString().contains("WindowAgg")
                          && ((Number) row.get("recordsIn")).longValue() > 0)
              .count();
      assertTrue(executed >= 3, metrics.nativeOperators().toString());
    }
  }

  @ParameterizedTest
  @MethodSource("recoveryCases")
  void pendingCountsSurviveTwoCheckpointsAndLateRows(String phase, String shape, boolean rocks) {
    var input =
        IntStream.range(0, 20)
            .mapToObj(
                i ->
                    Row.of(
                        i % 5 == 0 ? null : i % 3,
                        i % 7 == 0 ? null : (long) i,
                        Instant.ofEpochMilli(START + (i >= 16 ? 100 : i * 500L))))
            .collect(Collectors.toList());
    try (var recovery =
        new PortableSqlRecovery(
                backend(rocks), input, inputType(), schema("SOURCE_WATERMARK()"), false, 6, 12)
            .withExecutionMetrics()
            .withWatermarks(Map.of(16, START + 29999, 20, START + 59999))) {
      UnaryOperator<TableEnvironment> config = table -> configure(table, phase);
      var runs =
          NativeFailureParity.runRecovery(
              () -> config.apply(recovery.uninterrupted()),
              () -> config.apply(recovery.get()),
              query("recovery_input", shape, 10));
      runs.assertSuccess(NativeFailureParity.Route.NATIVE);
      assertEquals(runs.host().resultTypes(), runs.nativeRun().resultTypes());
      recovery.verifyRepeatedRecovery();
      assertWindowsConsumed(recovery.nativeExecution());
    }
  }

  private static TableEnvironment environment(
      RecoveryExecutionMetrics metrics, List<Row> input, String phase, boolean rocks) {
    var config = new Configuration();
    config.setString("state.backend", backend(rocks));
    config.setString("taskmanager.memory.managed.size", "256m");
    var env = metrics.environment(config, 1, 0);
    var table = StreamTableEnvironment.create(env);
    configure(table, phase);
    table.createTemporaryView(
        "src", fromData(env, input, inputType()), schema("rt - INTERVAL '1' SECOND"));
    return table;
  }

  private static void assertWindowsConsumed(List<Map<String, Object>> rows) {
    long count =
        rows.stream()
            .filter(
                row ->
                    row.get("operator").toString().contains("WindowAgg")
                        && ((Number) row.get("recordsIn")).longValue() > 0)
            .count();
    assertTrue(count >= 2, rows.toString());
  }

  private static TableEnvironment configure(TableEnvironment table, String phase) {
    table.getConfig().setLocalTimeZone(ZoneOffset.UTC);
    table.getConfig().set("table.optimizer.agg-phase-strategy", phase);
    table.getConfig().set("streamfusion.transpose.batchRows", "2");
    return table;
  }

  private static org.apache.flink.api.common.typeinfo.TypeInformation<Row> inputType() {
    return Types.ROW_NAMED(new String[] {"k", "v", "rt"}, Types.INT, Types.LONG, Types.INSTANT);
  }

  private static Schema schema(String watermark) {
    return Schema.newBuilder()
        .column("k", DataTypes.INT())
        .column("v", DataTypes.BIGINT())
        .column("rt", DataTypes.TIMESTAMP_LTZ(3))
        .watermark("rt", watermark)
        .build();
  }

  private static String backend(boolean rocks) {
    return rocks ? "tech.streamfusion.state.RocksDBNativeStateBackendFactory" : "hashmap";
  }

  private static String query(String input, String shape, int seconds) {
    String args =
        (shape.equals("TUMBLE") ? "" : "INTERVAL '2' SECOND, ")
            + "INTERVAL '"
            + seconds
            + "' SECOND";
    return "SELECT SUM(n), MIN(n), MAX(n), AVG(n), COUNT(*), SUM(nonnull), wstart, wend FROM"
        + " (SELECT k, COUNT(*) n, COUNT(v) nonnull, window_start wstart, window_end wend"
        + " FROM TABLE("
        + shape
        + "(TABLE "
        + input
        + ", DESCRIPTOR(rt), "
        + args
        + "))"
        + " GROUP BY k, window_start, window_end) counts GROUP BY wstart, wend";
  }
}
