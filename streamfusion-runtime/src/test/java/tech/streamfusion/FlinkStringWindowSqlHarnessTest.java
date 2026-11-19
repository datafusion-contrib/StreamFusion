package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.time.ZoneOffset;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.planner.NativePlanner;

class FlinkStringWindowSqlHarnessTest {
  private static final String[] TEXT = {
    "",
    "a",
    "aa",
    "z",
    "\u00e9",
    "e\u0301",
    "\u4e2d\u6587",
    "\ue000",
    "\ud83d\ude00",
    "a\u0000b",
    "prefix-" + "x".repeat(512)
  };

  @ParameterizedTest
  @CsvSource({
    "ONE_PHASE,TUMBLE", "TWO_PHASE,TUMBLE",
    "ONE_PHASE,HOP", "TWO_PHASE,HOP",
    "ONE_PHASE,CUMULATE", "TWO_PHASE,CUMULATE"
  })
  void stringExtremaAndMixedAccumulatorsMatchHost(String phase, String shape) throws Exception {
    check(
        phase,
        "SELECT k, window_start, window_end, MIN(text_value), MAX(text_value), "
            + "AVG(n), COUNT(*), SUM(n) FROM TABLE("
            + window(shape)
            + ") GROUP BY k, window_start, window_end");
  }

  @ParameterizedTest
  @CsvSource({
    "ONE_PHASE,TUMBLE", "TWO_PHASE,TUMBLE",
    "ONE_PHASE,HOP", "TWO_PHASE,HOP",
    "ONE_PHASE,CUMULATE", "TWO_PHASE,CUMULATE"
  })
  void independentlyFilteredStringExtremaMatchHost(String phase, String shape) throws Exception {
    check(
        phase,
        "SELECT k, window_start, window_end, "
            + "MIN(text_value) FILTER (WHERE selected), "
            + "MAX(text_value) FILTER (WHERE NOT selected), COUNT(*) "
            + "FROM TABLE("
            + window(shape)
            + ") GROUP BY k, window_start, window_end");
  }

  @ParameterizedTest
  @ValueSource(strings = {"ONE_PHASE", "TWO_PHASE"})
  void charExtremaMatchHost(String phase) throws Exception {
    org.junit.jupiter.api.Assumptions.assumeTrue(
        tech.streamfusion.compat.FlinkTestCapabilities.CHAR_EXTREMA,
        "Released Flink 1.18 cannot plan MIN/MAX over CHAR");
    check(
        phase,
        "SELECT k, window_start, window_end, MIN(CAST(text_value AS CHAR(8))), "
            + "MAX(CAST(text_value AS CHAR(8))), COUNT(*) FROM TABLE("
            + window("CUMULATE")
            + ") GROUP BY k, window_start, window_end");
  }

  @Test
  void attachedTwoPhaseExtremaMatchHost() throws Exception {
    check(
        "TWO_PHASE",
        "SELECT window_start, window_end, MIN(mn), MAX(mx), COUNT(*) FROM ("
            + "SELECT k, window_start, window_end, MIN(text_value) AS mn, MAX(text_value) AS mx "
            + "FROM TABLE("
            + window("TUMBLE")
            + ") GROUP BY k, window_start, window_end) "
            + "GROUP BY window_start, window_end");
  }

  @ParameterizedTest
  @ValueSource(strings = {"TUMBLE", "SESSION"})
  void legacyWindowExtremaMatchHost(String shape) throws Exception {
    String sql =
        "SELECT k, MIN(text_value), MAX(text_value), "
            + shape
            + "_START(rt, INTERVAL '2' SECOND), "
            + shape
            + "_END(rt, INTERVAL '2' SECOND) "
            + "FROM src GROUP BY k, "
            + shape
            + "(rt, INTERVAL '2' SECOND)";
    String plan = NativePlanner.explain(environment("ONE_PHASE"), sql);
    assertTrue(
        plan.contains(
            shape.equals("SESSION")
                ? "NativeColumnarSessionWindowAggregate"
                : "NativeColumnarWindowAggregate"),
        plan);
    NativeParity.assertParity(() -> environment("ONE_PHASE"), sql);
  }

  @Test
  void sessionTvfExtremaMatchHost() throws Exception {
    tech.streamfusion.compat.FlinkTestCapabilities.requireSessionTableFunction();
    String sql =
        "SELECT k, window_start, window_end, MIN(text_value), MAX(text_value), COUNT(*) "
            + "FROM TABLE(SESSION(TABLE src PARTITION BY k, DESCRIPTOR(rt), INTERVAL '2' SECOND)) "
            + "GROUP BY k, window_start, window_end";
    String plan = NativePlanner.explain(environment("ONE_PHASE"), sql);
    assertTrue(plan.contains("NativeColumnarSessionWindowAggregate"), plan);
    NativeParity.assertParity(() -> environment("ONE_PHASE"), sql);
  }

  @Test
  void issue316RetainsBothNativePhases() {
    var table =
        TableEnvironment.create(
            org.apache.flink.table.api.EnvironmentSettings.newInstance().inStreamingMode().build());
    table.getConfig().set("parallelism.default", "2");
    table.getConfig().set("table.optimizer.agg-phase-strategy", "TWO_PHASE");
    table.getConfig().set("table.exec.mini-batch.enabled", "true");
    table.getConfig().set("table.exec.mini-batch.allow-latency", "1 s");
    table.getConfig().set("table.exec.mini-batch.size", "128");
    table.executeSql(
        "CREATE TABLE input_events (k STRING, text_value STRING, ts TIMESTAMP(3), "
            + "WATERMARK FOR ts AS ts - INTERVAL '5' SECOND) "
            + "WITH ('connector' = 'datagen', 'number-of-rows' = '4096')");
    table.executeSql(
        "CREATE TABLE output_windows (k STRING, window_start TIMESTAMP(3), window_end TIMESTAMP(3),"
            + " min_value STRING, n BIGINT) WITH ('connector' = 'blackhole')");
    String plan =
        NativePlanner.explain(
            table,
            "INSERT INTO output_windows "
                + "SELECT k, window_start, window_end, MIN(text_value), COUNT(*) FROM TABLE("
                + "CUMULATE(TABLE input_events, DESCRIPTOR(ts), INTERVAL '10' MINUTE, "
                + "INTERVAL '60' MINUTE)) GROUP BY k, window_start, window_end");
    assertTrue(plan.contains("NativeColumnarLocalWindowAggregate"), plan);
    assertTrue(plan.contains("NativeColumnarGlobalWindowAggregate"), plan);
  }

  @ParameterizedTest
  @ValueSource(strings = {"ONE_PHASE", "TWO_PHASE"})
  void ordinaryStringCountUsesNativeValidityState(String phase) throws Exception {
    check(
        phase,
        "SELECT k, window_start, window_end, COUNT(text_value) FROM TABLE("
            + window("TUMBLE")
            + ") GROUP BY k, window_start, window_end");
  }

  private static void check(String phase, String sql) throws Exception {
    String plan = NativePlanner.explain(environment(phase), sql);
    assertTrue(
        plan.contains(
            phase.equals("TWO_PHASE")
                ? "NativeColumnarGlobalWindowAggregate"
                : "NativeColumnarWindowAggregate"),
        plan);
    if (phase.equals("TWO_PHASE")) {
      assertTrue(plan.contains("NativeColumnarLocalWindowAggregate"), plan);
    }
    NativeParity.assertParity(() -> environment(phase), sql);
  }

  private static String window(String shape) {
    switch (shape) {
      case "TUMBLE":
        return "TUMBLE(TABLE src, DESCRIPTOR(rt), INTERVAL '10' SECOND)";
      case "HOP":
        return "HOP(TABLE src, DESCRIPTOR(rt), INTERVAL '2' SECOND, INTERVAL '10' SECOND)";
      default:
        return "CUMULATE(TABLE src, DESCRIPTOR(rt), INTERVAL '2' SECOND, INTERVAL '10' SECOND)";
    }
  }

  private static TableEnvironment environment(String phase) {
    return environment(phase, false);
  }

  private static TableEnvironment environment(String phase, boolean singlePhaseStrings) {
    var env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(2);
    var table = StreamTableEnvironment.create(env);
    table.getConfig().setLocalTimeZone(ZoneOffset.UTC);
    table.getConfig().set("table.optimizer.agg-phase-strategy", phase);
    if (singlePhaseStrings) {
      table.getConfig().set("streamfusion.window.string-extrema.single-phase.enabled", "true");
    }
    table.getConfig().set("table.exec.mini-batch.enabled", "true");
    table.getConfig().set("table.exec.mini-batch.allow-latency", "1 s");
    table.getConfig().set("table.exec.mini-batch.size", "128");
    var input =
        env.fromSequence(0, 4102)
            .map(
                id ->
                    Row.of(
                        id % 9 == 1 ? null : (int) (id % 9),
                        id % 9 == 0 || id % 13 == 0 ? null : TEXT[(int) (id % TEXT.length)],
                        id % 7 == 0 ? null : id % 101 - 50,
                        id % 3 == 0 ? null : id % 3 == 1,
                        1000L + id % 11000))
            .returns(
                Types.ROW_NAMED(
                    new String[] {"k", "text_value", "n", "selected", "ts"},
                    Types.INT,
                    Types.STRING,
                    Types.LONG,
                    Types.BOOLEAN,
                    Types.LONG))
            .assignTimestampsAndWatermarks(
                WatermarkStrategy.<Row>forBoundedOutOfOrderness(Duration.ofDays(1))
                    .withTimestampAssigner((row, previous) -> (Long) row.getField(4)));
    table.createTemporaryView(
        "src",
        input,
        Schema.newBuilder()
            .column("k", DataTypes.INT())
            .column("text_value", DataTypes.STRING())
            .column("n", DataTypes.BIGINT())
            .column("selected", DataTypes.BOOLEAN())
            .column("ts", DataTypes.BIGINT())
            .columnByMetadata("rt", DataTypes.TIMESTAMP_LTZ(3), "rowtime")
            .watermark("rt", "SOURCE_WATERMARK()")
            .build());
    return table;
  }

  @Test
  void legacyStringExtremaRunNativelyWithoutAnOptIn() throws Exception {
    NativeParity.assertKindedParity(
        () -> environment("ONE_PHASE", false),
        "SELECT k, TUMBLE_START(rt, INTERVAL '10' SECOND), MIN(text_value), MAX(text_value),"
            + " COUNT(*) FROM src GROUP BY k, TUMBLE(rt, INTERVAL '10' SECOND)");
  }

  @ParameterizedTest
  @ValueSource(strings = {"TUMBLE", "HOP", "CUMULATE"})
  void measuredSinglePhaseShapesRunNativelyWithoutAnOptIn(String shape) throws Exception {
    NativeParity.assertKindedParity(
        () -> environment("ONE_PHASE", false),
        "SELECT k, window_start, window_end, MIN(text_value), MAX(text_value), COUNT(*) "
            + "FROM TABLE("
            + window(shape)
            + ") GROUP BY k, window_start, window_end");
  }

  @Test
  void explicitFalseRetainsStockForAnOtherwiseAdmittedShape() throws Exception {
    NativeParity.assertFallbackReasonContains(
        () -> {
          var table = environment("ONE_PHASE", false);
          table.getConfig().set("streamfusion.window.string-extrema.single-phase.enabled", "false");
          return table;
        },
        "SELECT k, window_start, window_end, MIN(text_value) FROM TABLE("
            + window("TUMBLE")
            + ") GROUP BY k, window_start, window_end",
        "single-phase string MIN/MAX requires");
  }
}
