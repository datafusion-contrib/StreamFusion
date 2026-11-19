package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
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

class FlinkWindowStringExtremaSqlHarnessTest {
  private static final String[] VALUES = {
    "", "a", "a\u0000", "a ", "z", "\u00e9", "\u4e2d", "\ud800\udc00",
    "\ue000", "\uffff", "long-value-".repeat(256)
  };

  @ParameterizedTest
  @CsvSource({"ONE_PHASE,false", "TWO_PHASE,false", "ONE_PHASE,true", "TWO_PHASE,true"})
  void checkpointRecoveryKeepsStringExtremaAndNullOnlyGroups(String phase, boolean rocks) {
    var input = List.of(
        Row.of(1, "z", Instant.ofEpochMilli(100)),
        Row.of(2, null, Instant.ofEpochMilli(100)),
        Row.of(1, "\u4e2d", Instant.ofEpochMilli(200)),
        Row.of(3, "duplicate", Instant.ofEpochMilli(200)),
        Row.of(1, "a\u0000", Instant.ofEpochMilli(300)),
        Row.of(2, null, Instant.ofEpochMilli(300)),
        Row.of(1, "\ud800\udc00", Instant.ofEpochMilli(400)),
        Row.of(3, "duplicate", Instant.ofEpochMilli(400)),
        Row.of(1, "", Instant.ofEpochMilli(600)),
        Row.of(2, null, Instant.ofEpochMilli(600)),
        Row.of(1, "later-".repeat(1024), Instant.ofEpochMilli(1700)),
        Row.of(99, "late", Instant.ofEpochMilli(300)));
    var schema = Schema.newBuilder().column("k", DataTypes.INT()).column("s", DataTypes.STRING())
        .column("rt", DataTypes.TIMESTAMP_LTZ(3)).watermark("rt", "SOURCE_WATERMARK()").build();
    String backend = rocks ? "tech.streamfusion.state.RocksDBNativeStateBackendFactory" : "hashmap";
    try (var recovery = new PortableSqlRecovery(backend, input,
        Types.ROW_NAMED(new String[] {"k", "s", "rt"}, Types.INT, Types.STRING, Types.INSTANT),
        schema, false, 4, 8).withExecutionMetrics().withWatermarks(Map.of(10, 999L, 12, 4999L))) {
      java.util.function.UnaryOperator<TableEnvironment> configure = table -> {
        table.getConfig().setLocalTimeZone(java.time.ZoneOffset.UTC);
        table.getConfig().set("table.optimizer.agg-phase-strategy", phase);
        table.getConfig().set("streamfusion.transpose.batchRows", "3");
        return table;
      };
      String query = "SELECT k, window_start, window_end, MIN(s), MAX(s), COUNT(s), COUNT(*) FROM TABLE("
          + "HOP(TABLE recovery_input, DESCRIPTOR(rt), INTERVAL '1' SECOND, INTERVAL '2' SECOND)) "
          + "GROUP BY k, window_start, window_end";
      var runs = NativeFailureParity.runRecovery(() -> configure.apply(recovery.uninterrupted()),
          () -> configure.apply(recovery.get()), query);
      runs.assertSuccess(NativeFailureParity.Route.NATIVE);
      assertEquals(runs.host().resultTypes(), runs.nativeRun().resultTypes());
      recovery.verifyRepeatedRecovery();
      RecoveryExecutionMetrics.assertConsumed(recovery.nativeExecution(), phase.equals("TWO_PHASE")
          ? "NativeColumnarGlobalWindowAggregate" : "NativeColumnarWindowAggregate");
      if (phase.equals("TWO_PHASE")) {
        RecoveryExecutionMetrics.assertConsumed(recovery.nativeExecution(), "NativeColumnarLocalWindowAggregate");
      }
    }
  }

  @Test
  void processingTimeUsesTheSameStringAccumulator() {
    var table = environment("ONE_PHASE", 2);
    table.executeSql("CREATE TEMPORARY VIEW timed_src AS SELECT *, PROCTIME() AS pt FROM src");
    String plan = NativePlanner.explain(table,
        "SELECT k, window_start, window_end, MIN(s), MAX(s) FROM TABLE("
            + "TUMBLE(TABLE timed_src, DESCRIPTOR(pt), INTERVAL '10' SECOND)) "
            + "GROUP BY k, window_start, window_end");
    assertTrue(plan.contains("NativeColumnarWindowAggregate"), plan);
  }

  @Test
  void cumulativeStringKeysAndStringPartialsRunTogether() {
    check(() -> {
      var table = environment("TWO_PHASE", 2);
      table.executeSql("CREATE TEMPORARY VIEW string_src AS SELECT CAST(k AS STRING) AS sk, s, rt FROM src");
      return table;
    }, "SELECT sk, window_start, window_end, MIN(s), MAX(s), COUNT(*) FROM TABLE("
        + "CUMULATE(TABLE string_src, DESCRIPTOR(rt), INTERVAL '2' SECOND, INTERVAL '10' SECOND)) "
        + "GROUP BY sk, window_start, window_end", "TWO_PHASE");
  }

  @ParameterizedTest
  @ValueSource(strings = {"ONE_PHASE", "TWO_PHASE"})
  void ordinaryStringCountSharesTheWindowWithStringExtrema(String phase) {
    var runs = NativeFailureParity.run(() -> environment(phase, 2),
        sql("TUMBLE", "MIN(s), MAX(s), COUNT(s)"));
    runs.assertSuccess(NativeFailureParity.Route.NATIVE);
    assertEquals(runs.host().resultTypes(), runs.nativeRun().resultTypes());
  }

  @ParameterizedTest
  @CsvSource({
    "ONE_PHASE,TUMBLE", "TWO_PHASE,TUMBLE", "ONE_PHASE,HOP", "TWO_PHASE,HOP",
    "ONE_PHASE,CUMULATE", "TWO_PHASE,CUMULATE"
  })
  void stringExtremaPreserveOrderingNullsAndLocalPartials(String phase, String shape) {
    check(() -> environment(phase, 2), sql(shape, "MIN(s), MAX(s), COUNT(*)"), phase);
  }

  @ParameterizedTest
  @ValueSource(strings = {"ONE_PHASE", "TWO_PHASE"})
  void unkeyedWindowsKeepStringExtrema(String phase) {
    check(() -> environment(phase, 2), sql("TUMBLE", "MIN(s), MAX(s), COUNT(*)")
        .replace("SELECT k, ", "SELECT ").replace("GROUP BY k, ", "GROUP BY "), phase);
  }

  @ParameterizedTest
  @ValueSource(strings = {"ONE_PHASE", "TWO_PHASE"})
  void stringNumericAndDistinctPartialsRetainTheirPositionsAndTypes(String phase) {
    check(() -> environment(phase, 2), sql("HOP",
        "MIN(s), AVG(ts), MAX(s), COUNT(DISTINCT s), SUM(CAST(ts AS DECIMAL(12, 3)))"), phase);
  }

  @ParameterizedTest
  @ValueSource(strings = {"ONE_PHASE", "TWO_PHASE"})
  void declaredCharacterWidthsAndIndependentFiltersArePreserved(String phase) {
    check(() -> environment(phase, 2), sql("HOP",
        "MIN(CAST(s AS CHAR(12))), MAX(CAST(s AS VARCHAR(5))), "
            + "MIN(s) FILTER (WHERE k < 3 AND MOD(ts, 2) = 0), "
            + "MAX(s) FILTER (WHERE k < 3 AND MOD(ts, 2) = 1), COUNT(*)"), phase);
  }

  @ParameterizedTest
  @CsvSource({"ONE_PHASE,1", "TWO_PHASE,1", "ONE_PHASE,2", "TWO_PHASE,2"})
  void supplementaryCharactersRetainFlinkOrdering(String phase, int parallelism) {
    check(() -> environment(phase, parallelism), sql("TUMBLE",
        "MIN(CASE WHEN MOD(ts, 2) = 0 THEN '\ud800\udc00' ELSE '\ue000' END), "
            + "MAX(CASE WHEN MOD(ts, 2) = 0 THEN '\ud800\udc00' ELSE '\ue000' END)"), phase);
  }

  @ParameterizedTest
  @CsvSource({"ONE_PHASE,1", "TWO_PHASE,1", "ONE_PHASE,2", "TWO_PHASE,2"})
  void sourceSurrogatesMatchFlinkSerializationBeforeExtrema(String phase, int parallelism) {
    check(() -> environment(phase, parallelism,
        new String[] {"\ud800", "?", "\udfff", "?x", "\ud800x", "?y"}),
        sql("TUMBLE", "MIN(s), MAX(s), COUNT(*)"), phase);
  }

  @ParameterizedTest
  @ValueSource(strings = {"ONE_PHASE", "TWO_PHASE"})
  void emptyInputEmitsNoWindow(String phase) {
    String query = sql("TUMBLE", "MIN(s), MAX(s)").replace("TABLE src", "TABLE empty_src");
    check(() -> {
      var table = environment(phase, 2);
      table.executeSql("CREATE TEMPORARY VIEW empty_src AS SELECT * FROM src WHERE k = 99");
      return table;
    }, query, phase);
  }

  @ParameterizedTest
  @ValueSource(strings = {"TUMBLE", "HOP", "SESSION"})
  void legacyWindowsKeepTheSameStringState(String shape) {
    String args = shape.equals("HOP") ? "INTERVAL '2' SECOND, INTERVAL '10' SECOND"
        : "INTERVAL '10' SECOND";
    check(() -> environment("ONE_PHASE", 2), "SELECT k, " + shape + "_START(rt, "
        + args + "), MIN(s), MAX(s), COUNT(*) FROM src GROUP BY k, " + shape
        + "(rt, " + args + ")", "ONE_PHASE");
  }

  @Test
  void sessionTableFunctionMergesStringPartials() {
    tech.streamfusion.compat.FlinkTestCapabilities.requireSessionTableFunction();
    check(() -> environment("ONE_PHASE", 2), "SELECT k, window_start, window_end, "
        + "MIN(s), MAX(s), COUNT(*) FROM TABLE(SESSION(TABLE src PARTITION BY k, "
        + "DESCRIPTOR(rt), INTERVAL '2' SECOND)) GROUP BY k, window_start, window_end",
        "ONE_PHASE");
  }

  @ParameterizedTest
  @ValueSource(strings = {"ONE_PHASE", "TWO_PHASE"})
  void attachedWindowsPreserveStringValuesInBothPhases(String phase) {
    String query = "SELECT window_start, window_end, MIN(lo), MAX(hi), COUNT(*) FROM ("
        + "SELECT k, window_start, window_end, MIN(s) AS lo, MAX(s) AS hi FROM TABLE("
        + window("TUMBLE") + ") GROUP BY k, window_start, window_end) "
        + "GROUP BY window_start, window_end";
    check(() -> environment(phase, 2), query, phase);
  }

  private static void check(Supplier<TableEnvironment> factory, String query, String phase) {
    var runs = NativeFailureParity.run(factory, query);
    runs.assertSuccess(NativeFailureParity.Route.NATIVE);
    assertEquals(runs.host().resultTypes(), runs.nativeRun().resultTypes());
    String plan = runs.nativeRun().plan();
    assertTrue(plan.contains(phase.equals("TWO_PHASE")
        ? "NativeColumnarGlobalWindowAggregate" : "NativeColumnar"), plan);
    if (phase.equals("TWO_PHASE")) {
      assertTrue(plan.contains("NativeColumnarLocalWindowAggregate"), plan);
    }
  }

  private static String sql(String shape, String aggregates) {
    return "SELECT k, window_start, window_end, " + aggregates + " FROM TABLE("
        + window(shape) + ") GROUP BY k, window_start, window_end";
  }

  private static String window(String shape) {
    switch (shape) {
      case "TUMBLE": return "TUMBLE(TABLE src, DESCRIPTOR(rt), INTERVAL '10' SECOND)";
      case "HOP": return "HOP(TABLE src, DESCRIPTOR(rt), INTERVAL '2' SECOND, INTERVAL '10' SECOND)";
      default: return "CUMULATE(TABLE src, DESCRIPTOR(rt), INTERVAL '2' SECOND, INTERVAL '10' SECOND)";
    }
  }

  private static TableEnvironment environment(String phase, int parallelism) {
    return environment(phase, parallelism, VALUES);
  }

  private static TableEnvironment environment(String phase, int parallelism, String[] values) {
    var env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(parallelism);
    var table = StreamTableEnvironment.create(env);
    table.getConfig().setLocalTimeZone(java.time.ZoneOffset.UTC);
    table.getConfig().set("table.optimizer.agg-phase-strategy", phase);
    var input = env.fromSequence(0, 5002).map(id -> Row.of(
        id % 5 == 0 ? null : (int) (id % 5),
        id % 5 == 4 || id % 7 == 0 ? null : values[(int) (id % values.length)],
        1000L + id % 11000))
        .returns(Types.ROW_NAMED(new String[] {"k", "s", "ts"}, Types.INT, Types.STRING, Types.LONG))
        .assignTimestampsAndWatermarks(WatermarkStrategy.<Row>forBoundedOutOfOrderness(Duration.ofDays(1))
            .withTimestampAssigner((row, previous) -> (Long) row.getField(2)));
    table.createTemporaryView("src", input, Schema.newBuilder()
        .column("k", DataTypes.INT()).column("s", DataTypes.STRING()).column("ts", DataTypes.BIGINT())
        .columnByMetadata("rt", DataTypes.TIMESTAMP_LTZ(3), "rowtime")
        .watermark("rt", "SOURCE_WATERMARK()").build());
    return table;
  }
}
