package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.List;
import java.util.Map;
import java.util.function.UnaryOperator;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

class FlinkWindowScalarExtremaSqlHarnessTest {
  @ParameterizedTest
  @CsvSource({"TUMBLE,ONE_PHASE", "TUMBLE,TWO_PHASE", "HOP,ONE_PHASE", "HOP,TWO_PHASE",
      "CUMULATE,ONE_PHASE", "CUMULATE,TWO_PHASE"})
  void scalarExtremaKeepTypesNullOnlyKeysFiltersAndPartials(String shape, String phase) {
    for (boolean filtered : new boolean[] {false, true}) {
      String reductions = "MIN(day_value), MAX(day_value), MIN(time_value), MAX(time_value), "
          + "MIN(flag), MAX(flag)";
      if (filtered) reductions = "MIN(day_value) FILTER (WHERE id % 3 = 0), "
          + "MAX(day_value) FILTER (WHERE id % 3 = 1), "
          + "MIN(time_value) FILTER (WHERE id % 4 = 0), "
          + "MAX(time_value) FILTER (WHERE id % 4 = 2), "
          + "MIN(flag) FILTER (WHERE id % 2 = 0), MAX(flag) FILTER (WHERE id % 3 = 2)";
      String window = shape + "(TABLE src, DESCRIPTOR(rt), "
          + (shape.equals("TUMBLE") ? "" : "INTERVAL '1' SECOND, ")
          + "INTERVAL '3' SECOND)";
      var result = NativeFailureParity.run(() -> configure(
          FlinkOverBasicCountsSqlHarnessTest.environment(5003), phase),
          "SELECT k, window_start, window_end, " + reductions + ", COUNT(*) FROM TABLE("
              + window + ") GROUP BY k, window_start, window_end");
      result.assertSuccess(NativeFailureParity.Route.NATIVE);
      assertEquals(result.host().resultTypes(), result.nativeRun().resultTypes());
      assertTrue(result.nativeRun().plan().contains(phase.equals("TWO_PHASE")
          ? "NativeColumnarGlobalWindowAggregate" : "NativeColumnarWindowAggregate"));
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"TUMBLE", "HOP", "SESSION"})
  void legacyWindowsKeepScalarTypes(String shape) {
    String parameters = shape.equals("HOP")
        ? "rt, INTERVAL '1' SECOND, INTERVAL '3' SECOND" : "rt, INTERVAL '3' SECOND";
    var result = NativeFailureParity.run(() -> configure(
        FlinkOverBasicCountsSqlHarnessTest.environment(5003), "ONE_PHASE"),
        "SELECT k, " + shape + "_START(" + parameters + "), " + shape + "_END(" + parameters
            + "), MIN(day_value), MAX(day_value), MIN(time_value), MAX(time_value), MIN(flag), MAX(flag)"
            + " FROM src GROUP BY k, " + shape + "(" + parameters + ")");
    result.assertSuccess(NativeFailureParity.Route.NATIVE);
    assertEquals(result.host().resultTypes(), result.nativeRun().resultTypes());
  }

  @Test
  void sessionTableFunctionKeepsScalarExtrema() {
    tech.streamfusion.compat.FlinkTestCapabilities.requireSessionTableFunction();
    var result = NativeFailureParity.run(() -> configure(
        FlinkOverBasicCountsSqlHarnessTest.environment(5003), "ONE_PHASE"),
        "SELECT k, window_start, window_end, MIN(day_value), MAX(day_value), MIN(time_value), "
            + "MAX(time_value), MIN(flag), MAX(flag) FROM TABLE(SESSION(TABLE src PARTITION BY k, "
            + "DESCRIPTOR(rt), INTERVAL '3' SECOND)) GROUP BY k, window_start, window_end");
    result.assertSuccess(NativeFailureParity.Route.NATIVE);
    assertEquals(result.host().resultTypes(), result.nativeRun().resultTypes());
    assertTrue(result.nativeRun().plan().contains("NativeColumnarSessionWindowAggregate"));
  }

  @Test
  void processingTimeAdmissionKeepsTypedAccumulators() {
    var table = configure(FlinkOverBasicCountsSqlHarnessTest.environment(5003), "ONE_PHASE");
    String plan = tech.streamfusion.planner.NativePlanner.explain(table,
        "SELECT k, window_start, window_end, MIN(day_value), MAX(time_value), MIN(flag) FROM TABLE("
            + "TUMBLE(TABLE src, DESCRIPTOR(pt), INTERVAL '3' SECOND)) GROUP BY k, window_start, window_end");
    assertTrue(plan.contains("NativeColumnarWindowAggregate"), plan);
  }

  @ParameterizedTest
  @ValueSource(strings = {"ONE_PHASE", "TWO_PHASE"})
  void attachedWindowsPreserveScalarExtremaAcrossPhases(String phase) {
    String sql = "SELECT window_start, window_end, MIN(d), MAX(t), MIN(b), COUNT(*) FROM ("
        + "SELECT k, window_start, window_end, MIN(day_value) AS d, MAX(time_value) AS t, MIN(flag) AS b "
        + "FROM TABLE(TUMBLE(TABLE src, DESCRIPTOR(rt), INTERVAL '3' SECOND)) "
        + "GROUP BY k, window_start, window_end) GROUP BY window_start, window_end";
    var result = NativeFailureParity.run(() -> configure(
        FlinkOverBasicCountsSqlHarnessTest.environment(5003), phase), sql);
    result.assertSuccess(NativeFailureParity.Route.NATIVE);
    assertEquals(result.host().resultTypes(), result.nativeRun().resultTypes());
  }

  @ParameterizedTest
  @CsvSource({"ONE_PHASE,false,HOP", "ONE_PHASE,true,HOP", "TWO_PHASE,false,HOP", "TWO_PHASE,true,HOP",
      "ONE_PHASE,false,SESSION", "ONE_PHASE,true,SESSION"})
  void repeatedRecoveryPreservesScalarStateAndLateRowAdmission(String phase, boolean rocks, String shape) {
    if (shape.equals("SESSION")) tech.streamfusion.compat.FlinkTestCapabilities.requireSessionTableFunction();
    var day = LocalDate.of(2024, 1, 1);
    var clock = LocalTime.of(12, 34, 56, 123000000);
    List<Row> input = List.of(
        row(1, day, clock, false, 100), row(2, null, null, null, 100),
        row(1, day.plusDays(2), clock.plusNanos(1000000), true, 200),
        row(3, day, clock, false, 200), row(1, day.minusDays(2), clock.minusNanos(1000000), false, 300),
        row(2, null, null, null, 300), row(1, day, clock, true, 400),
        row(3, day, clock, false, 500), row(99, day, clock, true, 100));
    String backend = rocks ? "tech.streamfusion.state.RocksDBNativeStateBackendFactory" : "hashmap";
    try (var recovery = new PortableSqlRecovery(backend, input,
        Types.ROW_NAMED(new String[] {"k", "d", "t", "b", "rt"},
            Types.INT, Types.LOCAL_DATE, Types.LOCAL_TIME, Types.BOOLEAN, Types.INSTANT),
        Schema.newBuilder().column("k", DataTypes.INT()).column("d", DataTypes.DATE())
            .column("t", DataTypes.TIME(3)).column("b", DataTypes.BOOLEAN())
            .column("rt", DataTypes.TIMESTAMP_LTZ(3)).watermark("rt", "SOURCE_WATERMARK()").build(),
        false, 3, 6).withExecutionMetrics().withWatermarks(Map.of(8, 9999L, 9, 19999L))) {
      UnaryOperator<TableEnvironment> setup = table -> configure(table, phase);
      String window = shape.equals("SESSION")
          ? "SESSION(TABLE recovery_input PARTITION BY k, DESCRIPTOR(rt), INTERVAL '3' SECOND)"
          : "HOP(TABLE recovery_input, DESCRIPTOR(rt), INTERVAL '1' SECOND, INTERVAL '3' SECOND)";
      var result = NativeFailureParity.runRecovery(
          () -> setup.apply(recovery.uninterrupted()), () -> setup.apply(recovery.get()),
          "SELECT k, window_start, window_end, MIN(d), MAX(d), MIN(t), MAX(t), MIN(b), MAX(b), "
              + "COUNT(*) FROM TABLE(" + window + ") GROUP BY k, window_start, window_end");
      result.assertSuccess(NativeFailureParity.Route.NATIVE);
      assertEquals(result.host().resultTypes(), result.nativeRun().resultTypes());
      recovery.verifyRepeatedRecovery();
      RecoveryExecutionMetrics.assertConsumed(recovery.nativeExecution(), shape.equals("SESSION")
          ? "NativeColumnarSessionWindowAggregate" : phase.equals("TWO_PHASE")
              ? "NativeColumnarGlobalWindowAggregate" : "NativeColumnarWindowAggregate");
    }
  }

  private static Row row(int key, LocalDate day, LocalTime clock, Boolean flag, long time) {
    return Row.of(key, day, clock, flag, Instant.ofEpochMilli(time));
  }

  private static TableEnvironment configure(TableEnvironment table, String phase) {
    table.getConfig().setLocalTimeZone(java.time.ZoneOffset.UTC);
    table.getConfig().set("table.optimizer.agg-phase-strategy", phase);
    table.getConfig().set("table.exec.mini-batch.enabled", "true");
    table.getConfig().set("table.exec.mini-batch.size", "127");
    table.getConfig().set("table.exec.mini-batch.allow-latency", "1 h");
    return table;
  }
}
