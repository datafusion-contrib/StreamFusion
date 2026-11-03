package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.*;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.functions.ScalarFunction;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class FlinkJvmOverBoundarySqlHarnessTest {
  @ParameterizedTest
  @CsvSource({"rt,false", "rt,true", "pt,false", "pt,true"})
  void computedJavaStringsCrossTheSameShuffleBoundaryAsReleasedFlink(
      String order, boolean partitioned) {
    try (var metrics = new RecoveryExecutionMetrics()) {
      var runs =
          NativeFailureParity.run(
              () -> {
                var table = FlinkJvmOverSqlHarnessTest.environment(metrics, 1);
                table.createTemporarySystemFunction("unpaired", new Unpaired());
                table.createTemporarySystemFunction(
                    "remember",
                    new FlinkTypedJvmAggregateSqlHarnessTest.Remember(DataTypes.STRING()));
                return table;
              },
              "SELECT id,remember(s) OVER ("
                  + (partitioned ? "PARTITION BY k " : "")
                  + "ORDER BY "
                  + order
                  + " ROWS UNBOUNDED PRECEDING)"
                  + " FROM (SELECT *,unpaired(v) AS s FROM src)");
      runs.assertSuccess(NativeFailureParity.Route.NATIVE);
      assertEquals(runs.host().resultTypes(), runs.nativeRun().resultTypes());
      RecoveryExecutionMetrics.assertConsumed(metrics.nativeOperators(), "NativeJvmOver");
    }
  }

  @ParameterizedTest
  @org.junit.jupiter.params.provider.ValueSource(strings = {"array", "async", "lag"})
  void unverifiedDefinitionsAndOperatorFieldsRetainWholeIslandFallback(String shape) {
    try (var metrics = new RecoveryExecutionMetrics()) {
      var runs =
          NativeFailureParity.run(
              () -> {
                var table = FlinkJvmOverSqlHarnessTest.environment(metrics, 1);
                if (shape.equals("async"))
                  table.getConfig().set("table.exec.async-state.enabled", "true");
                return table;
              },
              "SELECT id,"
                  + (shape.equals("array") ? "a," : "")
                  + "test_aggregate(v) OVER w"
                  + (shape.equals("lag") ? ",LAG(v) OVER w" : "")
                  + " FROM "
                  + (shape.equals("array") ? "(SELECT *,ARRAY[id] AS a FROM src)" : "src")
                  + " WINDOW w AS (PARTITION BY k ORDER BY rt"
                  + (shape.equals("lag") ? "" : " ROWS UNBOUNDED PRECEDING")
                  + ")");
      runs.assertSuccess(NativeFailureParity.Route.FALLBACK);
      assertFalse(runs.nativeRun().plan().contains("NativeJvmOver"));
      assertEquals(0, metrics.counterValue("jvmOverRows"));
    }
  }

  @ParameterizedTest
  @CsvSource({
    "rt,ROWS UNBOUNDED PRECEDING",
    "rt,RANGE UNBOUNDED PRECEDING",
    "rt,ROWS BETWEEN 3 PRECEDING AND CURRENT ROW",
    "rt,RANGE BETWEEN INTERVAL '1' SECOND PRECEDING AND CURRENT ROW",
    "pt,ROWS UNBOUNDED PRECEDING",
    "pt,RANGE UNBOUNDED PRECEDING",
    "pt,ROWS BETWEEN 3 PRECEDING AND CURRENT ROW"
  })
  void legacyMultipleArgumentsAndConstantsUseReleasedInference(String order, String frame) {
    try (var metrics = new RecoveryExecutionMetrics()) {
      var runs =
          NativeFailureParity.run(
              () ->
                  FlinkLegacyJvmAggregateSqlHarnessTest.register(
                      FlinkJvmOverSqlHarnessTest.environment(metrics, 1),
                      true,
                      new FlinkLegacyJvmAggregateSqlHarnessTest.WeightedAverage()),
              "SELECT id,legacy_avg(v,k) OVER w,legacy_avg(v,2) OVER w FROM src"
                  + " WINDOW w AS (PARTITION BY k ORDER BY "
                  + order
                  + " "
                  + frame
                  + ")");
      runs.assertSuccess(NativeFailureParity.Route.NATIVE);
      assertEquals(runs.host().resultTypes(), runs.nativeRun().resultTypes());
      RecoveryExecutionMetrics.assertConsumed(metrics.nativeOperators(), "NativeJvmOver");
    }
  }

  public static class Unpaired extends ScalarFunction {
    public String eval(Long value) {
      return value == null ? null : "original-" + (char) 0xD800 + "-" + value;
    }
  }
}
