package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.function.Supplier;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.planner.NativePlanner;

class FlinkIntegerCastSqlHarnessTest {
  @ParameterizedTest
  @ValueSource(
      strings = {
        "SELECT id, CAST(s AS INT), CAST(v AS STRING), CAST(v AS VARCHAR(1)), CAST(v AS VARCHAR(2))"
            + " FROM t",
        "SELECT id FROM t WHERE CAST(s AS INT) > 0",
        "SELECT id, CAST(s AS INT) IS NULL, CAST(v AS STRING) IS NULL FROM t",
        "SELECT id, COALESCE(CAST(v AS STRING), 'null') FROM t"
      })
  void nullableInputsMatchFlink(String sql) throws Exception {
    NativeParity.assertParity(FlinkIntegerCastSqlHarnessTest::validRows, sql);
  }

  @Test
  void nonNullableInputsAndCharSourcesMatchFlink() throws Exception {
    NativeParity.assertParity(
        () -> environment(false, Row.of(1, " 42 ", 42, true), Row.of(2, "-17", -17, false)),
        "SELECT id, CAST(s AS INT), CAST(v AS STRING), CAST(CAST(s AS CHAR(8)) AS INT) FROM t");
  }

  @Test
  void benchmarkPipelineFixturesCoverNonzeroBuckets() throws Exception {
    IntegerCastBenchmarkInputs.verifyPipeline();
  }

  @Test
  void nestedIngestionExpressionMatchesFlink() throws Exception {
    NativeParity.assertParity(
        () ->
            environment(
                true,
                Row.of(1, "timestamp:00", 0, true),
                Row.of(2, "timestamp:17", 17, false),
                Row.of(3, "timestamp:59", 59, null),
                Row.of(4, null, null, true)),
        "SELECT id, LPAD(CAST(CAST(SUBSTR(s, 11, 2) AS INT) / 15 * 15 AS VARCHAR), 2, '0') FROM t");
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "SELECT id, CASE WHEN guard_value THEN 0 ELSE CAST(s AS INT) END FROM t",
        "SELECT id, CAST(s AS INT) FROM t WHERE NOT guard_value"
      })
  void unevaluatedBadInputsDoNotFail(String sql) throws Exception {
    NativeParity.assertParity(
        () -> environment(true, Row.of(1, "bad", 0, true), Row.of(2, "42", null, false)), sql);
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "SELECT id, guard_value OR CAST(s AS INT) > 0 FROM t",
        "SELECT id FROM t WHERE guard_value OR CAST(s AS INT) > 0"
      })
  void booleanGuardsKeepFlinkShortCircuiting(String sql) throws Exception {
    NativeParity.assertFallbackReasonContains(
        () -> environment(true, Row.of(1, "bad", 0, true), Row.of(2, "42", 42, false)),
        sql,
        "row short-circuiting");
  }

  @Test
  void nonFallibleIntegerFormattingStaysNativeUnderBooleanGuards() throws Exception {
    NativeParity.assertParity(
        FlinkIntegerCastSqlHarnessTest::validRows,
        "SELECT id, guard_value OR CAST(v AS STRING) IS NULL, guard_value AND CAST(v AS STRING) IS"
            + " NOT NULL FROM t");
  }

  @Test
  void coalesceAndReorderedAndRetainFlinkFailures() throws Exception {
    String coalesce = "SELECT id, COALESCE(v, CAST(s AS INT)) FROM t";
    NativeParity.assertFallbackReasonContains(
        FlinkIntegerCastSqlHarnessTest::validRows, coalesce, "COALESCE");
    Supplier<TableEnvironment> rows = () -> environment(true, Row.of(1, "bad", 0, true));
    assertFailure(rows, coalesce, false);
    assertFailure(rows, "SELECT id, NOT guard_value AND CAST(s AS INT) > 0 FROM t", false);
  }

  @ParameterizedTest
  @ValueSource(strings = {"=", "<>"})
  void directStringNumericEqualityKeepsItsExistingGate(String operator) throws Exception {
    NativeParity.assertFallbackReasonContains(
        FlinkIntegerCastSqlHarnessTest::validRows,
        "SELECT id FROM t WHERE CAST(s AS INT) " + operator + " 42",
        "implicit VARCHAR/numeric equality");
  }

  @ParameterizedTest
  @ValueSource(strings = {"bad", "", "2147483648", "-2147483649", "\t42", "\uff11\uff12"})
  void evaluatedBadInputsFailOnBothEngines(String value) {
    assertFailure(
        () -> environment(true, Row.of(1, value, 0, true)), "SELECT CAST(s AS INT) FROM t", true);
  }

  @Test
  void evaluatedCaseBranchAndFilterStillFail() {
    Supplier<TableEnvironment> rows = () -> environment(true, Row.of(1, "bad", 0, true));
    assertFailure(rows, "SELECT CASE WHEN guard_value THEN CAST(s AS INT) ELSE 0 END FROM t", true);
    assertFailure(rows, "SELECT id FROM t WHERE CAST(s AS INT) > 0", true);
  }

  @Test
  void tryCastRetainsNullOnFailureThroughFallback() throws Exception {
    NativeParity.assertFallback(
        () ->
            environment(
                true,
                Row.of(1, "bad", 0, true),
                Row.of(2, "42", 42, false),
                Row.of(3, null, null, null)),
        "SELECT id, TRY_CAST(s AS INT) FROM t");
  }

  @Test
  void legacyModeRetainsItsExistingFallback() throws Exception {
    NativeParity.assertFallback(
        () -> {
          TableEnvironment table = validRows();
          table
              .getConfig()
              .getConfiguration()
              .setString("table.exec.legacy-cast-behaviour", "ENABLED");
          return table;
        },
        "SELECT id, CAST(s AS INT), CAST(v AS VARCHAR(1)) FROM t");
  }

  @Test
  void mixedNativeAndHostCastsKeepRemainingHostSemantics() throws Exception {
    NativeParity.assertParity(
        FlinkIntegerCastSqlHarnessTest::validRows,
        "SELECT id, CAST(s AS INT), CAST(v AS STRING), CAST(CAST(v AS DOUBLE) AS STRING), CAST(v AS"
            + " CHAR(12)), CAST(s AS VARCHAR(2)) FROM t");
  }

  private static void assertFailure(
      Supplier<TableEnvironment> rows, String sql, boolean nativeAdmitted) {
    String reason = null;
    for (boolean nativeRun : new boolean[] {false, true}) {
      TableEnvironment table = rows.get();
      if (nativeRun) {
        String plan = NativePlanner.explain(table, sql);
        assertEquals(
            nativeAdmitted, plan.contains("NativeCalc") || plan.contains("NativeFilter"), plan);
      }
      Exception error =
          assertThrows(
              Exception.class,
              () -> {
                try (var output = table.executeSql(sql).collect()) {
                  while (output.hasNext()) output.next();
                }
              });
      StringBuilder causes = new StringBuilder();
      for (Throwable cause = error; cause != null; cause = cause.getCause())
        causes.append(cause.getMessage()).append('\n');
      if (!nativeRun) {
        reason =
            causes.toString().contains("Overflow.")
                ? "Overflow."
                : causes.toString().contains("Input is empty.")
                    ? "Input is empty."
                    : "Invalid character found.";
      }
      assertTrue(causes.toString().contains(reason), causes.toString());
    }
  }

  private static TableEnvironment validRows() {
    return environment(
        true,
        Row.of(1, " +00042 ", 42, true),
        Row.of(2, "-12.9", -123, false),
        Row.of(3, ".", 0, null),
        Row.of(4, "2147483647.999", Integer.MAX_VALUE, true),
        Row.of(5, "-2147483648.999", Integer.MIN_VALUE, false),
        Row.of(6, null, null, null));
  }

  private static TableEnvironment environment(boolean nullable, Row... rows) {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    StreamTableEnvironment table = StreamTableEnvironment.create(env);
    table.createTemporaryView(
        "t",
        env.fromData(
            Types.ROW_NAMED(
                new String[] {"id", "s", "v", "guard_value"},
                Types.INT,
                Types.STRING,
                Types.INT,
                Types.BOOLEAN),
            rows),
        Schema.newBuilder()
            .column("id", DataTypes.INT())
            .column("s", nullable ? DataTypes.STRING() : DataTypes.STRING().notNull())
            .column("v", nullable ? DataTypes.INT() : DataTypes.INT().notNull())
            .column("guard_value", DataTypes.BOOLEAN())
            .build());
    return table;
  }
}
