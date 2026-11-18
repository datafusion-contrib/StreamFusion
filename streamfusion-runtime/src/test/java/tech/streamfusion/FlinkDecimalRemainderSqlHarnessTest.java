package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static tech.streamfusion.compat.FlinkTestSources.fromData;

import java.math.BigDecimal;
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

class FlinkDecimalRemainderSqlHarnessTest {
  private static final String SQL = "SELECT MOD(a, b) FROM t";

  @ParameterizedTest
  @ValueSource(
      strings = {
        "SELECT id, guard_value OR MOD(a, b) = CAST(0 AS DECIMAL(38,38)) FROM t",
        "SELECT id FROM t WHERE guard_value OR MOD(a, b) = CAST(0 AS DECIMAL(38,38))",
        "SELECT id, NOT (NOT guard_value AND MOD(a, b) <> CAST(0 AS DECIMAL(38,38))) FROM t"
      })
  void guardedRemainderSkipsFailingRows(String sql) throws Exception {
    NativeParity.assertFallbackReasonContains(() -> guardedRows(true), sql, "row short-circuiting");
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "SELECT id, guard_value AND MOD(a, b) = CAST(0 AS DECIMAL(38,38)) FROM t",
        "SELECT id FROM t WHERE guard_value AND MOD(a, b) = CAST(0 AS DECIMAL(38,38))"
      })
  void andSkipsFailingRows(String sql) throws Exception {
    NativeParity.assertFallbackReasonContains(
        () -> guardedRows(false), sql, "row short-circuiting");
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "SELECT id, guard_value OR MOD(a, b) = 0 FROM t",
        "SELECT id, guard_value OR a / b = 0 FROM t"
      })
  void booleanGuardsAlsoSkipZeroDivisors(String sql) throws Exception {
    NativeParity.assertFallbackReasonContains(
        () ->
            guardedRows(
                Row.of(1, true, BigDecimal.ONE, BigDecimal.ZERO),
                Row.of(2, false, BigDecimal.ONE, new BigDecimal("1E-38")),
                Row.of(3, null, BigDecimal.ONE, new BigDecimal("1E-38"))),
        sql,
        "row short-circuiting");
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "SELECT id, CASE WHEN guard_value THEN CAST(0 AS DECIMAL(38,38)) ELSE MOD(a, b) END FROM t",
        "SELECT id, CASE WHEN NOT guard_value THEN MOD(a, b) ELSE CAST(0 AS DECIMAL(38,38)) END"
            + " FROM t"
      })
  void caseStillSkipsFailingRowsNatively(String sql) throws Exception {
    NativeParity.assertParity(() -> guardedRows(true), sql);
  }

  @Test
  void evaluatedCaseBranchStillFailsNatively() {
    assertDivisionImpossible(
        () -> guardedRows(true),
        "SELECT id, CASE WHEN guard_value THEN MOD(a, b) ELSE CAST(0 AS DECIMAL(38,38)) END FROM t",
        true);
  }

  @Test
  void unknownGuardMustEvaluateTheRemainder() {
    assertDivisionImpossible(
        () ->
            guardedRows(
                Row.of(1, null, new BigDecimal("1E37"), new BigDecimal("3E-38")),
                Row.of(2, false, BigDecimal.ONE, new BigDecimal("1E-38"))),
        "SELECT id, guard_value OR MOD(a, b) = CAST(0 AS DECIMAL(38,38)) FROM t",
        false);
  }

  @Test
  void nonThrowingDecimalArithmeticRemainsNativeUnderBooleanOperators() throws Exception {
    NativeParity.assertParity(
        () -> guardedRows(true),
        "SELECT id, guard_value OR (a + a) IS NULL, guard_value AND (a - a) IS NULL, "
            + "guard_value OR (a * a) IS NULL FROM t");
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "10000000000000000000000000000000000000",
        "-10000000000000000000000000000000000000"
      })
  void integralQuotientMustFitTheMathContext(String value) {
    assertDivisionImpossible(
        () -> environment(value, "0.00000000000000000000000000000000000003"), SQL, true);
  }

  private static void assertDivisionImpossible(
      Supplier<TableEnvironment> environment, String sql, boolean nativeAdmitted) {
    for (boolean nativeRun : new boolean[] {false, true}) {
      TableEnvironment table = environment.get();
      if (nativeRun) {
        String plan = NativePlanner.explain(table, sql);
        assertEquals(nativeAdmitted, plan.contains("NativeCalc"), plan);
        NativePlanner.install(table);
      }
      Exception failure =
          assertThrows(
              Exception.class,
              () -> {
                try (var rows = table.executeSql(sql).collect()) {
                  while (rows.hasNext()) {
                    rows.next();
                  }
                }
              });
      StringBuilder causes = new StringBuilder();
      for (Throwable cause = failure; cause != null; cause = cause.getCause()) {
        causes.append(cause.getMessage()).append('\n');
      }
      assertTrue(causes.toString().contains("Division impossible"), causes.toString());
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"1", "2", "4", "5"})
  void largeIntegralQuotientMayHaveRemovableTrailingZeros(String divisor) throws Exception {
    NativeParity.assertParity(
        () ->
            environment(
                "10000000000000000000000000000000000000",
                "0.0000000000000000000000000000000000000" + divisor),
        SQL);
  }

  private static TableEnvironment environment(String left, String right) {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    StreamTableEnvironment table = StreamTableEnvironment.create(env);
    table.createTemporaryView(
        "t",
        fromData(
            env,
            Types.ROW_NAMED(new String[] {"a", "b"}, Types.BIG_DEC, Types.BIG_DEC),
            Row.of(new BigDecimal(left), new BigDecimal(right))),
        Schema.newBuilder()
            .column("a", DataTypes.DECIMAL(38, 0))
            .column("b", DataTypes.DECIMAL(38, 38))
            .build());
    return table;
  }

  private static TableEnvironment guardedRows(boolean skipOnTrue) {
    return guardedRows(
        Row.of(1, skipOnTrue, new BigDecimal("1E37"), new BigDecimal("3E-38")),
        Row.of(2, !skipOnTrue, BigDecimal.ONE, new BigDecimal("1E-38")),
        Row.of(3, null, BigDecimal.ONE, new BigDecimal("1E-38")));
  }

  private static TableEnvironment guardedRows(Row... rows) {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    StreamTableEnvironment table = StreamTableEnvironment.create(env);
    table.createTemporaryView(
        "t",
        fromData(
            env,
            Types.ROW_NAMED(
                new String[] {"id", "guard_value", "a", "b"},
                Types.INT,
                Types.BOOLEAN,
                Types.BIG_DEC,
                Types.BIG_DEC),
            rows),
        Schema.newBuilder()
            .column("id", DataTypes.INT())
            .column("guard_value", DataTypes.BOOLEAN())
            .column("a", DataTypes.DECIMAL(38, 0))
            .column("b", DataTypes.DECIMAL(38, 38))
            .build());
    return table;
  }
}
