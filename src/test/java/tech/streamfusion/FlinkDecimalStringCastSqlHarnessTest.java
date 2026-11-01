package tech.streamfusion;

import static org.apache.flink.table.api.DataTypes.*;
import static org.junit.jupiter.api.Assertions.*;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.runtime.typeutils.ExternalTypeInfo;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.planner.NativePlanner;

class FlinkDecimalStringCastSqlHarnessTest {
  @ParameterizedTest
  @CsvSource({"1,0", "1,1", "4,3", "10,5", "18,0", "18,3", "18,18",
      "19,0", "19,19", "38,0", "38,18", "38,38"})
  void plainNotationScaleNullsTypesAndConsumersMatchFlink(int precision, int scale)
      throws Exception {
    for (boolean legacy : new boolean[] {false, true}) {
      BuiltinFunctionParity.assertParity(() -> environment(precision, scale, legacy),
          "SELECT id, CAST(d AS STRING), TRY_CAST(d AS STRING), "
              + "CONCAT(CAST(d AS STRING),'!'), CONCAT(TRY_CAST(d AS STRING),'!'), "
              + "CHAR_LENGTH(CAST(d AS STRING)), CHAR_LENGTH(TRY_CAST(d AS STRING)) FROM src");
    }
  }

  @ParameterizedTest
  @CsvSource({"18,3", "19,0", "38,38"})
  void consumedRuntimeInputHasNoJvmEvaluator(int precision, int scale) throws Exception {
    try (var metrics = new RecoveryExecutionMetrics()) {
      var env = metrics.environment(new org.apache.flink.configuration.Configuration(), 1, 0);
      var table = StreamTableEnvironment.create(env);
      DataType type = type(precision, scale);
      List<Row> input = rows(precision, scale, 5003);
      table.createTemporaryView("src",
          tech.streamfusion.compat.FlinkTestSources.fromData(env, input, ExternalTypeInfo.<Row>of(type)),
          org.apache.flink.table.api.Schema.newBuilder().fromRowDataType(type).build());
      var scan = NativePlanner.install(table);
      String sql = "SELECT CAST(d AS STRING), TRY_CAST(d AS STRING) FROM src";
      assertTrue(table.explainSql(sql).contains("NativeCalc"));
      assertTrue(scan.functionExecutions().stream().anyMatch(route -> route.contains("jvm=[]")),
          scan.functionExecutions().toString());
      int count = 0;
      try (var output = table.executeSql(sql).collect()) {
        while (output.hasNext()) {
          BigDecimal value = (BigDecimal) input.get(count++).getField(1);
          String expected = value == null ? null : value.toPlainString();
          assertEquals(Row.of(expected, expected), output.next());
        }
      }
      assertEquals(input.size(), count);
      assertTrue(scan.fallbackReasons().isEmpty(), scan.fallbackReasons().toString());
      RecoveryExecutionMetrics.assertConsumed(metrics.nativeOperators(), "NativeCalc");
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void boundedTargetsAndComputedDecimalsRetainTheirBehavior(boolean legacy) throws Exception {
    BuiltinFunctionParity.assertParity(() -> environment(18, 3, legacy),
        "SELECT CAST(d AS CHAR(25)), CAST(d AS CHAR(3)), CAST(d AS VARCHAR(3)), "
            + "CAST(d + CAST(1 AS DECIMAL(18,3)) AS STRING), "
            + "CAST(ROUND(d,id) AS STRING), CAST(TRUNCATE(d,id) AS STRING) FROM src");
    BuiltinFunctionParity.assertParity(() -> environment(18, 3, legacy),
        "SELECT CASE WHEN id>=0 THEN CAST(d AS STRING) "
            + "ELSE CAST(d/(id-id) AS STRING) END FROM src");
    BuiltinFunctionParity.assertParity(() -> environment(18, 3, legacy),
        "SELECT CAST(d/(id-id) AS STRING) FROM src WHERE id<0");
  }

  @Test
  void selectedOperandFailureIsNotSuppressed() {
    var comparison = NativeFailureParity.run(() -> environment(18, 3, false),
        "SELECT CAST(d/(id-id) AS STRING) FROM src");
    assertInstanceOf(ArithmeticException.class, comparison.host().rootCause());
    assertInstanceOf(NativeException.class, comparison.nativeRun().rootCause());
    for (var outcome : List.of(comparison.host(), comparison.nativeRun())) {
      assertTrue(outcome.rootCause().getMessage().contains("zero"), outcome.toString());
      assertEquals(NativeFailureParity.Phase.ROW_EVALUATION, outcome.phase());
    }
    assertEquals(NativeFailureParity.Route.NATIVE, comparison.nativeRun().route());
  }

  @Test
  void tryCastPreservesNonNullableInputsLazyBranchesAndEmptyBatches() throws Exception {
    for (boolean legacy : new boolean[] {false, true}) {
      BuiltinFunctionParity.assertParity(() -> {
        var table = BuiltinFunctionParity.environment(
            ROW(FIELD("d", DECIMAL(38, 38).notNull())),
            List.of(Row.of(new BigDecimal("0." + "9".repeat(38))), Row.of(BigDecimal.ZERO)));
        table.getConfig().getConfiguration().setString("table.exec.legacy-cast-behaviour",
            legacy ? "ENABLED" : "DISABLED");
        return table;
      }, "SELECT TRY_CAST(d AS STRING) FROM src");
      BuiltinFunctionParity.assertParity(() -> environment(18, 3, legacy),
          "SELECT CASE WHEN id>=0 THEN TRY_CAST(d AS STRING) "
              + "ELSE CAST(d/(id-id) AS STRING) END FROM src");
      BuiltinFunctionParity.assertParity(() -> environment(18, 3, legacy),
          "SELECT TRY_CAST(d AS STRING) FROM src WHERE id<0");
    }
  }

  @Test
  void tryCastDoesNotCatchErrorsComputingAnUnverifiedOperand() {
    var comparison = NativeFailureParity.run(() -> environment(18, 3, false),
        "SELECT TRY_CAST(d/(id-id) AS STRING) FROM src");
    for (var outcome : List.of(comparison.host(), comparison.nativeRun())) {
      assertInstanceOf(ArithmeticException.class, outcome.rootCause());
      assertEquals(NativeFailureParity.Phase.ROW_EVALUATION, outcome.phase());
    }
    assertEquals(NativeFailureParity.Route.FALLBACK, comparison.nativeRun().route());
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void boundedTryCastTargetsRetainTheirExistingFallback(boolean legacy) throws Exception {
    NativeParity.assertFallback(() -> environment(18, 3, legacy),
        "SELECT TRY_CAST(d AS CHAR(2)), TRY_CAST(d AS VARCHAR(2)) FROM src");
  }

  private static TableEnvironment environment(int precision, int scale, boolean legacy) {
    var table = BuiltinFunctionParity.environment(type(precision, scale), rows(precision, scale, 31));
    table.getConfig().getConfiguration().setString("table.exec.legacy-cast-behaviour",
        legacy ? "ENABLED" : "DISABLED");
    return table;
  }

  private static DataType type(int precision, int scale) {
    return ROW(FIELD("id", INT().notNull()), FIELD("d", DECIMAL(precision, scale)));
  }

  private static List<Row> rows(int precision, int scale, int count) {
    BigInteger max = BigInteger.TEN.pow(precision).subtract(BigInteger.ONE);
    BigInteger[] values = {max.negate(), BigInteger.ONE.negate(), BigInteger.ZERO,
        BigInteger.ONE, max, null};
    var rows = new ArrayList<Row>();
    for (int i = 0; i < count; i++) {
      BigInteger value = values[i % values.length];
      rows.add(Row.of(i, value == null ? null : new BigDecimal(value, scale)));
    }
    return rows;
  }
}
