package tech.streamfusion;

import static org.apache.flink.table.api.DataTypes.*;
import static org.junit.jupiter.api.Assertions.*;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;
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
  @CsvSource({"1,1", "4,3", "18,0", "18,3", "18,18", "19,0", "38,18", "38,38"})
  void nullableDecimalArithmeticCastConsumesOnlyNativeWork(int precision, int scale)
      throws Exception {
    String sql = "SELECT id,CAST(d+d AS STRING),CAST(d-d AS STRING),CAST(d*d AS STRING),"
        + "CAST((d+d)*d AS VARCHAR(3)),CAST(d*(d-d) AS CHAR(44)),"
        + "CAST(ABS(d)+d AS CHAR(8)),CAST(d-(-d) AS VARCHAR(3)) FROM src";
    for (boolean legacy : new boolean[] {false, true}) {
      assertRuntimeCasts(sql, precision, scale, legacy, true);
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void nullableDecimalArithmeticCastKeepsConsumersAndLazyBranches(boolean legacy) throws Exception {
    java.util.function.Supplier<TableEnvironment> factory = () -> environment(38, 18, legacy);
    BuiltinFunctionParity.assertParity(factory,
        "SELECT id,CONCAT(CAST(d*d AS STRING),'!'),CHAR_LENGTH(CAST(d+d AS CHAR(8))),"
            + "CASE WHEN id>=0 THEN CAST(d*d AS STRING) ELSE CAST(d/(id-id) AS STRING) END FROM src");
    BuiltinFunctionParity.assertParity(factory,
        "SELECT id FROM src WHERE CAST(d*d AS STRING) LIKE '0%' OR CAST(d-d AS STRING) IS NULL");
    BuiltinFunctionParity.assertParity(factory,
        "SELECT CAST(d*d AS STRING),CAST(d/(id-id) AS STRING) FROM src WHERE id<0");
    NativeParity.assertChangelogParity(factory,
        "SELECT c,COUNT(*) FROM (SELECT CAST(d*d AS STRING) AS c FROM src) GROUP BY c");
    BuiltinFunctionParity.assertParity(() -> environment(18, 3, legacy),
        "SELECT id,LTRIM(CAST(d+d AS CHAR(5)),''),RTRIM(CAST(d+d AS VARCHAR(5)),''),"
            + "LTRIM(CAST(d*d AS VARCHAR(8)),''),RTRIM(CAST(d*d AS CHAR(8)),'') "
            + "FROM src WHERE id=2");
  }

  @ParameterizedTest
  @CsvSource({"false,CAST", "true,CAST", "false,TRY_CAST", "true,TRY_CAST"})
  void decimalUnaryCastConsumersAndEvaluationBoundariesMatch(boolean legacy, String operator)
      throws Exception {
    java.util.function.Supplier<TableEnvironment> factory = () -> environment(18, 3, legacy);
    BuiltinFunctionParity.assertParity(factory,
        ("SELECT id,CHAR_LENGTH($CAST(ABS(d) AS CHAR(25))),CONCAT($CAST(-d AS VARCHAR(3)),'!'),"
            + "$CAST(ABS(d) AS CHAR(8)) LIKE '%0%',LTRIM($CAST(-d AS VARCHAR(80)),''),"
            + "CASE WHEN id>=0 THEN $CAST(ABS(d) AS CHAR(8)) "
            + "ELSE CAST(ABS(d/(id-id)) AS CHAR(8)) END FROM src").replace("$CAST", operator));
    BuiltinFunctionParity.assertParity(factory,
        "SELECT $CAST(ABS(d) AS CHAR(8)),CAST(-(d/(id-id)) AS CHAR(8)) FROM src WHERE id<0"
            .replace("$CAST", operator));
    NativeParity.assertChangelogParity(factory,
        ("SELECT c,CHAR_LENGTH(c),CONCAT(c,'!'),COUNT(*) FROM "
            + "(SELECT CAST($CAST(ABS(d) AS CHAR(8)) AS STRING) AS c FROM src) GROUP BY c")
            .replace("$CAST", operator));
  }

  @ParameterizedTest
  @CsvSource({"1,1,CAST", "4,3,CAST", "18,0,CAST", "18,3,CAST", "18,18,CAST",
      "19,0,CAST", "38,18,CAST", "38,38,CAST", "1,1,TRY_CAST", "4,3,TRY_CAST",
      "18,0,TRY_CAST", "18,3,TRY_CAST", "18,18,TRY_CAST", "19,0,TRY_CAST",
      "38,18,TRY_CAST", "38,38,TRY_CAST"})
  void decimalUnaryCastFormattingConsumesNativeWork(int precision, int scale, String operator)
      throws Exception {
    String sql = ("SELECT id,$CAST(ABS(d) AS STRING),$CAST(-d AS STRING),"
        + "$CAST(ABS(ABS(d)) AS VARCHAR(3)),$CAST(-(-d) AS CHAR(44)),"
        + "$CAST(ABS(d) AS CHAR(8)),$CAST(-d AS VARCHAR(3)),"
        + "$CAST(CAST(ABS(d) AS DECIMAL(38," + scale + ")) AS STRING),"
        + "$CAST(ABS(CAST(d AS DECIMAL(38," + scale + "))) AS CHAR(8)),"
        + "$CAST(-(CAST(d AS DECIMAL(38," + scale + "))) AS CHAR(44)) FROM src")
        .replace("$CAST", operator);
    for (boolean legacy : new boolean[] {false, true}) {
      for (boolean nullable : new boolean[] {false, true}) {
        assertRuntimeCasts(sql, precision, scale, legacy, nullable);
      }
    }
  }

  @ParameterizedTest
  @CsvSource({"false,CAST", "true,CAST", "false,TRY_CAST", "true,TRY_CAST"})
  void decimalUnaryExactLengthTrimConsumersMatch(boolean legacy, String operator) throws Exception {
    BuiltinFunctionParity.assertParity(() -> environment(18, 3, legacy),
        ("SELECT id,LTRIM($CAST(ABS(d) AS CHAR(19)),''),RTRIM($CAST(-d AS CHAR(19)),''),"
            + "LTRIM($CAST(ABS(d) AS VARCHAR(19)),''),RTRIM($CAST(-d AS VARCHAR(19)),'') "
            + "FROM src WHERE id=0").replace("$CAST", operator));
  }

  @ParameterizedTest
  @CsvSource({"18,3,CAST", "19,0,CAST", "38,18,CAST", "38,38,CAST",
      "18,3,TRY_CAST", "19,0,TRY_CAST", "38,18,TRY_CAST", "38,38,TRY_CAST"})
  void boundedRuntimeCastsConsumeNativeWorkWithoutEvaluators(int precision, int scale, String operator)
      throws Exception {
    String sql = "SELECT id,CAST(d AS VARCHAR(1)),CAST(d AS VARCHAR(3)),"
        + "CAST(d AS VARCHAR(8)),CAST(d AS VARCHAR(20)),CAST(d AS VARCHAR(44)),"
        + "CAST(d AS CHAR(1)),CAST(d AS CHAR(3)),CAST(d AS CHAR(8)),"
        + "CAST(d AS CHAR(20)),CAST(d AS CHAR(44)),CAST(d AS CHAR(80)),"
        + "CAST(d AS CHAR(2147483647)) FROM src";
    sql = sql.replace("CAST(", operator + "(");
    for (boolean legacy : new boolean[] {false, true}) {
      for (boolean nullable : new boolean[] {false, true}) {
        assertRuntimeCasts(sql, precision, scale, legacy, nullable);
      }
    }
  }

  private static void assertRuntimeCasts(String sql, int precision, int scale,
      boolean legacy, boolean nullable) throws Exception {
    var decimal = nullable ? DECIMAL(precision, scale) : DECIMAL(precision, scale).notNull();
    var rowType = ROW(FIELD("id", INT().notNull()), FIELD("d", decimal));
    var input = rows(precision, scale, 5003);
    if (!nullable) for (Row row : input) if (row.getField(1) == null)
      row.setField(1, BigDecimal.ZERO.setScale(scale));
    TreeMap<Integer, List<Object>> expected = null;
    List<DataType> expectedTypes = null;
    for (boolean nativeRun : new boolean[] {false, true}) {
      try (var metrics = new RecoveryExecutionMetrics()) {
        var env = metrics.environment(new org.apache.flink.configuration.Configuration(), 1, 0);
        var table = StreamTableEnvironment.create(env);
        table.getConfig().set("table.exec.legacy-cast-behaviour", legacy ? "ENABLED" : "DISABLED");
        table.createTemporaryView("src", tech.streamfusion.compat.FlinkTestSources.fromData(env,
            input, ExternalTypeInfo.<Row>of(rowType)),
            org.apache.flink.table.api.Schema.newBuilder().fromRowDataType(rowType).build());
        var scan = nativeRun ? NativePlanner.install(table) : null;
        var types = table.sqlQuery(sql).getResolvedSchema().getColumnDataTypes();
        if (nativeRun) {
          assertEquals(expectedTypes, types);
          assertTrue(table.explainSql(sql).contains("NativeCalc"));
          assertTrue(scan.fallbackReasons().isEmpty(), scan.fallbackReasons().toString());
          assertFalse(scan.functionExecutions().isEmpty());
          assertTrue(scan.functionExecutions().stream().allMatch(route -> route.contains("jvm=[]")),
              scan.functionExecutions().toString());
        } else expectedTypes = types;
        var actual = new TreeMap<Integer, List<Object>>();
        try (var output = table.executeSql(sql).collect()) {
          while (output.hasNext()) {
            var row = output.next();
            int id = (Integer) row.getField(0);
            assertTrue(id >= 0 && id < input.size());
            var fields = new ArrayList<Object>();
            for (int i = 1; i < row.getArity(); i++) fields.add(row.getField(i));
            assertNull(actual.put(id, fields), "duplicate id " + id);
          }
        }
        assertEquals(input.size(), actual.size());
        if (nativeRun) {
          assertEquals(expected, actual);
          RecoveryExecutionMetrics.assertConsumed(metrics.nativeOperators(), "NativeCalc");
        } else expected = actual;
      }
    }
  }

  @ParameterizedTest
  @CsvSource({"false,CAST", "true,CAST", "false,TRY_CAST", "true,TRY_CAST"})
  void boundedCastConsumersLazyBranchesAndEmptyBatchesMatch(boolean legacy, String operator)
      throws Exception {
    java.util.function.Supplier<TableEnvironment> factory = () -> environment(18, 3, legacy);
    BuiltinFunctionParity.assertParity(factory,
        ("SELECT id,CHAR_LENGTH(CAST(d AS CHAR(25))),CONCAT(CAST(d AS VARCHAR(3)),'!'),"
            + "CAST(d AS CHAR(8)) LIKE '%0%',LTRIM(CAST(d AS VARCHAR(80)),''),"
            + "CASE WHEN id>=0 THEN CAST(d AS CHAR(8)) ELSE CAST(d/(id-id) AS CHAR(8)) END FROM src")
                .replace("CAST(d AS", operator + "(d AS"));
    BuiltinFunctionParity.assertParity(factory,
        "SELECT CAST(d AS CHAR(8)),CAST(d/(id-id) AS CHAR(8)) FROM src WHERE id<0"
            .replace("CAST(d AS", operator + "(d AS"));
  }

  @ParameterizedTest
  @CsvSource({"false,CAST", "true,CAST", "false,TRY_CAST", "true,TRY_CAST"})
  void boundedCastConsumersAcrossAggregationMatch(boolean legacy, String operator) throws Exception {
    java.util.function.Supplier<TableEnvironment> factory = () -> environment(18, 3, legacy);
    NativeParity.assertChangelogParity(factory,
        ("SELECT CHAR_LENGTH(MAX(CAST(CAST(d AS CHAR(25)) AS STRING))),"
            + "LTRIM(MAX(CAST(d AS VARCHAR(80))),''),"
            + "COUNT(DISTINCT CAST(d AS VARCHAR(3))) FROM src")
                .replace("CAST(d AS", operator + "(d AS"));
    NativeParity.assertChangelogParity(factory,
        ("SELECT c,CHAR_LENGTH(c),CONCAT(c,'!'),COUNT(*) FROM "
            + "(SELECT CAST(CAST(d AS CHAR(8)) AS STRING) AS c FROM src) GROUP BY c")
                .replace("CAST(d AS", operator + "(d AS"));
  }

  @ParameterizedTest
  @CsvSource({"false,CAST", "true,CAST", "false,TRY_CAST", "true,TRY_CAST"})
  void exactLengthCastTrimConsumersMatch(boolean legacy, String operator) throws Exception {
    BuiltinFunctionParity.assertParity(() -> environment(18, 3, legacy),
        ("SELECT id,LTRIM(CAST(d AS CHAR(20)),''),RTRIM(CAST(d AS CHAR(20)),''),"
            + "LTRIM(CAST(d AS VARCHAR(20)),''),RTRIM(CAST(d AS VARCHAR(20)),'') "
            + "FROM src WHERE id=0").replace("CAST(d AS", operator + "(d AS"));
  }

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
    for (String operand : List.of("d/(id-id)", "ABS(d/(id-id))", "-(d/(id-id))")) {
      var comparison = NativeFailureParity.run(() -> environment(18, 3, false),
          "SELECT TRY_CAST(" + operand + " AS STRING) FROM src");
      for (var outcome : List.of(comparison.host(), comparison.nativeRun())) {
        assertInstanceOf(ArithmeticException.class, outcome.rootCause());
        assertEquals(NativeFailureParity.Phase.ROW_EVALUATION, outcome.phase());
      }
      assertEquals(NativeFailureParity.Route.FALLBACK, comparison.nativeRun().route());
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void boundedTryCastComputedDecimalsRetainTheirExistingFallback(boolean legacy) throws Exception {
    NativeParity.assertFallback(() -> environment(18, 3, legacy),
        "SELECT TRY_CAST(d+CAST(1 AS DECIMAL(18,3)) AS CHAR(2)),"
            + "TRY_CAST(d+CAST(1 AS DECIMAL(18,3)) AS VARCHAR(2)) FROM src");
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
