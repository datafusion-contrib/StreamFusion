package tech.streamfusion;

import static org.apache.flink.table.api.DataTypes.*;
import static org.junit.jupiter.api.Assertions.*;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;
import java.util.function.Supplier;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import tech.streamfusion.planner.NativePlanner;

class FlinkMixedExactIfSqlHarnessTest {
  @ParameterizedTest
  @CsvSource({"4,2,false,false,false", "4,2,false,true,true", "4,2,true,false,false",
      "4,2,true,true,true", "18,2,true,false,false", "18,2,false,true,true",
      "19,19,false,false,true", "19,19,false,true,true", "19,19,true,false,true",
      "19,19,true,true,true", "18,2,true,true,true", "38,18,true,false,true", "38,18,true,true,true"})
  void completeRuntimeBatchesPreserveValuesTypesNullsAndResolvedScale(
      int precision, int scale, boolean nullable, boolean bigint, boolean pure) throws Exception {
    var type = inputType(precision, scale, nullable, bigint);
    var input = rows(precision, scale, nullable, bigint, 5003);
    String sql = "SELECT id,IF(n>=0,d,n),IF(n>=0,n,d) FROM src";
    TreeMap<Integer, List<Object>> expected = null;
    List<DataType> expectedTypes = null;
    for (boolean nativeRun : new boolean[] {false, true}) {
      try (var metrics = new RecoveryExecutionMetrics()) {
        var env = metrics.environment(new org.apache.flink.configuration.Configuration(), 1, 0);
        var table = org.apache.flink.table.api.bridge.java.StreamTableEnvironment.create(env);
        table.createTemporaryView("src", tech.streamfusion.compat.FlinkTestSources.fromData(env,
            input, org.apache.flink.table.runtime.typeutils.ExternalTypeInfo.<Row>of(type)),
            org.apache.flink.table.api.Schema.newBuilder().fromRowDataType(type).build());
        var scan = nativeRun ? NativePlanner.install(table) : null;
        var types = table.sqlQuery(sql).getResolvedSchema().getColumnDataTypes();
        if (nativeRun) {
          assertEquals(expectedTypes, types);
          if (pure) assertPureNative(table, scan, sql);
          else {
            table.explainSql(sql);
            assertTrue(scan.fallbackReasons().isEmpty(), scan.explainSummary());
            assertTrue(scan.functionExecutions().stream().anyMatch(route -> !route.contains("jvm=[]")),
                scan.explainSummary());
          }
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
  @CsvSource({"18,2,true,false", "18,2,false,true", "38,18,true,true"})
  void consumersFiltersBranchesAndGroupingRetainTheirContracts(
      int precision, int scale, boolean nullable, boolean bigint) throws Exception {
    Supplier<TableEnvironment> factory = () -> environment(precision, scale, nullable, bigint);
    BuiltinFunctionParity.assertParity(factory,
        "SELECT id,CAST(IF(n>=0,d,n) AS STRING),IF(n>=0,d,n) IS NULL,"
            + "IF(n>=0,d,n)>0,CASE WHEN id>=0 THEN IF(n>=0,d,n) ELSE d/(id-id) END FROM src");
    BuiltinFunctionParity.assertParity(factory,
        "SELECT id,IF(n>=0,d,n) FROM src WHERE IF(n>=0,d,n)>0 OR n IS NULL");
    BuiltinFunctionParity.assertParity(factory,
        "SELECT IF(n>=0,d/(id-id),n) FROM src WHERE id<0");
    NativeParity.assertChangelogParity(factory,
        "SELECT v,COUNT(*) FROM (SELECT IF(n>=0,d,n) AS v FROM src) GROUP BY v");
  }

  @Test
  void nullableDecimalMetadataChangesStayNullSafeInEitherSelectedBranch() throws Exception {
    for (int[] pair : new int[][] {{4,2},{19,19},{38,18}}) {
      Supplier<TableEnvironment> factory = () -> BuiltinFunctionParity.environment(
          inputType(pair[0], pair[1], true, false),
          List.of(Row.of(0, null, 2), Row.of(1, null, -2), Row.of(2, null, null)));
      BuiltinFunctionParity.assertParity(factory,
          "SELECT id,IF(n>=0,d,n),IF(n>=0,n,d),IF(n>=0,d,CAST(2 AS INT)),"
              + "IF(n>=0,CAST(NULL AS INT),d),IF(n>=0,d,CAST(NULL AS INT)) FROM src");
    }
  }

  @Test
  void compactNarrowIntegerBranchesKeepGeneratedParity() throws Exception {
    Supplier<TableEnvironment> factory = () -> BuiltinFunctionParity.environment(
        ROW(FIELD("d", DECIMAL(4,2)), FIELD("a", TINYINT()), FIELD("b", SMALLINT())),
        List.of(Row.of(new BigDecimal("99.99"), Byte.MIN_VALUE, Short.MIN_VALUE),
            Row.of(new BigDecimal("-99.99"), Byte.MAX_VALUE, Short.MAX_VALUE),
            Row.of(null, (byte) 1, (short) -1), Row.of(null, null, null)));
    String sql = "SELECT IF(a>=0,d,a),IF(a>=0,a,d),IF(b>=0,d,b),IF(b>=0,b,d) FROM src";
    BuiltinFunctionParity.assertParity(factory, sql);
    var table = factory.get();
    var scan = NativePlanner.install(table);
    table.explainSql(sql);
    assertTrue(scan.functionExecutions().stream().anyMatch(route -> !route.contains("jvm=[]")),
        scan.explainSummary());
  }

  @Test
  void failingConditionsSkipEmptyBatchesAndFailOnSurvivingRows() throws Exception {
    Supplier<TableEnvironment> factory = () -> environment(19, 19, true, false);
    String expression = "IF(id/(id-id)>0,d,n)";
    BuiltinFunctionParity.assertParity(factory,
        "SELECT " + expression + " FROM src WHERE id<0");
    NativeFailureParity.run(factory, "SELECT " + expression + " FROM src")
        .assertCompetingFailures(NativeFailureParity::isDivisionByZero,
            NativeFailureParity.Phase.ROW_EVALUATION,
            NativeFailureParity.Route.NATIVE);
  }

  @Test
  void lowerScaleAndUnverifiedCallsKeepGeneratedParity() throws Exception {
    Supplier<TableEnvironment> factory = () -> environment(38, 38, false, true);
    BuiltinFunctionParity.assertParity(factory,
        "SELECT id,IF(n>=0,d,n),CAST(IF(n>=0,d,n) AS STRING) FROM src");
    BuiltinFunctionParity.assertParity(() -> environment(18, 2, false, false),
        "SELECT IF(n>=0,ABS(d),n),IF(n>=0,d+d,n) FROM src");
    NativeFailureParity.run(() -> environment(18, 2, false, false),
        "SELECT IF(n>=0,d/(id-id),n) FROM src")
        .assertFailure(ArithmeticException.class, "zero", NativeFailureParity.Phase.ROW_EVALUATION,
            NativeFailureParity.Route.NATIVE);
  }

  private static void assertPureNative(TableEnvironment table,
      tech.streamfusion.planner.PhysicalPlanScan scan, String sql) {
    assertTrue(table.explainSql(sql).contains("NativeCalc"));
    assertTrue(scan.fallbackReasons().isEmpty(), scan.fallbackReasons().toString());
    assertTrue(scan.functionExecutions().stream().anyMatch(route -> route.contains("IF")),
        scan.functionExecutions().toString());
    assertTrue(scan.functionExecutions().stream().allMatch(route -> route.contains("jvm=[]")),
        scan.functionExecutions().toString());
  }

  private static TableEnvironment environment(int precision, int scale,
      boolean nullable, boolean bigint) {
    return BuiltinFunctionParity.environment(inputType(precision, scale, nullable, bigint),
        rows(precision, scale, nullable, bigint, 31));
  }

  private static DataType inputType(int precision, int scale, boolean nullable, boolean bigint) {
    var decimal = DECIMAL(precision, scale);
    return ROW(FIELD("id", INT().notNull()), FIELD("d", nullable ? decimal : decimal.notNull()),
        FIELD("n", bigint ? BIGINT() : INT()));
  }

  private static List<Row> rows(int precision, int scale, boolean nullable, boolean bigint,
      int count) {
    BigInteger max = BigInteger.TEN.pow(precision).subtract(BigInteger.ONE);
    BigInteger[] values = {max.negate(), BigInteger.ONE.negate(), BigInteger.ZERO,
        BigInteger.ONE, max};
    var rows = new ArrayList<Row>();
    for (int id = 0; id < count; id++) {
      Object integer;
      if (bigint) integer = id % 3 == 0 ? Long.MIN_VALUE
          : id % 3 == 1 ? Long.MAX_VALUE : 9007199254740993L + id;
      else integer = id % 3 == 0 ? Integer.MIN_VALUE : id % 3 == 1 ? Integer.MAX_VALUE : id;
      rows.add(Row.of(id, nullable && id % 7 == 0 ? null
          : new BigDecimal(values[id % values.length], scale), id % 11 == 0 ? null : integer));
    }
    return rows;
  }
}
