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

class FlinkMixedExactLeastSqlHarnessTest {
  @ParameterizedTest
  @CsvSource({"4,2,false,false", "4,2,false,true", "18,2,true,false", "18,2,false,true",
      "19,19,false,false", "19,19,false,true", "38,18,true,false", "38,18,true,true"})
  void completeRuntimeBatchesPreserveValuesTypesNullsAndResolvedScale(
      int precision, int scale, boolean nullable, boolean bigint) throws Exception {
    var type = inputType(precision, scale, nullable, bigint);
    var input = rows(precision, scale, nullable, bigint, 5003);
    String sql = "SELECT id,LEAST(d,n),LEAST(n,d),LEAST(d,n,CAST(2 AS INT)) FROM src";
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
          assertPureNative(table, scan, sql);
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
        "SELECT id,CAST(LEAST(d,n) AS STRING),LEAST(d,n) IS NULL,"
            + "LEAST(d,n)>0,CASE WHEN id>=0 THEN LEAST(d,n) ELSE d/(id-id) END FROM src");
    BuiltinFunctionParity.assertParity(factory,
        "SELECT id,LEAST(d,n) FROM src WHERE LEAST(d,n)>0 OR n IS NULL");
    BuiltinFunctionParity.assertParity(factory,
        "SELECT LEAST(d/(id-id),n) FROM src WHERE id<0");
    NativeParity.assertChangelogParity(factory,
        "SELECT v,COUNT(*) FROM (SELECT LEAST(d,n) AS v FROM src) GROUP BY v");
  }

  @Test
  void nullableDecimalMetadataChangesPreserveTheReleasedFailure() {
    for (int[] pair : new int[][] {{4,2},{19,19},{38,38}}) {
      for (String expression : List.of("LEAST(d,n)", "LEAST(n,d)")) {
        Supplier<TableEnvironment> factory = () -> BuiltinFunctionParity.environment(
            inputType(pair[0], pair[1], true, false), List.of(Row.of(0, null, 2)));
        NativeFailureParity.run(factory, "SELECT " + expression + " FROM src")
            .assertFailure(NullPointerException.class, "", NativeFailureParity.Phase.ROW_EVALUATION,
                NativeFailureParity.Route.NATIVE);
      }
    }
  }

  @Test
  void lowerScaleAndUnverifiedCallsKeepGeneratedParity() throws Exception {
    Supplier<TableEnvironment> factory = () -> environment(38, 38, false, true);
    BuiltinFunctionParity.assertParity(factory,
        "SELECT id,LEAST(d,n),CAST(LEAST(d,n) AS STRING) FROM src");
    BuiltinFunctionParity.assertParity(() -> environment(18, 2, false, false),
        "SELECT LEAST(ABS(d),n),LEAST(d+d,n) FROM src");
    NativeFailureParity.run(() -> environment(18, 2, false, false),
        "SELECT LEAST(d/(id-id),n) FROM src")
        .assertFailure(ArithmeticException.class, "zero", NativeFailureParity.Phase.ROW_EVALUATION,
            NativeFailureParity.Route.NATIVE);
  }

  private static void assertPureNative(TableEnvironment table,
      tech.streamfusion.planner.PhysicalPlanScan scan, String sql) {
    assertTrue(table.explainSql(sql).contains("NativeCalc"));
    assertTrue(scan.fallbackReasons().isEmpty(), scan.fallbackReasons().toString());
    assertTrue(scan.functionExecutions().stream().anyMatch(route -> route.contains("LEAST")),
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
