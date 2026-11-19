package tech.streamfusion;

import static org.apache.flink.table.api.DataTypes.*;
import static org.junit.jupiter.api.Assertions.*;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import tech.streamfusion.planner.NativePlanner;

class FlinkDecimalSignArithmeticSqlHarnessTest {
  @ParameterizedTest
  @CsvSource({"1,0", "1,1", "4,3", "18,0", "18,3", "18,18", "19,0", "19,19",
      "38,0", "38,18", "38,38"})
  void arithmeticBatchesPreserveAllValuesTypesAndOverflowNulls(int precision, int scale)
      throws Exception {
    var expressions = List.of("SIGN(d+d)", "SIGN(d-d)", "SIGN(d*d)", "SIGN((d+d)*d)",
        "SIGN(SIGN(d*d)+d)", "SIGN(CAST(d AS DECIMAL(38," + scale + "))+d)");
    for (boolean nullable : new boolean[] {false, true}) {
      var consumers = new ArrayList<String>();
      for (String expression : expressions) consumers.add(nullable ? expression
          : "(" + expression + ">0 OR CAST(NULL AS BOOLEAN))");
      String sql = "SELECT id," + String.join(",", consumers) + " FROM src";
      TreeMap<Integer, List<Object>> expected = null;
      List<DataType> expectedTypes = null;
      for (boolean nativeRun : new boolean[] {false, true}) {
        try (var metrics = new RecoveryExecutionMetrics()) {
          var env = metrics.environment(new org.apache.flink.configuration.Configuration(), 1, 0);
          var table = org.apache.flink.table.api.bridge.java.StreamTableEnvironment.create(env);
          var type = inputType(precision, scale, nullable);
          var input = rows(precision, scale, 5003, nullable);
          table.createTemporaryView("src", tech.streamfusion.compat.FlinkTestSources.fromData(env,
              input, org.apache.flink.table.runtime.typeutils.ExternalTypeInfo.<Row>of(type)),
              org.apache.flink.table.api.Schema.newBuilder().fromRowDataType(type).build());
          var scan = nativeRun ? NativePlanner.install(table) : null;
          var types = table.sqlQuery(sql).getResolvedSchema().getColumnDataTypes();
          if (nativeRun) {
            assertEquals(expectedTypes, types);
            assertTrue(table.explainSql(sql).contains("NativeCalc"));
            assertTrue(scan.fallbackReasons().isEmpty(), scan.fallbackReasons().toString());
            assertTrue(scan.functionExecutions().stream().anyMatch(route -> route.contains("SIGN")),
                scan.functionExecutions().toString());
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
  }

  @ParameterizedTest
  @CsvSource({"18,3", "19,19", "38,18", "38,38"})
  void consumersKeepNullFlagsLazinessAndBoundaries(int precision, int scale) throws Exception {
    java.util.function.Supplier<TableEnvironment> factory = () -> environment(precision, scale, true);
    BuiltinFunctionParity.assertParity(factory,
        "SELECT id,CAST(SIGN(d*d) AS STRING),SIGN(d*d) IS NULL,SIGN(d+d)>0,"
            + "CASE WHEN id>=0 THEN SIGN(d*d) ELSE SIGN(d/(id-id)) END FROM src");
    BuiltinFunctionParity.assertParity(factory,
        "SELECT id,SIGN(d*d) FROM src WHERE SIGN(d+d)>0 OR SIGN(d*d) IS NULL");
    BuiltinFunctionParity.assertParity(factory,
        "SELECT SIGN(d*d),SIGN(d/(id-id)) FROM src WHERE id<0");
    NativeParity.assertChangelogParity(factory,
        "SELECT v,COUNT(*) FROM (SELECT SIGN(d*d) AS v FROM src) GROUP BY v");
  }

  @Test
  void nonNullableOverflowAndUnrepresentableSignStillReachTheConstraintEnforcer() throws Exception {
    for (int scale : new int[] {18, 38}) {
      for (String policy : List.of("ERROR", "DROP")) {
        java.util.function.Supplier<TableEnvironment> factory = () -> {
          var table = environment(38, scale, false);
          table.getConfig().set("table.exec.sink.not-null-enforcer", policy);
          return table;
        };
        // Multiplication reduces scale 38 to 37, where SIGN can represent one.
        String sql = "SELECT id,SIGN(" + (scale == 38 ? "d" : "d*d") + ") FROM src";
        if (policy.equals("DROP")) BuiltinFunctionParity.assertParity(factory, sql);
        else {
          var runs = NativeFailureParity.run(factory, sql);
          assertNotNull(runs.host().rootCause(), runs.toString());
          runs.assertFailure(runs.host().rootCause().getClass(), "is NOT NULL",
              NativeFailureParity.Phase.ROW_EVALUATION, NativeFailureParity.Route.NATIVE);
        }
      }
    }
  }

  private static TableEnvironment environment(int precision, int scale, boolean nullable) {
    return BuiltinFunctionParity.environment(inputType(precision, scale, nullable),
        rows(precision, scale, 31, nullable));
  }

  private static DataType inputType(int precision, int scale, boolean nullable) {
    var decimal = DECIMAL(precision, scale);
    return ROW(FIELD("id", INT().notNull()), FIELD("d", nullable ? decimal : decimal.notNull()));
  }

  private static List<Row> rows(int precision, int scale, int count, boolean nullable) {
    BigInteger max = BigInteger.TEN.pow(precision).subtract(BigInteger.ONE);
    BigInteger[] values = {max.negate(), max.negate().add(BigInteger.ONE), BigInteger.ONE.negate(),
        BigInteger.ZERO, BigInteger.ONE, max, null};
    var rows = new ArrayList<Row>();
    for (int id = 0; id < count; id++) {
      BigInteger value = values[id % (nullable ? values.length : values.length - 1)];
      rows.add(Row.of(id, value == null ? null : new BigDecimal(value, scale)));
    }
    return rows;
  }
}
