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

class FlinkDecimalNegateSqlHarnessTest {
  @ParameterizedTest
  @CsvSource({"1,0", "1,1", "4,3", "18,0", "18,3", "18,18", "19,0", "19,19",
      "38,0", "38,18", "38,38"})
  void arithmeticBatchesPreserveAllValuesTypesAndOverflowNulls(int precision, int scale)
      throws Exception {
    String sql = "SELECT id,-(d+d),-(d-d),-(d*d),-((d+d)*d),-(-(d*d)+d),"
        + "-(CAST(d AS DECIMAL(38," + scale + "))+d) FROM src";
    for (boolean nullable : new boolean[] {false, true}) {
      TreeMap<Integer, List<Object>> expected = null;
      List<DataType> expectedTypes = null;
      for (boolean nativeRun : new boolean[] {false, true}) {
        try (var metrics = new RecoveryExecutionMetrics()) {
          var env = metrics.environment(new org.apache.flink.configuration.Configuration(), 1, 0);
          var table = org.apache.flink.table.api.bridge.java.StreamTableEnvironment.create(env);
          var type = inputType(precision, scale, nullable);
          var input = rows(precision, scale, 5003, nullable);
          // NOT NULL witnesses avoid overflow; nullable witnesses retain signed extrema/overflow.
          if (!nullable) {
            BigInteger[] values = {BigInteger.valueOf(-9), BigInteger.ONE.negate(),
                BigInteger.ZERO, BigInteger.ONE, BigInteger.valueOf(9)};
            input = new ArrayList<>();
            for (int i = 0; i < 5003; i++) {
              input.add(Row.of(i, new BigDecimal(values[i % values.length], scale)));
            }
          }
          table.createTemporaryView("src", tech.streamfusion.compat.FlinkTestSources.fromData(env,
              input, org.apache.flink.table.runtime.typeutils.ExternalTypeInfo.<Row>of(type)),
              org.apache.flink.table.api.Schema.newBuilder().fromRowDataType(type).build());
          var scan = nativeRun ? NativePlanner.install(table) : null;
          var types = table.sqlQuery(sql).getResolvedSchema().getColumnDataTypes();
          if (nativeRun) {
            assertEquals(expectedTypes, types);
            assertTrue(table.explainSql(sql).contains("NativeCalc"));
            assertTrue(scan.fallbackReasons().isEmpty(), scan.fallbackReasons().toString());
            assertTrue(scan.functionExecutions().stream()
                .anyMatch(route -> route.contains("native=[-")), scan.functionExecutions().toString());
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
  @CsvSource({"18,3", "38,18"})
  void arithmeticConsumersKeepNullFlagsLazinessAndBoundaries(int precision, int scale)
      throws Exception {
    java.util.function.Supplier<TableEnvironment> factory = () -> environment(precision, scale, true);
    BuiltinFunctionParity.assertParity(factory,
        "SELECT id,CAST(-(d*d) AS STRING),-(d*d) IS NULL,-(d+d)>0,"
            + "CASE WHEN id>=0 THEN -(d*d) ELSE -(d/(id-id)) END FROM src");
    BuiltinFunctionParity.assertParity(factory,
        "SELECT id,-(d*d) FROM src WHERE -(d+d)>0 OR -(d*d) IS NULL");
    BuiltinFunctionParity.assertParity(factory,
        "SELECT -(d*d),-(d/(id-id)) FROM src WHERE id<0");
    NativeParity.assertChangelogParity(factory,
        "SELECT v,COUNT(*) FROM (SELECT -(d*d) AS v FROM src) GROUP BY v");
  }

  @Test
  void nonNullableOverflowStillReachesTheConstraintEnforcer() throws Exception {
    for (String policy : List.of("ERROR", "DROP")) {
      java.util.function.Supplier<TableEnvironment> factory = () -> {
        var table = environment(38, 18, false);
        table.getConfig().set("table.exec.sink.not-null-enforcer", policy);
        return table;
      };
      String sql = "SELECT id,-(d*d) FROM src";
      if (policy.equals("DROP")) BuiltinFunctionParity.assertParity(factory, sql);
      else {
        var runs = NativeFailureParity.run(factory, sql);
        assertNotNull(runs.host().rootCause(), runs.toString());
        runs.assertFailure(runs.host().rootCause().getClass(), "is NOT NULL",
            NativeFailureParity.Phase.ROW_EVALUATION, NativeFailureParity.Route.NATIVE);
      }
    }
  }

  @ParameterizedTest
  @CsvSource({
    "1,0", "1,1", "18,0", "18,3", "18,18", "19,0", "19,3", "19,19", "38,0", "38,18", "38,38"
  })
  void boundariesNullsAndConsumersKeepReleasedTypesAndValues(int precision, int scale)
      throws Exception {
    BuiltinFunctionParity.assertParity(
        () -> environment(precision, scale, true),
        "SELECT id, -(d), -(-(d)), CAST(-(d) AS STRING), -(d)<0, "
            + "-(CAST(d AS DECIMAL(38,"
            + scale
            + "))) FROM src");
    BuiltinFunctionParity.assertParity(
        () -> environment(precision, scale, false), "SELECT -(d) FROM src WHERE MOD(id,2)=0");
  }

  @ParameterizedTest
  @CsvSource({"1,0", "18,3", "19,3", "38,38"})
  void consumedNativeBatchesNeedNoJvmEvaluator(int precision, int scale) throws Exception {
    try (var metrics = new RecoveryExecutionMetrics()) {
      var env = metrics.environment(new org.apache.flink.configuration.Configuration(), 1, 0);
      var table = org.apache.flink.table.api.bridge.java.StreamTableEnvironment.create(env);
      DataType type = inputType(precision, scale, true);
      List<Row> input = rows(precision, scale, 5003, true);
      table.createTemporaryView(
          "src",
          tech.streamfusion.compat.FlinkTestSources.fromData(
              env, input, org.apache.flink.table.runtime.typeutils.ExternalTypeInfo.<Row>of(type)),
          org.apache.flink.table.api.Schema.newBuilder().fromRowDataType(type).build());
      var scan = NativePlanner.install(table);
      String sql = "SELECT -(d), -(-(d)), -(CAST(d AS DECIMAL(38," + scale + "))) FROM src";
      assertTrue(table.explainSql(sql).contains("NativeCalc"));
      assertTrue(
          scan.functionExecutions().stream()
              .anyMatch(route -> route.contains("native=[-") && route.contains("jvm=[]")),
          scan.functionExecutions().toString());
      int count = 0;
      try (var results = table.executeSql(sql).collect()) {
        while (results.hasNext()) {
          BigDecimal value = (BigDecimal) input.get(count).getField(1);
          BigDecimal expected = value == null ? null : value.negate();
          assertEquals(Row.of(expected, value, expected), results.next());
          count++;
        }
      }
      assertEquals(5003, count);
      assertTrue(scan.fallbackReasons().isEmpty(), scan.fallbackReasons().toString());
      RecoveryExecutionMetrics.assertConsumed(metrics.nativeOperators(), "NativeCalc");
    }
  }

  @Test
  void unverifiedProducersKeepShortCircuitingAndEmptyBatchSemantics() throws Exception {
    BuiltinFunctionParity.assertParity(
        () -> environment(18, 3, true),
        "SELECT -(d+CAST(1 AS DECIMAL(18,3))), -(-d), " + "-(CAST(d AS DECIMAL(10,2))) FROM src");
    BuiltinFunctionParity.assertParity(
        () -> environment(19, 3, true), "SELECT -(d/(id-id)) FROM src WHERE id<0");
    BuiltinFunctionParity.assertParity(
        () -> environment(38, 18, true),
        "SELECT CASE WHEN id<0 THEN -(d/(id-id)) ELSE -(d) END FROM src");
    NativeFailureParity.run(() -> environment(19, 3, true), "SELECT -(d/(id-id)) FROM src")
        .assertFailure(
            ArithmeticException.class,
            "zero",
            NativeFailureParity.Phase.ROW_EVALUATION,
            NativeFailureParity.Route.NATIVE);
  }

  private static TableEnvironment environment(int precision, int scale, boolean nullable) {
    return BuiltinFunctionParity.environment(
        inputType(precision, scale, nullable), rows(precision, scale, 31, nullable));
  }

  private static DataType inputType(int precision, int scale, boolean nullable) {
    DataType decimal = DECIMAL(precision, scale);
    return ROW(FIELD("id", INT().notNull()), FIELD("d", nullable ? decimal : decimal.notNull()));
  }

  private static List<Row> rows(int precision, int scale, int count, boolean nullable) {
    BigInteger max = BigInteger.TEN.pow(precision).subtract(BigInteger.ONE);
    BigInteger[] values = {
      max.negate(),
      max.negate().add(BigInteger.ONE),
      BigInteger.ONE.negate(),
      BigInteger.ZERO,
      BigInteger.ONE,
      max,
      null
    };
    List<Row> rows = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      BigInteger value = values[i % (nullable ? values.length : values.length - 1)];
      rows.add(Row.of(i, value == null ? null : new BigDecimal(value, scale)));
    }
    return rows;
  }
}
