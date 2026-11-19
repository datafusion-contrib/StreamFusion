package tech.streamfusion;

import static org.apache.flink.table.api.DataTypes.*;
import static org.junit.jupiter.api.Assertions.*;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.planner.NativePlanner;

class FlinkDecimalSignProjectionSqlHarnessTest {
  @ParameterizedTest
  @CsvSource({"19,false", "19,true", "38,false", "38,true"})
  void noncompactSignRetainsEveryProjectionDefaultAndRow(int precision, boolean legacy)
      throws Exception {
    String sql = "SELECT id,SIGN(d)>0,NOT(SIGN(d)>0),CAST(SIGN(d) AS INT),"
        + "CAST(SIGN(d) AS BIGINT),CAST(SIGN(d) AS SMALLINT),CAST(SIGN(d) AS TINYINT),"
        + "CAST(SIGN(d) AS FLOAT),CAST(SIGN(d) AS DOUBLE),"
        + "CAST(SIGN(d)>0 AS STRING),CAST(SIGN(d)>0 AS CHAR(8)),"
        + "CAST(SIGN(d)>0 AS VARCHAR(3)),(SIGN(d)>0) AND id>=0,"
        + "(SIGN(d)>0) OR id<0,CASE WHEN id>=0 THEN SIGN(d)>0 ELSE id<0 END,"
        + "SIGN(SIGN(d))>0,CAST(SIGN(d) AS DECIMAL(38," + precision + "))>0,"
        + "ABS(id-2500) FROM src";
    for (boolean nullable : new boolean[] {false, true}) {
      for (String policy : List.of("ERROR", "DROP")) {
        TreeMap<Integer, List<Object>> expected = null;
        List<DataType> expectedTypes = null;
        for (boolean nativeRun : new boolean[] {false, true}) {
          try (var metrics = new RecoveryExecutionMetrics()) {
            var env = metrics.environment(new org.apache.flink.configuration.Configuration(), 1, 0);
            var table = org.apache.flink.table.api.bridge.java.StreamTableEnvironment.create(env);
            var type = inputType(precision, precision, nullable);
            var input = rows(precision, nullable, 5003);
            configure(table, legacy, policy);
            table.createTemporaryView("src", tech.streamfusion.compat.FlinkTestSources.fromData(env,
                input, org.apache.flink.table.runtime.typeutils.ExternalTypeInfo.<Row>of(type)),
                org.apache.flink.table.api.Schema.newBuilder().fromRowDataType(type).build());
            var scan = nativeRun ? NativePlanner.install(table) : null;
            var types = table.sqlQuery(sql).getResolvedSchema().getColumnDataTypes();
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
            assertEquals(input.size(), actual.size(), "DROP must retain stock's default-valued rows");
            if (nativeRun) {
              assertEquals(expectedTypes, types);
              assertEquals(expected, actual);
              assertTrue(scan.fallbackReasons().isEmpty(), scan.fallbackReasons().toString());
              assertTrue(scan.functionExecutions().stream().anyMatch(route -> route.contains("ABS")),
                  scan.functionExecutions().toString());
              RecoveryExecutionMetrics.assertConsumed(metrics.nativeOperators(), "NativeCalc");
            } else {
              expected = actual;
              expectedTypes = types;
              assertEquals(nullable ? null : false, expected.get(0).get(0));
              assertEquals(nullable ? null : false, expected.get(0).get(1));
              assertEquals(nullable ? null : -1, expected.get(0).get(2));
              assertEquals(nullable ? null : "", expected.get(0).get(8));
              assertEquals(false, expected.get(2).get(0));
              assertEquals(true, expected.get(2).get(1));
              assertEquals(0, expected.get(2).get(2));
            }
          }
        }
      }
    }
  }

  @ParameterizedTest
  @CsvSource({"19,false", "19,true", "38,false", "38,true"})
  void intermediateNullsBranchesAndRelationalConsumersKeepTheirContracts(
      int precision, boolean nullable) throws Exception {
    java.util.function.Supplier<TableEnvironment> factory =
        () -> environment(precision, nullable, false, "ERROR");
    BuiltinFunctionParity.assertParity(factory,
        "SELECT id,(SIGN(d)>0) OR CAST(NULL AS BOOLEAN),(SIGN(d)>0) IS NULL,"
            + "CASE WHEN id>=0 THEN SIGN(d)>0 ELSE SIGN(d/(id-id))>0 END FROM src");
    BuiltinFunctionParity.assertParity(factory,
        "SELECT id FROM src WHERE NOT(SIGN(d)>0) OR id=2");
    String flag = "(SIGN(d)>0 OR CAST(NULL AS BOOLEAN))";
    BuiltinFunctionParity.assertParity(factory, "SELECT id," + flag + " IS TRUE,"
        + flag + " IS NOT TRUE," + flag + " IS FALSE," + flag + " IS NOT FALSE FROM src");
    BuiltinFunctionParity.assertParity(factory,
        "SELECT SIGN(d)>0,SIGN(d/(id-id))>0 FROM src WHERE id<0");
    NativeParity.assertChangelogParity(factory,
        "SELECT flag,COUNT(*) FROM (SELECT SIGN(d)>0 AS flag FROM src) GROUP BY flag");
  }

  @ParameterizedTest
  @ValueSource(ints = {19, 38})
  void selectedFailuresAndDirectDecimalConstraintsStillApply(int precision) throws Exception {
    NativeFailureParity.run(() -> environment(precision, false, false, "ERROR"),
        "SELECT SIGN(d/(id-id))>0 FROM src")
        .assertFailure(ArithmeticException.class, "zero", NativeFailureParity.Phase.ROW_EVALUATION,
            NativeFailureParity.Route.NATIVE);
    NativeFailureParity.run(() -> environment(precision, false, false, "ERROR"),
        "SELECT id,SIGN(d) FROM src")
        .assertFailure(RuntimeException.class, "NOT NULL", NativeFailureParity.Phase.ROW_EVALUATION,
            NativeFailureParity.Route.NATIVE);
    BuiltinFunctionParity.assertParity(() -> environment(precision, false, false, "DROP"),
        "SELECT id,SIGN(d) FROM src");
    NativeParity.assertFallbackReasonContains(() -> environment(precision, false, false, "ERROR"),
        "SELECT id,TRY_CAST(SIGN(d) AS INT) FROM src", "unsupported TRY_CAST DECIMAL");
    BuiltinFunctionParity.assertParity(() -> environment(precision, false, false, "ERROR"),
        "SELECT id,TRY_CAST(SIGN(d)>0 AS STRING) FROM src");
  }

  @ParameterizedTest
  @CsvSource({"18,3,false", "19,3,false", "38,18,false", "19,19,true", "38,38,true"})
  void unaffectedSignComparisonsKeepEmptyEvaluatorDescriptors(
      int precision, int scale, boolean nullable) throws Exception {
    try (var metrics = new RecoveryExecutionMetrics()) {
      java.util.function.Supplier<TableEnvironment> factory = () -> {
        var env = metrics.environment(new org.apache.flink.configuration.Configuration(), 1, 0);
        var table = org.apache.flink.table.api.bridge.java.StreamTableEnvironment.create(env);
        var type = inputType(precision, scale, nullable);
        table.createTemporaryView("src", tech.streamfusion.compat.FlinkTestSources.fromData(env,
            rows(scale, nullable, 31), org.apache.flink.table.runtime.typeutils.ExternalTypeInfo.<Row>of(type)),
            org.apache.flink.table.api.Schema.newBuilder().fromRowDataType(type).build());
        return table;
      };
      assertPureNative(factory, "SELECT id,SIGN(d)>0,NOT(SIGN(d)>0) FROM src");
      RecoveryExecutionMetrics.assertConsumed(metrics.nativeOperators(), "NativeCalc");
    }
  }

  @ParameterizedTest
  @ValueSource(ints = {19, 38})
  void retainedNullAwareConsumersKeepEmptyEvaluatorDescriptors(int precision) throws Exception {
    try (var metrics = new RecoveryExecutionMetrics()) {
      java.util.function.Supplier<TableEnvironment> factory = () -> {
        var env = metrics.environment(new org.apache.flink.configuration.Configuration(), 1, 0);
        var table = org.apache.flink.table.api.bridge.java.StreamTableEnvironment.create(env);
        var type = inputType(precision, precision, false);
        table.createTemporaryView("src", tech.streamfusion.compat.FlinkTestSources.fromData(env,
            rows(precision, false, 31), org.apache.flink.table.runtime.typeutils.ExternalTypeInfo.<Row>of(type)),
            org.apache.flink.table.api.Schema.newBuilder().fromRowDataType(type).build());
        return table;
      };
      String flag = "(SIGN(d)>0 OR CAST(NULL AS BOOLEAN))";
      assertPureNative(factory, "SELECT id," + flag + " IS NULL," + flag + " IS NOT NULL,"
          + flag + " IS NOT TRUE," + flag + " IS FALSE FROM src");
      RecoveryExecutionMetrics.assertConsumed(metrics.nativeOperators(), "NativeCalc");
    }
  }

  private static void assertPureNative(
      java.util.function.Supplier<TableEnvironment> factory, String sql) throws Exception {
    var table = factory.get();
    var scan = NativePlanner.install(table);
    assertTrue(table.explainSql(sql).contains("NativeCalc"));
    assertTrue(scan.fallbackReasons().isEmpty(), scan.fallbackReasons().toString());
    assertFalse(scan.functionExecutions().isEmpty());
    assertTrue(scan.functionExecutions().stream().allMatch(route -> route.contains("jvm=[]")),
        scan.functionExecutions().toString());
    BuiltinFunctionParity.assertParity(factory, sql);
  }

  private static TableEnvironment environment(
      int precision, boolean nullable, boolean legacy, String policy) {
    var table = BuiltinFunctionParity.environment(inputType(precision, precision, nullable),
        rows(precision, nullable, 31));
    configure(table, legacy, policy);
    return table;
  }

  private static void configure(TableEnvironment table, boolean legacy, String policy) {
    table.getConfig().set("table.exec.legacy-cast-behaviour", legacy ? "ENABLED" : "DISABLED");
    table.getConfig().set("table.exec.sink.not-null-enforcer", policy);
  }

  private static DataType inputType(int precision, int scale, boolean nullable) {
    var decimal = DECIMAL(precision, scale);
    return ROW(FIELD("id", INT().notNull()), FIELD("d", nullable ? decimal : decimal.notNull()));
  }

  private static List<Row> rows(int scale, boolean nullable, int count) {
    int[] values = {9, -9, 0, 1, -1, 8, -8};
    var rows = new ArrayList<Row>();
    for (int id = 0; id < count; id++) {
      var value = BigDecimal.valueOf(values[id % values.length], 1).setScale(scale);
      rows.add(Row.of(id, nullable && id % 7 == 6 ? null : value));
    }
    return rows;
  }
}
