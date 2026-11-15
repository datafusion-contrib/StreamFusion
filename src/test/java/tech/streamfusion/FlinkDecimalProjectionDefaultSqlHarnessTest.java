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

class FlinkDecimalProjectionDefaultSqlHarnessTest {
  @ParameterizedTest
  @CsvSource({"false,ERROR", "false,DROP", "true,ERROR", "true,DROP"})
  void overflowKeepsEveryBooleanProjectionAndRow(boolean nullable, String policy) throws Exception {
    String sql = "SELECT id,d*d>0,d*d>=0,d*d<0,d*d<=0,d*d=0,d*d<>0,NOT(d*d>0),"
        + "ABS(d*d)>0,(d*d>0) AND id>=0,(d*d>0) OR id<0,"
        + "CASE WHEN id>=0 THEN d*d>0 ELSE id<0 END FROM src";
    var expected = assertAllRows(sql, nullable, policy, false);
    assertEquals(nullable ? null : false, expected.get(0).get(0));
    assertEquals(nullable ? null : false, expected.get(0).get(6));
  }

  @ParameterizedTest
  @CsvSource({"false,false", "false,true", "true,false", "true,true"})
  void numericAndCharacterConsumersKeepTheirOwnDefaults(boolean nullable, boolean legacy)
      throws Exception {
    String sql = "SELECT id,CAST(d*d AS INT),CAST(d*d AS BIGINT),CAST(d*d AS SMALLINT),"
        + "CAST(d*d AS TINYINT),CAST(d*d AS FLOAT),CAST(d*d AS DOUBLE),"
        + "CAST(d*d>0 AS STRING),CAST(d*d>0 AS VARCHAR(3)),CAST(d*d>0 AS CHAR(8)),"
        + "CAST(d*d AS INT)+2,CASE WHEN id>=0 THEN CAST(d*d AS BIGINT) ELSE 2 END FROM src";
    for (String policy : List.of("ERROR", "DROP")) {
      var expected = assertAllRows(sql, nullable, policy, legacy);
      assertEquals(nullable ? null : -1, expected.get(0).get(0));
      assertEquals(nullable ? null : -1L, expected.get(0).get(1));
      assertEquals(nullable ? null : "", expected.get(0).get(6));
    }
  }

  private static TreeMap<Integer, List<Object>> assertAllRows(
      String sql, boolean nullable, String policy, boolean legacy) throws Exception {
    TreeMap<Integer, List<Object>> expected = null;
    List<DataType> expectedTypes = null;
    for (boolean nativeRun : new boolean[] {false, true}) {
      try (var metrics = new RecoveryExecutionMetrics()) {
        var env = metrics.environment(new org.apache.flink.configuration.Configuration(), 1, 0);
        var table = org.apache.flink.table.api.bridge.java.StreamTableEnvironment.create(env);
        var decimal = nullable ? DECIMAL(38, 18) : DECIMAL(38, 18).notNull();
        var type = ROW(FIELD("id", INT().notNull()), FIELD("d", decimal));
        var input = rows(nullable, 5003);
        table.getConfig().set("table.exec.sink.not-null-enforcer", policy);
        table.getConfig().set("table.exec.legacy-cast-behaviour", legacy ? "ENABLED" : "DISABLED");
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
        assertEquals(input.size(), actual.size(), "DROP must not hide overflow rows");
        if (nativeRun) {
          assertEquals(expectedTypes, types);
          assertEquals(expected, actual);
          assertTrue(scan.fallbackReasons().isEmpty(), scan.fallbackReasons().toString());
          RecoveryExecutionMetrics.assertConsumed(metrics.nativeOperators(), "NativeCalc");
        } else {
          expected = actual;
          expectedTypes = types;
        }
      }
    }
    return expected;
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void consumersConditionsAndEmptyBatchesRetainNullFlags(boolean nullable) throws Exception {
    java.util.function.Supplier<TableEnvironment> factory = () -> environment(nullable);
    BuiltinFunctionParity.assertParity(factory,
        "SELECT id,(d*d>0) OR CAST(NULL AS BOOLEAN),"
            + "CASE WHEN id<0 THEN d*d>0 ELSE CAST(NULL AS BOOLEAN) END,"
            + "(d*d>0) IS NULL FROM src");
    BuiltinFunctionParity.assertParity(factory,
        "SELECT id FROM src WHERE NOT(d*d>0) OR id=2");
    BuiltinFunctionParity.assertParity(factory,
        "SELECT id,(d*d>0) IS TRUE,(d*d>0) IS NOT TRUE,"
            + "CASE WHEN id>=0 THEN d*d>0 ELSE d/(id-id)>0 END FROM src");
    BuiltinFunctionParity.assertParity(factory,
        "SELECT d*d>0,d/(id-id)>0 FROM src WHERE id<0");
    NativeParity.assertChangelogParity(factory,
        "SELECT v,COUNT(*) FROM (SELECT d*d>0 AS v FROM src) GROUP BY v");
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void selectedOperandFailuresRemainFailures(boolean nullable) {
    var runs = NativeFailureParity.run(() -> environment(nullable),
        "SELECT d/(id-id)>0 FROM src");
    assertNotNull(runs.host().rootCause(), runs.toString());
    assertInstanceOf(ArithmeticException.class, runs.host().rootCause());
    if (nullable) assertInstanceOf(NativeException.class, runs.nativeRun().rootCause());
    else assertInstanceOf(ArithmeticException.class, runs.nativeRun().rootCause());
    for (var outcome : List.of(runs.host(), runs.nativeRun())) {
      assertTrue(outcome.rootCause().getMessage().contains("zero"), outcome.toString());
      assertEquals(NativeFailureParity.Phase.ROW_EVALUATION, outcome.phase());
    }
    assertEquals(NativeFailureParity.Route.NATIVE, runs.nativeRun().route());
  }

  private static TableEnvironment environment(boolean nullable) {
    var decimal = nullable ? DECIMAL(38, 18) : DECIMAL(38, 18).notNull();
    return BuiltinFunctionParity.environment(
        ROW(FIELD("id", INT().notNull()), FIELD("d", decimal)), rows(nullable, 7));
  }

  private static List<Row> rows(boolean nullable, int count) {
    BigDecimal max = new BigDecimal("99999999999999999999.999999999999999999");
    BigDecimal[] values = {max, max.negate(), BigDecimal.ZERO.setScale(18),
        BigDecimal.ONE.setScale(18), BigDecimal.ONE.negate().setScale(18),
        nullable ? null : BigDecimal.ZERO.setScale(18)};
    var result = new ArrayList<Row>();
    for (int i = 0; i < count; i++) result.add(Row.of(i, values[i % values.length]));
    return result;
  }
}
