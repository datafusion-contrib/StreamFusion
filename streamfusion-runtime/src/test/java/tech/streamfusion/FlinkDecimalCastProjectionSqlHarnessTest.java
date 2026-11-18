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

class FlinkDecimalCastProjectionSqlHarnessTest {
  @ParameterizedTest
  @CsvSource({"TINYINT,false", "TINYINT,true", "SMALLINT,false", "SMALLINT,true",
      "INT,false", "INT,true", "BIGINT,false", "BIGINT,true",
      "DECIMAL,false", "DECIMAL,true", "ROUND,false", "ROUND,true"})
  void overflowRetainsEveryProjectionDefaultAndRow(String source, boolean legacy) throws Exception {
    String decimal = cast(source);
    String sql = "SELECT id," + decimal + ">0,NOT(" + decimal + ">0),"
        + "CAST(" + decimal + " AS INT),CAST(" + decimal + " AS BIGINT),"
        + "CAST(" + decimal + " AS SMALLINT),CAST(" + decimal + " AS TINYINT),"
        + "CAST(" + decimal + " AS FLOAT),CAST(" + decimal + " AS DOUBLE),"
        + "CAST(" + decimal + ">0 AS STRING),CAST(" + decimal + ">0 AS CHAR(8)),"
        + "CAST(" + decimal + ">0 AS VARCHAR(3)),(" + decimal + ">0) AND id>=0,"
        + "(" + decimal + ">0) OR id<0,CASE WHEN id>=0 THEN " + decimal
        + ">0 ELSE id<0 END FROM src";
    for (boolean nullable : new boolean[] {false, true}) {
      for (String policy : List.of("ERROR", "DROP")) {
        TreeMap<Integer, List<Object>> expected = null;
        List<DataType> expectedTypes = null;
        for (boolean nativeRun : new boolean[] {false, true}) {
          try (var metrics = new RecoveryExecutionMetrics()) {
            var env = metrics.environment(new org.apache.flink.configuration.Configuration(), 1, 0);
            var table = org.apache.flink.table.api.bridge.java.StreamTableEnvironment.create(env);
            var type = inputType(source, nullable);
            var input = rows(source, nullable, 5003);
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
              RecoveryExecutionMetrics.assertConsumed(metrics.nativeOperators(), "NativeCalc");
            } else {
              expected = actual;
              expectedTypes = types;
              assertEquals(nullable ? null : false, expected.get(0).get(0));
              assertEquals(nullable ? null : false, expected.get(0).get(1));
              assertEquals(nullable ? null : -1, expected.get(0).get(2));
              assertEquals(nullable ? null : "", expected.get(0).get(8));
            }
          }
        }
      }
    }
  }

  @ParameterizedTest
  @CsvSource({"BIGINT,false", "BIGINT,true", "ROUND,false", "ROUND,true"})
  void intermediateNullsBranchesAndRelationalConsumersKeepTheirContracts(
      String source, boolean nullable) throws Exception {
    java.util.function.Supplier<TableEnvironment> factory =
        () -> environment(source, nullable, false, "ERROR");
    String decimal = cast(source);
    BuiltinFunctionParity.assertParity(factory,
        "SELECT id,(" + decimal + ">0) OR CAST(NULL AS BOOLEAN),(" + decimal
            + ">0) IS NULL,CASE WHEN id>=0 THEN " + decimal
            + ">0 ELSE CAST(1/(id-id) AS DECIMAL(1,0))>0 END FROM src");
    BuiltinFunctionParity.assertParity(factory,
        "SELECT id FROM src WHERE NOT(" + decimal + ">0) OR id=2");
    String flag = "(" + decimal + ">0 OR CAST(NULL AS BOOLEAN))";
    BuiltinFunctionParity.assertParity(factory, "SELECT id," + flag + " IS TRUE,"
        + flag + " IS NOT TRUE," + flag + " IS FALSE," + flag + " IS NOT FALSE FROM src");
    BuiltinFunctionParity.assertParity(factory,
        "SELECT " + decimal + ">0,CAST(1/(id-id) AS DECIMAL(1,0))>0 FROM src WHERE id<0");
    NativeParity.assertChangelogParity(factory,
        "SELECT flag,COUNT(*) FROM (SELECT " + decimal + ">0 AS flag FROM src) GROUP BY flag");
  }

  @ParameterizedTest
  @ValueSource(strings = {"BIGINT", "ROUND"})
  void selectedFailuresAndDirectDecimalConstraintsStillApply(String source) throws Exception {
    var runs = NativeFailureParity.run(() -> environment(source, false, false, "ERROR"),
        "SELECT CAST(v/(id-id) AS DECIMAL(1,0))>0 FROM src");
    assertInstanceOf(ArithmeticException.class, runs.host().rootCause(), runs.toString());
    assertEquals(NativeFailureParity.Route.NATIVE, runs.nativeRun().route());
    for (var outcome : List.of(runs.host(), runs.nativeRun())) {
      assertNotNull(outcome.rootCause(), outcome.toString());
      assertTrue(outcome.rootCause().getMessage().contains("zero"), outcome.toString());
      assertEquals(NativeFailureParity.Phase.ROW_EVALUATION, outcome.phase());
    }
    String sql = "SELECT id," + cast(source) + " FROM src";
    var constraints = NativeFailureParity.run(() -> environment(source, false, false, "ERROR"), sql);
    assertEquals(NativeFailureParity.Route.NATIVE, constraints.nativeRun().route());
    for (var outcome : List.of(constraints.host(), constraints.nativeRun())) {
      assertNotNull(outcome.rootCause(), outcome.toString());
      assertTrue(outcome.rootCause().getMessage().contains("is NOT NULL"), outcome.toString());
    }
    BuiltinFunctionParity.assertParity(() -> environment(source, false, false, "DROP"), sql);
  }

  @ParameterizedTest
  @ValueSource(strings = {"TINYINT", "SMALLINT", "INT", "BIGINT", "DECIMAL", "ROUND"})
  void safeCastsAndNullAwareConsumersKeepEmptyEvaluatorDescriptors(String source) throws Exception {
    try (var metrics = new RecoveryExecutionMetrics()) {
      java.util.function.Supplier<TableEnvironment> factory = () -> {
        var env = metrics.environment(new org.apache.flink.configuration.Configuration(), 1, 0);
        var table = org.apache.flink.table.api.bridge.java.StreamTableEnvironment.create(env);
        var type = inputType(source, false);
        table.createTemporaryView("src", tech.streamfusion.compat.FlinkTestSources.fromData(env,
            rows(source, false, 31), org.apache.flink.table.runtime.typeutils.ExternalTypeInfo.<Row>of(type)),
            org.apache.flink.table.api.Schema.newBuilder().fromRowDataType(type).build());
        configure(table, false, "ERROR");
        return table;
      };
      String target = source.equals("ROUND") ? "DECIMAL(3,0)" : "DECIMAL(38,2)";
      String flag = "(" + cast(source) + ">0 OR CAST(NULL AS BOOLEAN))";
      for (String sql : List.of("SELECT id,CAST(v AS " + target + ")>0 FROM src",
          "SELECT id," + flag + " IS NULL," + flag + " IS NOT NULL,"
              + flag + " IS NOT TRUE," + flag + " IS FALSE FROM src")) {
        var table = factory.get();
        var scan = NativePlanner.install(table);
        assertTrue(table.explainSql(sql).contains("NativeCalc"));
        assertTrue(scan.fallbackReasons().isEmpty(), scan.fallbackReasons().toString());
        assertFalse(scan.functionExecutions().isEmpty());
        assertTrue(scan.functionExecutions().stream().allMatch(route -> route.contains("jvm=[]")),
            scan.functionExecutions().toString());
        BuiltinFunctionParity.assertParity(factory, sql);
      }
      RecoveryExecutionMetrics.assertConsumed(metrics.nativeOperators(), "NativeCalc");
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"DECIMAL", "ROUND"})
  void tryCastKeepsItsNullableResult(String source) throws Exception {
    String target = source.equals("ROUND") ? "DECIMAL(2,0)" : "DECIMAL(1,0)";
    for (boolean legacy : new boolean[] {false, true}) {
      BuiltinFunctionParity.assertParity(() -> environment(source, false, legacy, "ERROR"),
          "SELECT id,TRY_CAST(v AS " + target + "),TRY_CAST(v AS " + target + ")>0 FROM src");
    }
  }

  private static TableEnvironment environment(
      String source, boolean nullable, boolean legacy, String policy) {
    var table = BuiltinFunctionParity.environment(inputType(source, nullable), rows(source, nullable, 31));
    configure(table, legacy, policy);
    return table;
  }

  private static void configure(TableEnvironment table, boolean legacy, String policy) {
    table.getConfig().set("table.exec.legacy-cast-behaviour", legacy ? "ENABLED" : "DISABLED");
    table.getConfig().set("table.exec.sink.not-null-enforcer", policy);
  }

  private static DataType inputType(String source, boolean nullable) {
    DataType value;
    switch (source) {
      case "TINYINT": value = TINYINT(); break;
      case "SMALLINT": value = SMALLINT(); break;
      case "INT": value = INT(); break;
      case "BIGINT": value = BIGINT(); break;
      case "DECIMAL": value = DECIMAL(18, 2); break;
      case "ROUND": value = DECIMAL(3, 1); break;
      default: throw new IllegalArgumentException(source);
    }
    return ROW(FIELD("id", INT().notNull()), FIELD("v", nullable ? value : value.notNull()));
  }

  private static String cast(String source) {
    return "CAST(v AS DECIMAL(" + (source.equals("ROUND") ? "2" : "1") + ",0))";
  }

  private static List<Row> rows(String source, boolean nullable, int count) {
    int[] values = {99, -99, 1, -1, 0, 9, -9};
    var rows = new ArrayList<Row>();
    for (int id = 0; id < count; id++) {
      int value = values[id % values.length];
      Object field;
      switch (source) {
        case "TINYINT": field = (byte) value; break;
        case "SMALLINT": field = (short) value; break;
        case "INT": field = value; break;
        case "BIGINT": field = (long) value; break;
        default:
          field = source.equals("ROUND") && Math.abs(value) == 99
              ? BigDecimal.valueOf(value > 0 ? 995 : -995, 1)
              : BigDecimal.valueOf(value).setScale(source.equals("ROUND") ? 1 : 2);
      }
      rows.add(Row.of(id, nullable && id % 7 == 6 ? null : field));
    }
    return rows;
  }
}
