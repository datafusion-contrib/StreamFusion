package tech.streamfusion;

import static org.apache.flink.table.api.DataTypes.*;
import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.List;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.planner.NativePlanner;

class FlinkIntegralSignSqlHarnessTest {
  @ParameterizedTest
  @ValueSource(strings = {"INT", "BIGINT"})
  void boundariesNullsAndCompositionMatchReleasedFlink(String type) throws Exception {
    BuiltinFunctionParity.assertParity(
        () -> environment(type),
        "SELECT id, SIGN(v), SIGN(SIGN(v)), CAST(SIGN(v) AS STRING), SIGN(v)<0 FROM src");
    BuiltinFunctionParity.assertParity(
        () -> environment(type), "SELECT SIGN(v) FROM src WHERE MOD(id,2)=0");
    BuiltinFunctionParity.assertParity(
        () -> environment(type), "SELECT id, SIGN(v) FROM src WHERE SIGN(v)>0");
    BuiltinFunctionParity.assertParity(
        () -> environment(type), "SELECT SIGN(v) FROM src WHERE id<0");
    BuiltinFunctionParity.assertParity(
        () -> environment(type),
        "SELECT SIGN(v+CAST(1 AS " + type + ")), SIGN(-v), SIGN(CAST(v AS BIGINT)) FROM src");
  }

  @ParameterizedTest
  @ValueSource(strings = {"INT", "BIGINT"})
  void nonnullableInputsRetainTheirResolvedWidth(String type) throws Exception {
    BuiltinFunctionParity.assertParity(
        () ->
            BuiltinFunctionParity.environment(
                ROW(FIELD("id", INT()), FIELD("v", dataType(type).notNull())),
                rows(type, 39).stream()
                    .filter(row -> row.getField(1) != null)
                    .collect(java.util.stream.Collectors.toList())),
        "SELECT SIGN(v), SIGN(CAST(v AS BIGINT)) FROM src");
  }

  @ParameterizedTest
  @ValueSource(strings = {"TINYINT", "SMALLINT", "INT", "BIGINT"})
  void admittedFunctionConsumesNativeBatchesWithoutJvmEvaluation(String type) throws Exception {
    try (var metrics = new RecoveryExecutionMetrics()) {
      var env = metrics.environment(new org.apache.flink.configuration.Configuration(), 1, 0);
      var table = org.apache.flink.table.api.bridge.java.StreamTableEnvironment.create(env);
      DataType inputType = ROW(FIELD("id", INT()), FIELD("v", dataType(type)));
      table.createTemporaryView(
          "src",
          tech.streamfusion.compat.FlinkTestSources.fromData(
              env,
              rows(type, 5003),
              org.apache.flink.table.runtime.typeutils.ExternalTypeInfo.<Row>of(inputType)),
          org.apache.flink.table.api.Schema.newBuilder().fromRowDataType(inputType).build());
      var scan = NativePlanner.install(table);
      String operand = type.equals("TINYINT") || type.equals("SMALLINT") ? "CAST(v AS INT)" : "v";
      String sql =
          "SELECT SIGN("
              + operand
              + "), SIGN(SIGN("
              + operand
              + ")), SIGN(CAST(v AS BIGINT)) FROM src";
      assertTrue(table.explainSql(sql).contains("NativeCalc"));
      assertTrue(
          scan.functionExecutions().stream()
              .anyMatch(route -> route.contains("SIGN") && route.contains("jvm=[]")),
          scan.functionExecutions().toString());
      int count = 0;
      try (var results = table.executeSql(sql).collect()) {
        while (results.hasNext()) {
          results.next();
          count++;
        }
      }
      assertEquals(5003, count);
      assertTrue(scan.fallbackReasons().isEmpty(), scan.fallbackReasons().toString());
      RecoveryExecutionMetrics.assertConsumed(metrics.nativeOperators(), "NativeCalc");
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"TINYINT", "SMALLINT"})
  void narrowInputsRequireExplicitWidening(String type) throws Exception {
    BuiltinFunctionParity.assertParity(
        () -> environment(type), "SELECT SIGN(CAST(v AS INT)), SIGN(CAST(v AS BIGINT)) FROM src");
    var runs = NativeFailureParity.run(() -> environment(type), "SELECT SIGN(v) FROM src");
    runs.assertFailure(
        org.codehaus.commons.compiler.CompileException.class,
        "Assignment conversion not possible",
        runs.host().phase(),
        NativeFailureParity.Route.FALLBACK);
    String destination = type.equals("TINYINT") ? "byte" : "short";
    assertTrue(runs.host().rootCause().getMessage().contains(destination));
    assertTrue(runs.nativeRun().rootCause().getMessage().contains(destination));
  }

  @Test
  void unverifiedProducersKeepTheGeneratedProgram() {
    for (String expression :
        List.of(
            "SIGN(v+MOD(v,2))", "SIGN(-v)", "SIGN(CAST(CAST(v AS SMALLINT) AS INT))", "SIGN(1/(id-id))")) {
      var table = environment("INT");
      var scan = NativePlanner.install(table);
      table.explainSql("SELECT " + expression + " FROM src");
      assertTrue(
          scan.functionExecutions().stream()
              .anyMatch(
                  route ->
                      route.contains("jvm=[")
                          && route.contains("SIGN")
                          && !route.contains("jvm=[]")),
          scan.functionExecutions().toString());
    }
  }

  @Test
  void emptyBatchesAndUnselectedBranchesDoNotEvaluateFailingArguments() throws Exception {
    BuiltinFunctionParity.assertParity(
        () -> environment("INT"), "SELECT SIGN(1/(id-id)) FROM src WHERE id<0");
    BuiltinFunctionParity.assertParity(
        () -> environment("BIGINT"),
        "SELECT CASE WHEN id<0 THEN SIGN(1/(id-id)) ELSE SIGN(v) END FROM src");
    var runs = NativeFailureParity.run(() -> environment("INT"), "SELECT SIGN(1/(id-id)) FROM src");
    assertNotNull(runs.host().failure());
    runs.assertFailure(
        ArithmeticException.class,
        "/ by zero",
        runs.host().phase(),
        NativeFailureParity.Route.NATIVE);
  }

  private static TableEnvironment environment(String type) {
    return BuiltinFunctionParity.environment(
        ROW(FIELD("id", INT()), FIELD("v", dataType(type))), rows(type, 39));
  }

  private static DataType dataType(String type) {
    switch (type) {
      case "TINYINT":
        return TINYINT();
      case "SMALLINT":
        return SMALLINT();
      case "INT":
        return INT();
      case "BIGINT":
        return BIGINT();
      default:
        throw new IllegalArgumentException(type);
    }
  }

  private static List<Row> rows(String type, int count) {
    List<Row> rows = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      Object value;
      switch (type) {
        case "TINYINT":
          value =
              new Byte[] {Byte.MIN_VALUE, Byte.MIN_VALUE + 1, -1, 0, 1, Byte.MAX_VALUE, null}
                  [i % 7];
          break;
        case "SMALLINT":
          value =
              new Short[] {Short.MIN_VALUE, Short.MIN_VALUE + 1, -1, 0, 1, Short.MAX_VALUE, null}
                  [i % 7];
          break;
        case "INT":
          value =
              new Integer[] {
                    Integer.MIN_VALUE, Integer.MIN_VALUE + 1, -1, 0, 1, Integer.MAX_VALUE, null
                  }
                  [i % 7];
          break;
        case "BIGINT":
          value =
              new Long[] {Long.MIN_VALUE, Long.MIN_VALUE + 1, -1L, 0L, 1L, Long.MAX_VALUE, null}
                  [i % 7];
          break;
        default:
          throw new IllegalArgumentException(type);
      }
      rows.add(Row.of(i, value));
    }
    return rows;
  }
}
