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

class FlinkIntegralTruncateSqlHarnessTest {
  @ParameterizedTest
  @ValueSource(strings = {"INT", "BIGINT"})
  void boundariesNullsAndCompositionMatchReleasedFlink(String type) throws Exception {
    BuiltinFunctionParity.assertParity(
        () -> environment(type),
        "SELECT id, TRUNCATE(v), TRUNCATE(TRUNCATE(v)), FLOOR(TRUNCATE(v)),"
            + " TRUNCATE(ROUND(CEIL(v))), CAST(TRUNCATE(v) AS STRING), TRUNCATE(v)<0 FROM src");
    BuiltinFunctionParity.assertParity(
        () -> environment(type), "SELECT TRUNCATE(v) FROM src WHERE MOD(id,2)=0");
    BuiltinFunctionParity.assertParity(
        () -> environment(type), "SELECT id, TRUNCATE(v) FROM src WHERE TRUNCATE(v)>0");
    BuiltinFunctionParity.assertParity(
        () -> environment(type), "SELECT TRUNCATE(v) FROM src WHERE id<0");
    BuiltinFunctionParity.assertParity(
        () -> environment(type),
        "SELECT TRUNCATE(v+CAST(1 AS "
            + type
            + ")), TRUNCATE(-v), TRUNCATE(CAST(v AS BIGINT)) FROM src");
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
        "SELECT TRUNCATE(v), TRUNCATE(CAST(v AS BIGINT)) FROM src");
  }

  @ParameterizedTest
  @ValueSource(strings = {"INT", "BIGINT"})
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
      String sql =
          "SELECT TRUNCATE(v), TRUNCATE(TRUNCATE(v)), TRUNCATE(CAST(v AS BIGINT)) FROM src";
      assertTrue(table.explainSql(sql).contains("NativeCalc"));
      assertTrue(
          scan.functionExecutions().stream()
              .anyMatch(route -> route.contains("TRUNCATE") && route.contains("jvm=[]")),
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
        () -> environment(type),
        "SELECT TRUNCATE(CAST(v AS INT)), TRUNCATE(CAST(v AS BIGINT)) FROM src");
    var runs = NativeFailureParity.run(() -> environment(type), "SELECT TRUNCATE(v) FROM src");
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
            "TRUNCATE(v+1)",
            "TRUNCATE(-v)",
            "TRUNCATE(CAST(CAST(v AS SMALLINT) AS INT))",
            "TRUNCATE(v, -39)",
            "TRUNCATE(v, 39)",
            "TRUNCATE(v, id-1)",
            "TRUNCATE(1/(id-id))")) {
      var table = environment("INT");
      var scan = NativePlanner.install(table);
      table.explainSql("SELECT " + expression + " FROM src");
      assertTrue(
          scan.functionExecutions().stream()
              .anyMatch(
                  route ->
                      route.contains("jvm=[")
                          && route.contains("TRUNCATE")
                          && !route.contains("jvm=[]")),
          scan.functionExecutions().toString());
    }
  }

  @Test
  void explicitAndRuntimeScalesKeepTheirReleasedSemantics() throws Exception {
    BuiltinFunctionParity.assertParity(
        () -> environment("BIGINT"),
        "SELECT TRUNCATE(v,0), TRUNCATE(v,-1), TRUNCATE(v,2), TRUNCATE(v,MOD(id,5)-2),"
            + " TRUNCATE(v,CAST(NULL AS INT)) FROM src");
    BuiltinFunctionParity.assertParity(
        () -> environment("INT"), "SELECT TRUNCATE(v,-1), TRUNCATE(v,MOD(id,5)-2) FROM src");
  }

  @Test
  void emptyBatchesAndUnselectedBranchesDoNotEvaluateFailingArguments() throws Exception {
    BuiltinFunctionParity.assertParity(
        () -> environment("INT"), "SELECT TRUNCATE(1/(id-id)) FROM src WHERE id<0");
    BuiltinFunctionParity.assertParity(
        () -> environment("BIGINT"),
        "SELECT CASE WHEN id<0 THEN TRUNCATE(1/(id-id)) ELSE TRUNCATE(v) END FROM src");
    var runs =
        NativeFailureParity.run(() -> environment("INT"), "SELECT TRUNCATE(1/(id-id)) FROM src");
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
