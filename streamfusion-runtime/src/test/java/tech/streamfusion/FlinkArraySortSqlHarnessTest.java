package tech.streamfusion;

import static org.apache.flink.table.api.DataTypes.*;
import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class FlinkArraySortSqlHarnessTest {
  @ParameterizedTest
  @ValueSource(strings = {"TINYINT", "SMALLINT", "INT", "BIGINT", "DECIMAL"})
  void shortArraysRetainOrderNullPlacementAndDecimalScale(String type) throws Exception {
    for (String options : List.of("", ",FALSE", ",FALSE,TRUE", ",TRUE,FALSE"))
      FlinkArrayEditSqlHarnessTest.verify(
          type, "ARRAY_SORT(ARRAY_SLICE(a,1,31)" + options + ")", "ARRAY_SORT");
  }

  @ParameterizedTest
  @ValueSource(strings = {"TINYINT", "SMALLINT", "INT", "BIGINT"})
  void distinctArraysCanUseTheReleasedSortAtAnyLength(String type) throws Exception {
    FlinkArrayEditSqlHarnessTest.verify(type, "ARRAY_SORT(ARRAY_DISTINCT(a))", "ARRAY_SORT");
  }

  @Test
  void unrestrictedLongArraysRetainTheReleasedComparatorFailure() {
    tech.streamfusion.compat.FlinkTestCapabilities.requireSqlFunction("ARRAY_SORT");
    Integer[] values = {
      3, 1, 3, 4, 4, -3, 2, 0, 0, -2, 2, 1, -1, -4, 1, -3, 1, 3, -4, -2, -4, 2, -4, 1, -1, -3, 0, 1,
      0, 1, 3, 3, 1, -4, 3, 2, 3, 1, 1, 0, 2, -1, 2, -2, 2, -1, 1, -1, 0, -2, 1, 1, 2, -1, -3, -2,
      1, -2, -3, -1, 4, 1, 4, -2
    };
    var runs =
        NativeFailureParity.run(
            () ->
                BuiltinFunctionParity.environment(
                    ROW(FIELD("a", ARRAY(INT()))), List.of(Row.of((Object) values))),
            "SELECT ARRAY_SORT(a) FROM src");
    assertNotNull(runs.host().failure());
    runs.assertFailure(
        IllegalArgumentException.class,
        "Comparison method violates its general contract!",
        runs.host().phase(),
        NativeFailureParity.Route.NATIVE);
  }

  @Test
  void nullableRuntimeOptionsPreserveReleasedDefaults() throws Exception {
    tech.streamfusion.compat.FlinkTestCapabilities.requireSqlFunction("ARRAY_SORT");
    BuiltinFunctionParity.assertParity(
        () -> FlinkArrayEditSqlHarnessTest.environment("INT"),
        "SELECT ARRAY_SORT(ARRAY_SLICE(a,1,8),n>0,n<0) FROM src");
  }

  @ParameterizedTest
  @ValueSource(strings = {"TINYINT", "SMALLINT", "INT", "BIGINT", "DECIMAL"})
  void runtimeOptionsUseRustForVerifiedShortArrays(String type) throws Exception {
    for (String options :
        List.of(",n>0", ",n>0,n<0", ",CAST(NULL AS BOOLEAN),TRUE", ",TRUE,CAST(NULL AS BOOLEAN)")) {
      FlinkArrayEditSqlHarnessTest.verify(
          type, "ARRAY_SORT(ARRAY_SLICE(a,1,31)" + options + ")", "ARRAY_SORT");
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"TINYINT", "SMALLINT", "INT", "BIGINT"})
  void runtimeOptionsUseRustForDistinctArraysBeyondTheShortLimit(String type) throws Exception {
    FlinkArrayEditSqlHarnessTest.verify(
        type, "ARRAY_SORT(ARRAY_DISTINCT(a),n>0,n<0)", "ARRAY_SORT");
  }

  @ParameterizedTest
  @ValueSource(strings = {"TINYINT", "SMALLINT", "INT", "BIGINT", "DECIMAL"})
  void runtimeOptionsRequireConsumedNativeWork(String type) throws Exception {
    tech.streamfusion.compat.FlinkTestCapabilities.requireSqlFunction("ARRAY_SORT");
    try (var metrics = new RecoveryExecutionMetrics()) {
      var env = metrics.environment(new org.apache.flink.configuration.Configuration(), 1, 0);
      var tables = org.apache.flink.table.api.bridge.java.StreamTableEnvironment.create(env);
      var inputType = ROW(FIELD("id", INT()), FIELD("up", BOOLEAN()), FIELD("nf", BOOLEAN()));
      var source =
          tech.streamfusion.compat.FlinkTestSources.fromData(
              env,
              List.of(Row.of(1, true, false), Row.of(2, false, true), Row.of(3, null, true)),
              org.apache.flink.table.runtime.typeutils.ExternalTypeInfo.<Row>of(inputType));
      tables.createTemporaryView(
          "inputs",
          source,
          org.apache.flink.table.api.Schema.newBuilder().fromRowDataType(inputType).build());
      String element = FlinkArrayEditSqlHarnessTest.sqlType(type);
      String sql =
          "SELECT ARRAY_SORT(ARRAY[CAST(id AS "
              + element
              + "),CAST(NULL AS "
              + element
              + "),CAST(-id AS "
              + element
              + ")],up,nf) FROM inputs";
      var scan = tech.streamfusion.planner.NativePlanner.install(tables);
      String plan = tables.explainSql(sql);
      assertTrue(plan.contains("NativeCalc"), plan);
      assertTrue(
          scan.functionExecutions().stream()
              .anyMatch(route -> route.contains("ARRAY_SORT") && route.contains("jvm=[]")),
          scan.functionExecutions().toString());
      int rows = 0;
      try (var results = tables.executeSql(sql).collect()) {
        while (results.hasNext()) {
          results.next();
          rows++;
        }
      }
      assertEquals(3, rows);
      RecoveryExecutionMetrics.assertConsumed(metrics.nativeOperators(), "NativeCalc");
    }
  }

  @Test
  void distinctRuntimeSortKeepsLongArraysAndIndependentNullableFlags() throws Exception {
    tech.streamfusion.compat.FlinkTestCapabilities.requireSqlFunction("ARRAY_SORT");
    List<Row> rows = new java.util.ArrayList<>();
    Integer[] values = new Integer[66];
    for (int i = 1; i < values.length; i++) values[i] = 65 - i;
    for (Boolean ascending : new Boolean[] {true, false, null}) {
      for (Boolean nullFirst : new Boolean[] {true, false, null}) {
        rows.add(Row.of(values, ascending, nullFirst));
      }
    }
    java.util.function.Supplier<org.apache.flink.table.api.TableEnvironment> environment =
        () ->
            BuiltinFunctionParity.environment(
                ROW(FIELD("a", ARRAY(INT())), FIELD("up", BOOLEAN()), FIELD("nf", BOOLEAN())),
                rows);
    String sql = "SELECT ARRAY_SORT(ARRAY_DISTINCT(a),up,nf) FROM src";
    var table = environment.get();
    var scan = tech.streamfusion.planner.NativePlanner.install(table);
    table.explainSql(sql);
    assertTrue(
        scan.functionExecutions().stream()
            .anyMatch(route -> route.contains("ARRAY_SORT") && route.contains("jvm=[]")),
        scan.functionExecutions().toString());
    BuiltinFunctionParity.assertParity(environment, sql);
  }

  @Test
  void fallibleOptionsKeepEmptyBatchesAndConditionalEvaluation() throws Exception {
    tech.streamfusion.compat.FlinkTestCapabilities.requireSqlFunction("ARRAY_SORT");
    BuiltinFunctionParity.assertParity(
        () -> FlinkArrayEditSqlHarnessTest.environment("INT"),
        "SELECT ARRAY_SORT(ARRAY_SLICE(a,1,31),1/(id-id)>0) FROM src WHERE id<0");
    BuiltinFunctionParity.assertParity(
        () -> FlinkArrayEditSqlHarnessTest.environment("INT"),
        "SELECT CASE WHEN id<0 THEN ARRAY_SORT(ARRAY_SLICE(a,1,31),1/(id-id)>0) ELSE a END FROM"
            + " src");
  }
}
