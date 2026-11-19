package tech.streamfusion.planner;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.util.List;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class NativeOperatorDescriptionTest {
  @TempDir Path directory;

  @Test
  void absorbedUnnestProjectionKeepsTheFinalFilterSelection() {
    var execution = StreamExecutionEnvironment.getExecutionEnvironment();
    var table = StreamTableEnvironment.create(execution);
    NativePlanner.install(table);
    table.executeSql(
        "CREATE TABLE input_rows (id INT, items ARRAY<INT>) WITH ('connector'='datagen')");
    String plan =
        table.explainSql(
            "SELECT item AS chosen FROM input_rows CROSS JOIN UNNEST(items) AS u(item) WHERE item >"
                + " id");
    String filter =
        plan.lines().filter(line -> line.contains("NativeFilter(")).findFirst().orElseThrow();
    assertTrue(filter.contains("AS chosen]"), filter);
    assertTrue(filter.contains("where=["), filter);
    assertTrue(filter.contains("nativeFunctions=[]"), filter);
    assertTrue(filter.contains("jvmFunctions=[]"), filter);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void sourceRetainsTableAndPrunedFieldsInTheSubmittedGraph(boolean simplify) {
    var execution = StreamExecutionEnvironment.getExecutionEnvironment();
    execution.setParallelism(1);
    var table = StreamTableEnvironment.create(execution);
    table.getConfig().set("table.exec.simplify-operator-name-enabled", "" + simplify);
    NativePlanner.install(table);
    table.executeSql(
        "CREATE TABLE input_rows (id INT, unused STRING) WITH ("
            + "'connector'='kafka', 'topic'='input', "
            + "'properties.bootstrap.servers'='broker:9092', "
            + "'scan.startup.mode'='earliest-offset', 'format'='json')");
    table.executeSql("CREATE TABLE output_rows (id INT) WITH ('connector'='blackhole')");
    String insert = "INSERT INTO output_rows SELECT id + 1 FROM input_rows";
    String plan = table.explainSql(insert);
    assertTrue(
        plan.contains("NativeKafkaDecode(table=[[default_catalog, default_database, input_rows]]"),
        plan);
    assertTrue(plan.contains("fields=[id]"), plan);

    table.createStatementSet().addInsertSql(insert).attachAsDataStream();
    String name =
        execution.getStreamGraph().getStreamNodes().stream()
            .map(node -> node.getOperatorName())
            .filter(value -> value.contains("NativeSource("))
            .findFirst()
            .orElseThrow();
    assertTrue(name.contains("table=[[default_catalog, default_database, input_rows]]"), name);
    assertTrue(name.contains("fields=[id]"), name);
    assertFalse(name.contains("unused"), name);
    assertFalse(name.contains("reuseBarrier"), name);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void sinkRetainsOriginalTableAndFieldsInTheSubmittedGraph(boolean simplify) {
    var execution = StreamExecutionEnvironment.getExecutionEnvironment();
    execution.setParallelism(1);
    var table = StreamTableEnvironment.create(execution);
    table.getConfig().set("table.exec.simplify-operator-name-enabled", "" + simplify);
    NativePlanner.install(table);
    table.executeSql("CREATE TABLE input_rows (id INT, label STRING) WITH ('connector'='datagen')");
    table.executeSql(
        "CREATE TABLE output_rows (id INT, label STRING) WITH ("
            + "'connector'='filesystem', 'path'='"
            + directory.toUri()
            + "', 'format'='parquet')");
    String insert = "INSERT INTO output_rows SELECT * FROM input_rows";
    String plan = table.explainSql(insert);
    assertTrue(
        plan.contains("NativeParquetSink(table=[default_catalog.default_database.output_rows]"),
        plan);
    assertTrue(plan.contains("fields=[id, label]"), plan);

    table.createStatementSet().addInsertSql(insert).attachAsDataStream();
    String name =
        execution.getStreamGraph().getStreamNodes().stream()
            .map(node -> node.getOperatorName())
            .filter(value -> value.contains("NativeSink("))
            .findFirst()
            .orElseThrow();
    assertTrue(name.contains("table=[default_catalog.default_database.output_rows]"), name);
    assertTrue(name.contains("fields=[id, label]"), name);
    assertFalse(name.contains("reuseBarrier"), name);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void calcDetailsAndFunctionPlacementReachExplainAndSubmittedGraph(boolean simplify) {
    var execution = StreamExecutionEnvironment.getExecutionEnvironment();
    execution.setParallelism(1);
    var table = StreamTableEnvironment.create(execution);
    table.getConfig().set("table.exec.simplify-operator-name-enabled", "" + simplify);
    PhysicalPlanScan scan = NativePlanner.install(table);
    table.createTemporarySystemFunction("JVM_ONLY", JvmOnlyFunction.class);
    table.executeSql(
        "CREATE TABLE input_rows (id INT, unused STRING) WITH ('connector'='datagen')");
    table.executeSql("CREATE TABLE output_rows (id INT, size INT) WITH ('connector'='blackhole')");
    String insert =
        "INSERT INTO output_rows SELECT JVM_ONLY(id), "
            + "CHAR_LENGTH(CAST(id + 1 AS STRING)) FROM input_rows WHERE id > 0";

    String plan = table.explainSql(insert);
    String calc =
        plan.lines().filter(line -> line.contains("NativeCalc(")).findFirst().orElseThrow();
    assertTrue(calc.contains("select=["), calc);
    assertTrue(calc.contains("where=["), calc);
    assertTrue(calc.contains("id"), calc);
    assertFalse(calc.contains("unused"), calc);
    assertTrue(calc.contains("nativeFunctions=[CAST, CHAR_LENGTH]"), calc);
    assertTrue(calc.contains("jvmFunctions=[JVM_ONLY]"), calc);
    assertTrue(
        scan.explainSummary()
            .contains("NativeCalc functions: native=[CAST, CHAR_LENGTH], jvm=[JVM_ONLY]"),
        scan.explainSummary());

    table.createStatementSet().addInsertSql(insert).attachAsDataStream();
    List<String> names =
        execution.getStreamGraph().getStreamNodes().stream()
            .map(node -> node.getOperatorName())
            .collect(java.util.stream.Collectors.toList());
    String name =
        names.stream().filter(value -> value.contains("NativeCalc(")).findFirst().orElseThrow();
    assertTrue(name.contains("select="), name);
    assertTrue(name.contains("where="), name);
    assertTrue(name.contains("nativeFunctions=[CAST, CHAR_LENGTH]"), name);
    assertTrue(name.contains("jvmFunctions=[JVM_ONLY]"), name);
    assertFalse(name.contains("reuseBarrier"), name);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void filterAndMixedCalcRetainTheirActualExecutionPlacement(boolean filter) {
    var execution = StreamExecutionEnvironment.getExecutionEnvironment();
    var table = StreamTableEnvironment.create(execution);
    PhysicalPlanScan scan = NativePlanner.install(table);
    table.createTemporarySystemFunction("JVM_ONLY", JvmOnlyFunction.class);
    table.executeSql("CREATE TABLE input_rows (id INT, text STRING) WITH ('connector'='datagen')");
    String query =
        filter
            ? "SELECT id FROM input_rows WHERE CHAR_LENGTH(text) > 1"
            : "SELECT JVM_ONLY(id), JVM_ONLY(CAST(id AS BIGINT)) FROM input_rows";
    String plan = table.explainSql(query);
    String node =
        plan.lines()
            .filter(line -> line.contains(filter ? "NativeFilter(" : "NativeCalc("))
            .findFirst()
            .orElseThrow();
    assertTrue(node.contains("select=["), node);
    if (filter) {
      assertTrue(node.contains("where=["), node);
      assertTrue(node.contains("nativeFunctions=[CHAR_LENGTH]"), node);
      assertTrue(node.contains("jvmFunctions=[]"), node);
    } else {
      assertTrue(node.contains("nativeFunctions=[CAST]"), node);
      assertTrue(node.contains("jvmFunctions=[JVM_ONLY]"), node);
      assertTrue(
          scan.explainSummary().contains("reason=host-exact or scalar-UDF implementation selected"),
          scan.explainSummary());
    }
  }

  public static class JvmOnlyFunction extends ScalarFunction {
    public Integer eval(Integer value) {
      return value;
    }

    public Long eval(Long value) {
      return value;
    }
  }
}
