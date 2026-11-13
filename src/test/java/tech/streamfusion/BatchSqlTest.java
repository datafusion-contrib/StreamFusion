package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.TableEnvironment;
import org.junit.jupiter.api.Test;

class BatchSqlTest {
  @Test
  void sharedTransformationsKeepSimplifiedNamesAndOnlyPerimeterTransposes() throws Exception {
    Configuration config = new Configuration();
    config.setString("pipeline.operator-chaining.enabled", "false");
    config.setString("table.exec.simplify-operator-name-enabled", "true");
    try (var metrics = new RecoveryExecutionMetrics()) {
      metrics.setAsContext(config, 1, 0);
      var nativeEnv = environment(true, 13, config);
      String sql =
          "SELECT SHA2(CAST(id AS STRING), 256) FROM input WHERE id < 5 UNION ALL "
              + "SELECT SHA2(CAST(id AS STRING), 256) FROM input WHERE id >= 5";
      nativeEnv.executeSql("CREATE TABLE output(v STRING) WITH ('connector'='blackhole')");
      String plan = nativeEnv.explainSql("INSERT INTO output " + sql);
      String physical =
          plan.substring(
              plan.indexOf("== Optimized Physical Plan =="),
              plan.indexOf("== Optimized Execution Plan =="));
      assertEquals(2, physical.split("RowDataToArrow", -1).length - 1, physical);
      assertEquals(1, physical.split("ArrowToRowData", -1).length - 1, physical);
      assertEquals(collect(environment(false, 13, config), sql), collect(nativeEnv, sql));
      RecoveryExecutionMetrics.assertConsumed(metrics.nativeOperators(), "NativeCalc");
      for (String operator : List.of("RowDataToArrow", "ArrowToRowData")) {
        var names = metrics.taskNames(operator);
        assertFalse(names.isEmpty(), operator);
        assertTrue(
            names.stream().allMatch(name -> name.startsWith(operator + "ExecNode[")),
            names.toString());
      }
    } finally {
      tech.streamfusion.compat.FlinkTestContexts.clearForRemoteSubmission();
    }
  }

  @Test
  void parallelBoundedInputsPreserveResults() throws Exception {
    try (var metrics = new RecoveryExecutionMetrics()) {
      metrics.setAsContext(new Configuration(), 2, 0);
      var nativeEnv = environment(true, 2051);
      var stock = environment(false, 2051);
      nativeEnv.getConfig().set("parallelism.default", "2");
      stock.getConfig().set("parallelism.default", "2");
      String sql = "SELECT SHA2(CAST(id * 3 AS STRING), 256) FROM input WHERE MOD(id, 2) = 0";
      String plan = nativeEnv.explainSql(sql);
      assertTrue(plan.contains("NativeCalc"), plan);
      assertEquals(collect(stock, sql), collect(nativeEnv, sql));
      RecoveryExecutionMetrics.assertConsumed(metrics.nativeOperators(), "NativeCalc");
    } finally {
      tech.streamfusion.compat.FlinkTestContexts.clearForRemoteSubmission();
    }
  }

  @Test
  void batchCompilationRetainsTheStockHostContract() throws Exception {
    var stock = environment(false, 3);
    var nativeEnv = environment(true, 3);
    for (var env : List.of(stock, nativeEnv))
      env.executeSql("CREATE TABLE output (value_string STRING) WITH ('connector'='blackhole')");
    String sql = "INSERT INTO output SELECT SHA2(CAST(id AS STRING), 256) FROM input";
    try {
      stock.compilePlanSql(sql);
    } catch (UnsupportedOperationException unsupported) {
      org.junit.jupiter.api.Assertions.assertThrows(
          UnsupportedOperationException.class, () -> nativeEnv.compilePlanSql(sql));
      return;
    }
    String plan = nativeEnv.compilePlanSql(sql).asJsonString();
    assertFalse(plan.contains("batch-exec-native"), plan);
  }

  @Test
  void unchainedBatchStagesExchangeArrowIpc() throws Exception {
    Configuration config = new Configuration();
    config.setString("pipeline.operator-chaining.enabled", "false");
    try (var metrics = new RecoveryExecutionMetrics()) {
      metrics.setAsContext(config, 1, 0);
      String sql = "SELECT SHA2(CAST(id * 2 AS STRING), 256) FROM input WHERE MOD(id, 3) = 0";
      assertEquals(
          collect(environment(false, 3107, config), sql),
          collect(environment(true, 3107, config), sql));
      RecoveryExecutionMetrics.assertConsumed(metrics.nativeOperators(), "NativeCalc");
      assertTrue(
          metrics.taskNames("NativeCalc").stream().allMatch(name -> name.startsWith("NativeCalc")),
          metrics.taskNames("NativeCalc").toString());
    } finally {
      tech.streamfusion.compat.FlinkTestContexts.clearForRemoteSubmission();
    }
  }

  @Test
  void calcFlushesShortAndEmptyInputsAndMatchesStock() throws Exception {
    for (int count : new int[] {0, 3, 2051}) {
      try (var metrics = new RecoveryExecutionMetrics()) {
        metrics.setAsContext(new Configuration(), 1, 0);
        TableEnvironment nativeEnv = environment(true, count);
        String sql =
            "SELECT id, id * 3 + 7 AS calculated, SHA2(CAST(id AS STRING), 256) "
                + "FROM input WHERE MOD(id, 2) = 0 AND id < "
                + count;
        String plan = nativeEnv.explainSql(sql);
        if (count > 0) assertTrue(plan.contains("NativeCalc"), plan);
        List<String> actual = collect(nativeEnv, sql);
        List<String> expected = collect(environment(false, count), sql);
        assertEquals(expected, actual, "count=" + count);
        if (count > 0)
          RecoveryExecutionMetrics.assertConsumed(metrics.nativeOperators(), "NativeCalc");
      } finally {
        tech.streamfusion.compat.FlinkTestContexts.clearForRemoteSubmission();
      }
    }
  }

  @Test
  void unionCarriesArrowBetweenCalcs() throws Exception {
    try (var metrics = new RecoveryExecutionMetrics()) {
      metrics.setAsContext(new Configuration(), 1, 0);
      String sql =
          "SELECT SHA2(CAST(id * 2 AS STRING), 256) FROM input WHERE id < 5 UNION ALL "
              + "SELECT SHA2(CAST(id * 3 AS STRING), 256) FROM input WHERE id >= 5";
      var nativeEnv = environment(true, 13);
      String plan = nativeEnv.explainSql(sql);
      assertTrue(plan.contains("NativeUnion"), plan);
      assertEquals(collect(environment(false, 13), sql), collect(nativeEnv, sql));
      RecoveryExecutionMetrics.assertConsumed(metrics.nativeOperators(), "NativeCalc");
    } finally {
      tech.streamfusion.compat.FlinkTestContexts.clearForRemoteSubmission();
    }
  }

  @Test
  void unsupportedInteriorRetainsWholeQueryOnStock() throws Exception {
    var env = environment(true, 10);
    String plan = env.explainSql("SELECT SUM(id * 2) FROM input");
    assertFalse(plan.contains("NativeCalc"), plan);
    String cheap = tech.streamfusion.planner.NativePlanner.explain(env, "SELECT id + 1 FROM input");
    assertFalse(cheap.contains("NativeCalc("), cheap);
    assertTrue(cheap.contains("standalone scalar pipeline has no verified speedup"), cheap);
    env.getConfig().set("streamfusion.native.enabled", "false");
    assertFalse(env.explainSql("SELECT id + 1 FROM input").contains("NativeCalc"));
  }

  static TableEnvironment environment(boolean nativeRun, int count) throws Exception {
    return environment(nativeRun, count, new Configuration());
  }

  static TableEnvironment environment(boolean nativeRun, int count, Configuration options)
      throws Exception {
    Configuration config = new Configuration(options);
    config.setString("streamfusion.native.enabled", Boolean.toString(nativeRun));
    config.setString("parallelism.default", "1");
    var env = NativePlannerTestEnvironment.createBatch(config);
    env.executeSql(
        "CREATE TABLE input (id BIGINT) WITH ('connector'='datagen', "
            + "'rows-per-second'='100000000', "
            + "'number-of-rows'='"
            + Math.max(1, count)
            + "', 'fields.id.kind'='sequence', "
            + "'fields.id.start'='0', 'fields.id.end'='"
            + Math.max(0, count - 1)
            + "')");
    return env;
  }

  static List<String> collect(TableEnvironment env, String sql) throws Exception {
    List<String> result = new ArrayList<>();
    try (var rows = env.executeSql(sql).collect()) {
      while (rows.hasNext()) result.add(rows.next().toString());
    }
    result.sort(Comparator.naturalOrder());
    return result;
  }
}
