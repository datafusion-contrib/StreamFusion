package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.List;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.TableEnvironment;
import org.junit.jupiter.api.Test;

class BatchSortSqlTest {
  @Test
  void failedConsumerReplaysTheCompletedBlockingArrowShuffle() throws Exception {
    Configuration config = new Configuration();
    config.setString("restart-strategy.type", "fixed-delay");
    config.setString("restart-strategy.fixed-delay.attempts", "1");
    config.setString("restart-strategy.fixed-delay.delay", "1 ms");
    config.setString("streamfusion.native.enabled", "true");
    config.setString("parallelism.default", "2");
    config.setString("table.optimizer.range-sort.enabled", "false");
    BatchReplaySinkFactory.FAILED.set(false);
    try (var metrics = new RecoveryExecutionMetrics()) {
      metrics.setAsContext(config, 2, 128L << 20);
      var env = NativePlannerTestEnvironment.createBatch(config, BatchReplaySinkFactory.class);
      env.executeSql(
          "CREATE TABLE input(id BIGINT) WITH ('connector'='datagen',"
              + " 'rows-per-second'='100000000', 'number-of-rows'='2051',"
              + " 'fields.id.kind'='sequence', 'fields.id.start'='0', 'fields.id.end'='2050')");
      env.executeSql("CREATE TABLE output(id BIGINT) WITH ('connector'='batch-replay')");
      String sql = "INSERT INTO output SELECT id FROM input ORDER BY id";
      String plan = env.explainSql(sql);
      assertTrue(plan.contains("NativeSort") && plan.contains("NativeExchange"), plan);
      var result = env.executeSql(sql);
      result.await();
      var completed = result.getJobClient().orElseThrow().getJobExecutionResult().get();
      List<Long> rows = completed.getAccumulatorResult(BatchReplaySinkFactory.RESULTS);
      assertTrue(BatchReplaySinkFactory.FAILED.get(), "failure injection did not run");
      assertEquals(2051, rows.size());
      for (int index = 0; index < rows.size(); index++) assertEquals((long) index, rows.get(index));
      assertTrue(
          metrics.attempts("NativeSort").contains("1")
              || metrics.attempts("ArrowToRowData").contains("1"),
          metrics.nativeOperators().toString());
      assertEquals(
          java.util.Set.of("0"),
          metrics.attempts("RowDataToArrow"),
          "the completed shuffle producer should not restart");
      System.out.printf(
          "[batch-sort-replay] sourceAttempts=%s sortAttempts=%s exitAttempts=%s rows=%d%n",
          metrics.attempts("RowDataToArrow"),
          metrics.attempts("NativeSort"),
          metrics.attempts("ArrowToRowData"),
          rows.size());
    } finally {
      tech.streamfusion.compat.FlinkTestContexts.clearForRemoteSubmission();
    }
  }

  @Test
  void nullableIntegerSortPreservesOrderAcrossBlockingArrowShuffle() throws Exception {
    Configuration config = new Configuration();
    config.setString("pipeline.operator-chaining.enabled", "false");
    try (var metrics = new RecoveryExecutionMetrics()) {
      metrics.setAsContext(config, 2, 128L << 20);
      var nativeEnv = environment(true, 2051, config);
      var stock = environment(false, 2051, config);
      String sql =
          "SELECT CASE WHEN MOD(id, 9) = 0 THEN CAST(NULL AS INT) ELSE CAST(MOD(id, 7) AS INT) END"
              + " AS k, id FROM input ORDER BY k DESC NULLS LAST, id ASC";
      String plan = nativeEnv.explainSql(sql);
      assertTrue(plan.contains("NativeSort"), plan);
      assertTrue(plan.contains("NativeExchange"), plan);
      nativeEnv.executeSql("CREATE TABLE output(k INT, id BIGINT) WITH ('connector'='blackhole')");
      String insert = nativeEnv.explainSql("INSERT INTO output " + sql);
      assertTrue(insert.contains("RowDataToArrow") && insert.contains("ArrowToRowData"), insert);
      var result = nativeEnv.executeSql(sql);
      String job = result.getJobClient().orElseThrow().getJobID().toString();
      List<String> actual = collect(result);
      assertEquals(java.util.Set.of("0"), metrics.subtasks("NativeSort"));
      assertEquals(java.util.Set.of("0"), metrics.subtasks("ArrowToRowData"));
      List<String> expected = collect(stock.executeSql(sql));
      assertEquals(expected.size(), actual.size());
      for (int row = 0; row < expected.size(); row++)
        assertEquals(expected.get(row), actual.get(row), "row=" + row);
      assertEquals(2051, metrics.recordsIn(job, "NativeSort"));
      assertTrue(
          metrics.taskNames("NativeSort").stream().allMatch(name -> name.startsWith("NativeSort")));
    } finally {
      tech.streamfusion.compat.FlinkTestContexts.clearForRemoteSubmission();
    }
  }

  @Test
  void emptyAndShortSortInputsFinish() throws Exception {
    try (var metrics = new RecoveryExecutionMetrics()) {
      metrics.setAsContext(new Configuration(), 1, 128L << 20);
      for (int count : new int[] {0, 3}) {
        String sql = "SELECT id FROM input WHERE id < " + count + " ORDER BY id DESC";
        var nativeEnv = environment(true, count, new Configuration());
        assertEquals(
            collect(environment(false, count, new Configuration()).executeSql(sql)),
            collect(nativeEnv.executeSql(sql)));
      }
    } finally {
      tech.streamfusion.compat.FlinkTestContexts.clearForRemoteSubmission();
    }
  }

  @Test
  void nullableScalarPayloadUsesOwnedArrowBuffers() throws Exception {
    try (var metrics = new RecoveryExecutionMetrics()) {
      metrics.setAsContext(new Configuration(), 2, 128L << 20);
      var nativeEnv = environment(true, 3, new Configuration());
      var stock = environment(false, 3, new Configuration());
      for (String keyType : List.of("TINYINT", "SMALLINT", "INTEGER", "BIGINT")) {
        String sql =
            "SELECT * FROM (VALUES "
                + "(CAST(2 AS "
                + keyType
                + "), 'λ-payload', CAST(1.25 AS DECIMAL(10,2))),"
                + "(CAST(1 AS "
                + keyType
                + "), CAST(NULL AS STRING), CAST(NULL AS DECIMAL(10,2))),"
                + "(CAST(3 AS "
                + keyType
                + "), 'last', CAST(-3.50 AS DECIMAL(10,2)))) AS t(k, s, d)"
                + " ORDER BY k DESC";
        assertTrue(nativeEnv.explainSql(sql).contains("NativeSort"), keyType);
        var result = nativeEnv.executeSql(sql);
        String job = result.getJobClient().orElseThrow().getJobID().toString();
        assertEquals(collect(stock.executeSql(sql)), collect(result), keyType);
        assertEquals(3, metrics.recordsIn(job, "NativeSort"), keyType);
      }
    } finally {
      tech.streamfusion.compat.FlinkTestContexts.clearForRemoteSubmission();
    }
  }

  @Test
  void unsupportedKeysAndDisabledSortRetainTheWholeStockQuery() throws Exception {
    var env = environment(true, 3, new Configuration());
    String unsupported =
        env.explainSql(
            "SELECT SHA2(CAST(id AS STRING), 256) FROM input ORDER BY SIN(CAST(id AS DOUBLE))");
    assertFalse(
        unsupported.contains("NativeSort") || unsupported.contains("NativeCalc"), unsupported);
    env.getConfig().set("streamfusion.operator.sort.enabled", "false");
    assertFalse(env.explainSql("SELECT id FROM input ORDER BY id").contains("NativeSort"));
  }

  static TableEnvironment environment(boolean nativeRun, int rows, Configuration config)
      throws Exception {
    var env = BatchSqlTest.environment(nativeRun, rows, config);
    env.getConfig().set("parallelism.default", "2");
    env.getConfig().set("table.optimizer.range-sort.enabled", "false");
    return env;
  }

  static List<String> collect(org.apache.flink.table.api.TableResult result) throws Exception {
    List<String> rows = new ArrayList<>();
    try (var iterator = result.collect()) {
      while (iterator.hasNext()) rows.add(iterator.next().toString());
    }
    return rows;
  }
}
