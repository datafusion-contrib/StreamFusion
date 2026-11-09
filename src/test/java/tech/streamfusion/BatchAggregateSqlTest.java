package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import org.apache.flink.configuration.Configuration;
import org.junit.jupiter.api.Test;

class BatchAggregateSqlTest {
  @Test
  void globalIntegerAggregatesPreserveWidthsFiltersAndBothPhases() throws Exception {
    try (var metrics = new RecoveryExecutionMetrics()) {
      metrics.setAsContext(new Configuration(), 2, 128L << 20);
      for (String disabled : List.of("", "HashAgg")) {
        for (String phase : List.of("ONE_PHASE", "TWO_PHASE")) {
          Configuration config = new Configuration();
          config.setString("table.optimizer.agg-phase-strategy", phase);
          config.setString("table.exec.disabled-operators", disabled);
          config.setString("pipeline.operator-chaining.enabled", "false");
          var nativeEnv = BatchSortSqlTest.environment(true, 2051, config);
          var stock = BatchSortSqlTest.environment(false, 2051, config);
          String sql =
              "SELECT SUM(CAST(id AS TINYINT)), SUM(CAST(id AS SMALLINT)), SUM(CAST(id AS INT)),"
                  + " SUM(id), COUNT(*), COUNT(id), MIN(CAST(id AS INT)), MAX(id), AVG(id),"
                  + " AVG(CAST(id AS TINYINT)) FILTER (WHERE MOD(id,3) <> 0) FROM input"
                  + " WHERE SHA2(CAST(id AS STRING),256) <> ''";
          String plan = nativeEnv.explainSql(sql);
          assertTrue(plan.contains("NativeAggregate"), plan);
          var result = nativeEnv.executeSql(sql);
          String job = result.getJobClient().orElseThrow().getJobID().toString();
          assertEquals(
              BatchSortSqlTest.collect(stock.executeSql(sql)), BatchSortSqlTest.collect(result));
          long consumed = metrics.recordsIn(job, "NativeAggregate");
          if (phase.equals("ONE_PHASE")) assertEquals(2051, consumed);
          else
            assertTrue(
                consumed >= 2052 && consumed <= 2053,
                "each nonempty local partition contributes one final input: " + consumed);
          System.out.printf(
              "[batch-aggregate-sql] implementation=%s phase=%s nativeRows=%d%n",
              disabled.isEmpty() ? "Hash" : "Sort", phase, consumed);
        }
      }
    } finally {
      tech.streamfusion.compat.FlinkTestContexts.clearForRemoteSubmission();
    }
  }

  @Test
  void finalAggregateReplaysCompletedPartialStateAfterConsumerFailure() throws Exception {
    Configuration config = new Configuration();
    config.setString("restart-strategy.type", "fixed-delay");
    config.setString("restart-strategy.fixed-delay.attempts", "1");
    config.setString("restart-strategy.fixed-delay.delay", "1 ms");
    config.setString("streamfusion.native.enabled", "true");
    config.setString("parallelism.default", "2");
    config.setString("table.optimizer.agg-phase-strategy", "TWO_PHASE");
    BatchReplaySinkFactory.FAILED.set(false);
    try (var metrics = new RecoveryExecutionMetrics()) {
      metrics.setAsContext(config, 2, 128L << 20);
      var env = NativePlannerTestEnvironment.createBatch(config, BatchReplaySinkFactory.class);
      env.executeSql(
          "CREATE TABLE input(id BIGINT) WITH ('connector'='datagen',"
              + " 'number-of-rows'='1000', 'fields.id.kind'='sequence',"
              + " 'fields.id.start'='0', 'fields.id.end'='999')");
      env.executeSql("CREATE TABLE output(id BIGINT) WITH ('connector'='batch-replay')");
      String sql =
          "INSERT INTO output SELECT COUNT(*) FROM input"
              + " WHERE SHA2(CAST(id AS STRING),256) <> ''";
      String plan = env.explainSql(sql);
      assertTrue(plan.contains("NativeAggregate") && plan.contains("NativeExchange"), plan);
      var result = env.executeSql(sql);
      result.await();
      var completed = result.getJobClient().orElseThrow().getJobExecutionResult().get();
      assertEquals(List.of(1000L), completed.getAccumulatorResult(BatchReplaySinkFactory.RESULTS));
      assertTrue(BatchReplaySinkFactory.FAILED.get(), "consumer failure did not run");
      assertTrue(
          metrics.attempts("NativeAggregate").contains("1"), metrics.nativeOperators().toString());
      assertEquals(
          java.util.Set.of("0"),
          metrics.attempts("RowDataToArrow"),
          "completed partial-state producers must stay on their first attempt");
      System.out.printf(
          "[batch-aggregate-replay] producerAttempts=%s aggregateAttempts=%s rows=1 count=1000%n",
          metrics.attempts("RowDataToArrow"), metrics.attempts("NativeAggregate"));
    } finally {
      tech.streamfusion.compat.FlinkTestContexts.clearForRemoteSubmission();
    }
  }

  @Test
  void unsupportedAndDisabledAggregatesRetainTheWholeStockQuery() throws Exception {
    var env = BatchSortSqlTest.environment(true, 3, new Configuration());
    String predicate = " WHERE SHA2(CAST(id AS STRING),256) <> ''";
    for (String sql :
        List.of(
            "SELECT SUM(CAST(id AS DOUBLE)) FROM input" + predicate,
            "SELECT COUNT(DISTINCT id) FROM input" + predicate,
            "SELECT MOD(id,7),SUM(id) FROM input" + predicate + " GROUP BY MOD(id,7)")) {
      String plan = env.explainSql(sql);
      assertFalse(plan.contains("NativeAggregate") || plan.contains("NativeCalc"), plan);
    }
    String cheap =
        tech.streamfusion.planner.NativePlanner.explain(env, "SELECT SUM(id) FROM input");
    assertFalse(cheap.contains("NativeAggregate("), cheap);
    assertTrue(
        cheap.contains("batch aggregate: standalone pipeline has no verified speedup"), cheap);
    for (String sql :
        List.of(
            "SELECT SHA2(CAST(SUM(id) AS STRING),256) FROM input",
            "SELECT SUM(id) FROM input ORDER BY SUM(id)")) {
      String postAggregate = env.explainSql(sql);
      assertFalse(postAggregate.contains("NativeAggregate"), postAggregate);
    }
    env.createTemporarySystemFunction("after_aggregate", BatchSqlBenchmark.AddOne.class);
    String userFunction = env.explainSql("SELECT after_aggregate(SUM(id)) FROM input");
    assertTrue(userFunction.contains("NativeAggregate"), userFunction);
    env.getConfig().set("streamfusion.operator.groupAggregate.enabled", "false");
    String disabled = env.explainSql("SELECT COUNT(*) FROM input" + predicate);
    assertFalse(disabled.contains("NativeAggregate") || disabled.contains("NativeCalc"), disabled);
  }

  @Test
  void emptyAndAllNullInputsKeepFinalDefaults() throws Exception {
    try (var metrics = new RecoveryExecutionMetrics()) {
      metrics.setAsContext(new Configuration(), 2, 128L << 20);
      for (String phase : List.of("ONE_PHASE", "TWO_PHASE")) {
        Configuration config = new Configuration();
        config.setString("table.optimizer.agg-phase-strategy", phase);
        var nativeEnv = BatchSortSqlTest.environment(true, 3, config);
        var stock = BatchSortSqlTest.environment(false, 3, config);
        for (String sql :
            List.of(
                "SELECT SUM(id), COUNT(id), COUNT(*), MIN(id), MAX(id), AVG(id) FROM input WHERE id"
                    + " < 0 AND SHA2(CAST(id AS STRING),256) <> ''",
                "SELECT SUM(CAST(NULL AS INT)), COUNT(CAST(NULL AS INT)), AVG(CAST(NULL AS"
                    + " SMALLINT)), COUNT(*) FROM input WHERE SHA2(CAST(id AS STRING),256) <>"
                    + " ''")) {
          String plan = nativeEnv.explainSql(sql);
          assertTrue(plan.contains("NativeAggregate"), plan);
          assertEquals(
              BatchSortSqlTest.collect(stock.executeSql(sql)),
              BatchSortSqlTest.collect(nativeEnv.executeSql(sql)));
        }
      }
    } finally {
      tech.streamfusion.compat.FlinkTestContexts.clearForRemoteSubmission();
    }
  }
}
