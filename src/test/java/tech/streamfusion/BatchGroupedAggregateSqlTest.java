package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import org.apache.flink.configuration.Configuration;
import org.junit.jupiter.api.Test;

class BatchGroupedAggregateSqlTest {
  @Test
  void hashGroupsKeepHostStatesAcrossBlockingArrowPartitions() throws Exception {
    try (var metrics = new RecoveryExecutionMetrics()) {
      metrics.setAsContext(new Configuration(), 2, 128L << 20);
      for (String phase : List.of("ONE_PHASE", "TWO_PHASE")) {
        Configuration config = new Configuration();
        config.setString("execution.batch.adaptive.auto-parallelism.enabled", "false");
        config.setString("table.exec.resource.default-parallelism", "2");
        config.setString("table.optimizer.agg-phase-strategy", phase);
        config.setString("pipeline.operator-chaining.enabled", "false");
        var stock = BatchSortSqlTest.environment(false, 1037, config);
        var nativeEnv = BatchSortSqlTest.environment(true, 1037, config);
        String input =
            " FROM (SELECT CASE WHEN MOD(id,17)=0 THEN CAST(NULL AS INT)"
                + " ELSE CAST(MOD(id,37) AS INT) END AS k,"
                + " CASE WHEN MOD(id,13)=0 THEN CAST(NULL AS SMALLINT)"
                + " ELSE CAST(id AS SMALLINT) END AS v FROM input"
                + " WHERE SHA2(CAST(id AS STRING),256) <> ''%s) AS t";
        for (String predicate : List.of("", " AND MOD(id,13)=0", " AND id<0")) {
          String sql =
              "SELECT k,SUM(v),COUNT(v),COUNT(*),MIN(v),MAX(v),AVG(v),"
                  + "SUM(v) FILTER(WHERE v>1000),COUNT(v) FILTER(WHERE v<0)"
                  + String.format(input, predicate)
                  + " GROUP BY k";
          String plan = nativeEnv.explainSql(sql);
          assertTrue(plan.contains("NativeAggregate") && plan.contains("NativeExchange"), plan);
          var result = nativeEnv.executeSql(sql);
          String job = result.getJobClient().orElseThrow().getJobID().toString();
          var actual = BatchSortSqlTest.collect(result);
          var expected = BatchSortSqlTest.collect(stock.executeSql(sql));
          actual.sort(String::compareTo);
          expected.sort(String::compareTo);
          assertEquals(expected, actual, phase + predicate);
          if (predicate.isEmpty()) {
            assertTrue(metrics.recordsIn(job, "NativeAggregate") >= 1037);
            assertEquals(java.util.Set.of("0", "1"), metrics.subtasks("NativeAggregate"));
          }
        }
      }
    } finally {
      tech.streamfusion.compat.FlinkTestContexts.clearForRemoteSubmission();
    }
  }

  @Test
  void distinctAggregateRewritesReuseTheVerifiedDeduplicationStages() throws Exception {
    try (var metrics = new RecoveryExecutionMetrics()) {
      metrics.setAsContext(new Configuration(), 2, 128L << 20);
      var nativeEnv = BatchSortSqlTest.environment(true, 1037, new Configuration());
      var stock = BatchSortSqlTest.environment(false, 1037, new Configuration());
      for (String function : List.of("COUNT", "SUM", "AVG")) {
        String sql =
            "SELECT "
                + function
                + "(DISTINCT k) FROM (SELECT CASE WHEN MOD(id,17)=0"
                + " THEN CAST(NULL AS BIGINT) ELSE MOD(id,37) END AS k FROM input) t";
        String plan = nativeEnv.explainSql(sql);
        assertTrue(plan.contains("NativeAggregate") && plan.contains("NativeExchange"), plan);
        var result = nativeEnv.executeSql(sql);
        String job = result.getJobClient().orElseThrow().getJobID().toString();
        assertEquals(
            BatchSortSqlTest.collect(stock.executeSql(sql)), BatchSortSqlTest.collect(result));
        assertTrue(metrics.recordsIn(job, "NativeAggregate") >= 1037);
      }
    } finally {
      tech.streamfusion.compat.FlinkTestContexts.clearForRemoteSubmission();
    }
  }

  @Test
  void completedHashPartitionsReplayTypedStatesAfterConsumerFailure() throws Exception {
    Configuration config = new Configuration();
    config.setString("restart-strategy.type", "fixed-delay");
    config.setString("restart-strategy.fixed-delay.attempts", "1");
    config.setString("restart-strategy.fixed-delay.delay", "1 ms");
    config.setString("table.optimizer.agg-phase-strategy", "TWO_PHASE");
    config.setString("execution.batch.adaptive.auto-parallelism.enabled", "false");
    config.setString("table.exec.resource.default-parallelism", "2");
    BatchReplaySinkFactory.FAILED.set(false);
    try (var metrics = new RecoveryExecutionMetrics()) {
      metrics.setAsContext(config, 2, 128L << 20);
      var nativeEnv =
          BatchExtremaSqlTest.environment(true, 2000, config, 3, BatchReplaySinkFactory.class);
      var stock =
          BatchExtremaSqlTest.environment(false, 2000, config, 3, BatchReplaySinkFactory.class);
      for (var env : List.of(nativeEnv, stock))
        env.executeSql(
            "CREATE TABLE replay(n BIGINT,k INT,min_d DECIMAL(38,18),max_tm TIME(3))"
                + " WITH ('connector'='batch-replay')");
      String sql =
          "INSERT INTO replay SELECT COUNT(*),CAST(MOD(id,2) AS INT),MIN(d),MAX(tm)"
              + " FROM extrema GROUP BY CAST(MOD(id,2) AS INT)";
      String plan = nativeEnv.explainSql(sql);
      assertTrue(plan.contains("NativeAggregate") && plan.contains("NativeExchange"), plan);
      var result = nativeEnv.executeSql(sql);
      result.await();
      var actual = result.getJobClient().orElseThrow().getJobExecutionResult().get();
      assertTrue(BatchReplaySinkFactory.FAILED.get());
      assertEquals(
          List.of(1000L, 1000L), actual.getAccumulatorResult(BatchReplaySinkFactory.RESULTS));
      assertEquals(java.util.Set.of("0"), metrics.attempts("RowDataToArrow"));
      assertTrue(metrics.attempts("NativeAggregate").contains("1"));
      var baseline = stock.executeSql(sql);
      baseline.await();
      var expected = baseline.getJobClient().orElseThrow().getJobExecutionResult().get();
      List<String> actualFields = actual.getAccumulatorResult(BatchReplaySinkFactory.DETAILS);
      List<String> expectedFields = expected.getAccumulatorResult(BatchReplaySinkFactory.DETAILS);
      actualFields.sort(String::compareTo);
      expectedFields.sort(String::compareTo);
      assertEquals(expectedFields, actualFields);
      System.out.printf(
          "[batch-grouped-replay] producerAttempts=%s aggregateAttempts=%s fields=%s%n",
          metrics.attempts("RowDataToArrow"), metrics.attempts("NativeAggregate"), actualFields);
    } finally {
      tech.streamfusion.compat.FlinkTestContexts.clearForRemoteSubmission();
    }
  }

  @Test
  void distinctKeysAndCompositeGroupsMatchStock() throws Exception {
    try (var metrics = new RecoveryExecutionMetrics()) {
      metrics.setAsContext(new Configuration(), 2, 128L << 20);
      var nativeEnv = BatchSortSqlTest.environment(true, 1037, new Configuration());
      var stock = BatchSortSqlTest.environment(false, 1037, new Configuration());
      for (String projection : List.of("SELECT DISTINCT k,v", "SELECT k,v,COUNT(*)")) {
        String sql =
            projection
                + " FROM (SELECT CAST(MOD(id,3) AS TINYINT) AS k,"
                + "CASE WHEN MOD(id,5)=0 THEN CAST(NULL AS BIGINT) ELSE MOD(id,7) END AS v"
                + " FROM input WHERE SHA2(CAST(id AS STRING),256) <> '') t"
                + (projection.contains("DISTINCT") ? "" : " GROUP BY k,v");
        String plan = nativeEnv.explainSql(sql);
        assertTrue(plan.contains("NativeAggregate") && plan.contains("NativeExchange"), plan);
        var result = nativeEnv.executeSql(sql);
        String job = result.getJobClient().orElseThrow().getJobID().toString();
        var actual = BatchSortSqlTest.collect(result);
        var expected = BatchSortSqlTest.collect(stock.executeSql(sql));
        actual.sort(String::compareTo);
        expected.sort(String::compareTo);
        assertEquals(expected, actual);
        assertTrue(metrics.recordsIn(job, "NativeAggregate") >= 1037);
      }
    } finally {
      tech.streamfusion.compat.FlinkTestContexts.clearForRemoteSubmission();
    }
  }
}
