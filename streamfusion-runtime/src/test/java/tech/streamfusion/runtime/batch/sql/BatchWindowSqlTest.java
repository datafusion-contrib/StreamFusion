package tech.streamfusion.runtime.batch.sql;

import tech.streamfusion.RecoveryExecutionMetrics;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.List;
import org.apache.flink.configuration.Configuration;
import org.junit.jupiter.api.Test;

class BatchWindowSqlTest {
  @Test
  void partitionKeysAlsoUsedForOrderingKeepTheirDirectionAndNullPlacement() throws Exception {
    var config = new Configuration();
    config.setString("execution.batch.adaptive.auto-parallelism.enabled", "false");
    config.setString("table.exec.resource.default-parallelism", "1");
    config.setString("table.optimizer.range-sort.enabled", "false");
    try (var metrics = RecoveryExecutionMetrics.countersOnly()) {
      metrics.setAsContext(config, 1, 128L << 20);
      var stock = BatchSqlTest.environment(false, 33, config);
      var nativeEnv = BatchSqlTest.environment(true, 33, config);
      for (String ordering :
          List.of("ASC NULLS FIRST", "ASC NULLS LAST", "DESC NULLS FIRST", "DESC NULLS LAST")) {
        String sql =
            "SELECT k, COUNT(*) OVER (PARTITION BY k ORDER BY k "
                + ordering
                + " RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) n FROM (SELECT CASE WHEN"
                + " MOD(id,5)=0 THEN CAST(NULL AS BIGINT) ELSE MOD(id,3) END k FROM input WHERE"
                + " CHAR_LENGTH(SHA2(CAST(id AS STRING),256))=64) ORDER BY k "
                + ordering;
        String plan = nativeEnv.explainSql(sql);
        assertTrue(plan.contains("NativeWindow"), plan);
        var result = nativeEnv.executeSql(sql);
        String job = result.getJobClient().orElseThrow().getJobID().toString();
        assertEquals(
            BatchSqlTestSupport.collect(stock.executeSql(sql)),
            BatchSqlTestSupport.collect(result),
            ordering);
        assertTrue(metrics.recordsIn(job, "NativeWindow") > 0);
      }
    } finally {
      tech.streamfusion.compat.FlinkTestContexts.clearForRemoteSubmission();
    }
  }

  @Test
  void partitionedFramesAndPeerRanksMatchStockAcrossBatchesAndShuffles() throws Exception {
    var config = new Configuration();
    config.setString("streamfusion.transpose.batchRows", "1024");
    config.setString("pipeline.operator-chaining.enabled", "false");
    config.setString("execution.batch.adaptive.auto-parallelism.enabled", "false");
    config.setString("table.exec.resource.default-parallelism", "2");
    try (var metrics = new RecoveryExecutionMetrics()) {
      metrics.setAsContext(config, 2, 128L << 20);
      int rows = 4099;
      var stock = BatchSqlTestSupport.environment(false, rows, config);
      var nativeEnv = BatchSqlTestSupport.environment(true, rows, config);
      for (String expression :
          List.of(
              "SUM(v) OVER (PARTITION BY k)",
              "SUM(v) OVER (PARTITION BY k ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT"
                  + " ROW)",
              "MAX(v) OVER (PARTITION BY k ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT"
                  + " ROW)",
              "COUNT(v) OVER (PARTITION BY k)",
              "RANK() OVER (PARTITION BY k ORDER BY MOD(id,5) DESC NULLS LAST)",
              "DENSE_RANK() OVER (PARTITION BY k ORDER BY MOD(id,5) ASC NULLS FIRST)",
              "ROW_NUMBER() OVER (ORDER BY id DESC)")) {
        for (String filter : List.of("", " WHERE id < 0")) {
          String sql =
              "SELECT id, "
                  + expression
                  + " AS w FROM (SELECT id, CASE WHEN MOD(id,17)=0 THEN CAST(NULL AS INT) ELSE"
                  + " CAST(MOD(id,7) AS INT) END k, CASE WHEN MOD(id,11)=0 THEN CAST(NULL AS"
                  + " DECIMAL(15,2)) ELSE CAST(id AS DECIMAL(15,2)) END v FROM input"
                  + filter
                  + ")";
          String plan = nativeEnv.explainSql(sql);
          assertTrue(
              plan.contains("NativeWindow"),
              tech.streamfusion.planner.NativePlanner.explain(nativeEnv, sql));
          var result = nativeEnv.executeSql(sql);
          String job = result.getJobClient().orElseThrow().getJobID().toString();
          var actual = new ArrayList<>(BatchSqlTestSupport.collect(result));
          var expected = new ArrayList<>(BatchSqlTestSupport.collect(stock.executeSql(sql)));
          actual.sort(String::compareTo);
          expected.sort(String::compareTo);
          assertEquals(expected.size(), actual.size(), expression + filter);
          for (int index = 0; index < expected.size(); index++)
            assertEquals(
                expected.get(index), actual.get(index), expression + filter + " row=" + index);
          if (filter.isEmpty()) assertTrue(metrics.recordsIn(job, "NativeWindow") > 0);
        }
      }
    } finally {
      tech.streamfusion.compat.FlinkTestContexts.clearForRemoteSubmission();
    }
  }

  @Test
  void extremaKeepNarrowIntegersBooleanDatesAndAllNullPartitions() throws Exception {
    try (var metrics = new RecoveryExecutionMetrics()) {
      metrics.setAsContext(new Configuration(), 2, 128L << 20);
      var stock = BatchSqlTestSupport.environment(false, 2051, new Configuration());
      var nativeEnv = BatchSqlTestSupport.environment(true, 2051, new Configuration());
      String sql =
          "SELECT id, MIN(v) OVER (PARTITION BY k), MAX(v) OVER (PARTITION BY k), MIN(b) OVER"
              + " (PARTITION BY k), MAX(b) OVER (PARTITION BY k), MIN(d) OVER (PARTITION BY k),"
              + " MAX(d) OVER (PARTITION BY k) FROM (SELECT id, MOD(id,7) k, CASE WHEN MOD(id,7)=0"
              + " THEN CAST(NULL AS TINYINT) ELSE CAST(MOD(id,97)-48 AS TINYINT) END v, CASE WHEN"
              + " MOD(id,7)=0 THEN CAST(NULL AS BOOLEAN) ELSE MOD(id,2)=0 END b, CASE WHEN"
              + " MOD(id,7)=0 THEN CAST(NULL AS DATE) WHEN MOD(id,2)=0 THEN DATE '1969-12-31' ELSE"
              + " DATE '2000-02-29' END d FROM input)";
      String plan = nativeEnv.explainSql(sql);
      assertTrue(plan.contains("NativeWindow"), plan);
      var result = nativeEnv.executeSql(sql);
      String job = result.getJobClient().orElseThrow().getJobID().toString();
      var actual = new ArrayList<>(BatchSqlTestSupport.collect(result));
      var expected = new ArrayList<>(BatchSqlTestSupport.collect(stock.executeSql(sql)));
      actual.sort(String::compareTo);
      expected.sort(String::compareTo);
      assertEquals(expected, actual);
      assertTrue(metrics.recordsIn(job, "NativeWindow") > 0);
    } finally {
      tech.streamfusion.compat.FlinkTestContexts.clearForRemoteSubmission();
    }
  }
}
