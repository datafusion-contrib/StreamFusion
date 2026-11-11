package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.*;

import org.apache.flink.configuration.Configuration;
import org.junit.jupiter.api.Test;

class BatchCrossJoinSqlTest {
  @Test
  void linkedDimensionsAvoidCartesianMaterializationAndKeepDuplicateNullSemantics()
      throws Exception {
    Configuration config = new Configuration();
    config.setString("table.optimizer.join-reorder-enabled", "false");
    config.setString("table.optimizer.join.broadcast-threshold", "-1");
    config.setString("execution.batch.adaptive.auto-parallelism.enabled", "false");
    config.setString("table.exec.resource.default-parallelism", "2");
    try (var metrics = new RecoveryExecutionMetrics()) {
      metrics.setAsContext(config, 2, 128L << 20);
      var stock = BatchSortSqlTest.environment(false, 31, config);
      var nativeEnv = BatchSortSqlTest.environment(true, 31, config);
      String sql =
          "SELECT a.k,b.k,SUM(CAST(c.v AS DECIMAL(15,2))),COUNT(*) FROM (SELECT CASE WHEN"
              + " MOD(id,7)=0 THEN CAST(NULL AS BIGINT) ELSE MOD(id,3) END AS k FROM input) a CROSS"
              + " JOIN (SELECT MOD(id,5) AS k FROM input) b JOIN (SELECT MOD(id,3) AS p,MOD(id,5)"
              + " AS s,id AS v FROM input) c ON a.k=c.p AND b.k=c.s GROUP BY a.k,b.k ORDER BY"
              + " a.k,b.k";
      String baseline = stock.explainSql(sql);
      assertTrue(baseline.contains("where=[true]"), baseline);
      String plan = nativeEnv.explainSql(sql);
      assertTrue(plan.contains("NativeJoin"), plan);
      assertFalse(plan.contains("where=[true]"), plan);
      var result = nativeEnv.executeSql(sql);
      String job = result.getJobClient().orElseThrow().getJobID().toString();
      assertEquals(
          BatchSortSqlTest.collect(stock.executeSql(sql)), BatchSortSqlTest.collect(result));
      assertTrue(metrics.recordsIn(job, "NativeJoin") > 0);
    } finally {
      tech.streamfusion.compat.FlinkTestContexts.clearForRemoteSubmission();
    }
  }
}
