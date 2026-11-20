package tech.streamfusion.runtime.batch.sql;

import tech.streamfusion.RecoveryExecutionMetrics;

import static org.junit.jupiter.api.Assertions.*;

import org.apache.flink.configuration.Configuration;
import org.junit.jupiter.api.Test;

class BatchLimitSqlTest {
  @Test
  void unorderedLimitsComposeWithAggregationAndConsumeMultipleBatches() throws Exception {
    var config = new Configuration();
    config.setString("streamfusion.transpose.batchRows", "1024");
    config.setString("pipeline.operator-chaining.enabled", "false");
    try (var metrics = new RecoveryExecutionMetrics()) {
      metrics.setAsContext(config, 2, 128L << 20);
      var nativeEnv = BatchSqlTestSupport.environment(true, 4099, config);
      var stock = BatchSqlTestSupport.environment(false, 4099, config);
      for (int count : new int[] {1, 2053, 5000}) {
        // Constant rows make an unordered LIMIT deterministic across independent source scheduling.
        String sql =
            "SELECT COUNT(*), MIN(k), MAX(k) FROM (SELECT MOD(id,1) k FROM input WHERE SHA2(CAST(id"
                + " AS STRING),256) <> '' LIMIT "
                + count
                + ")";
        String plan = nativeEnv.explainSql(sql);
        assertTrue(plan.contains("NativeLimit"), plan);
        var result = nativeEnv.executeSql(sql);
        String job = result.getJobClient().orElseThrow().getJobID().toString();
        assertEquals(
            BatchSqlTestSupport.collect(stock.executeSql(sql)), BatchSqlTestSupport.collect(result));
        assertTrue(metrics.recordsIn(job, "NativeLimit") > 0);
      }
    } finally {
      tech.streamfusion.compat.FlinkTestContexts.clearForRemoteSubmission();
    }
  }
}
