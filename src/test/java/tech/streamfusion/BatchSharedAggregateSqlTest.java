package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import org.apache.flink.configuration.Configuration;
import org.junit.jupiter.api.Test;

class BatchSharedAggregateSqlTest {
  @Test
  void retainedAggregateViewsRespectAllReuseSwitches() throws Exception {
    String sql =
        "WITH x AS (SELECT MOD(id,7) k,SUM(CAST(id AS DECIMAL(15,2))) v"
            + " FROM input GROUP BY MOD(id,7))"
            + " SELECT x.k,x.v FROM x WHERE x.v=(SELECT MAX(v) FROM x) ORDER BY x.k";
    try (var metrics = new RecoveryExecutionMetrics()) {
      metrics.setAsContext(new Configuration(), 2, 128L << 20);
      var stock = BatchSortSqlTest.environment(false, 259, new Configuration());
      var expected = BatchSortSqlTest.collect(stock.executeSql(sql));
      for (String disabled :
          List.of(
              "",
              "streamfusion.plan.shareSources",
              "table.optimizer.reuse-sub-plan-enabled",
              "table.optimizer.reuse-source-enabled")) {
        var config = new Configuration();
        if (!disabled.isEmpty()) config.setString(disabled, "false");
        var env = BatchSortSqlTest.environment(true, 259, config);
        String plan = env.explainSql(sql);
        assertEquals(disabled.isEmpty(), plan.contains("NativeShare"), plan);
        var result = env.executeSql(sql);
        String job = result.getJobClient().orElseThrow().getJobID().toString();
        assertEquals(expected, BatchSortSqlTest.collect(result));
        assertTrue(metrics.recordsIn(job, "NativeAggregate") > 0);
        if (disabled.isEmpty()) assertTrue(metrics.recordsIn(job, "NativeShare") > 0);
      }
    } finally {
      tech.streamfusion.compat.FlinkTestContexts.clearForRemoteSubmission();
    }
  }
}
