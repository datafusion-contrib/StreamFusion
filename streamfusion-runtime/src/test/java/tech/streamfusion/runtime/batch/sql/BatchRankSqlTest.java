package tech.streamfusion.runtime.batch.sql;

import tech.streamfusion.RecoveryExecutionMetrics;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.List;
import org.apache.flink.configuration.Configuration;
import org.junit.jupiter.api.Test;

class BatchRankSqlTest {
  @Test
  void boundedRankRangesPreservePeersAndOptionalRankColumn() throws Exception {
    try (var metrics = new RecoveryExecutionMetrics()) {
      metrics.setAsContext(new Configuration(), 2, 128L << 20);
      var stock = BatchSqlTestSupport.environment(false, 2051, new Configuration());
      var nativeEnv = BatchSqlTestSupport.environment(true, 2051, new Configuration());
      for (String projection : List.of("id", "id, rk")) {
        String sql =
            "SELECT "
                + projection
                + " FROM (SELECT id, RANK() OVER "
                + "(PARTITION BY MOD(id,7) ORDER BY MOD(id,5) DESC) rk FROM input) WHERE rk <= 3";
        String plan = nativeEnv.explainSql(sql);
        assertTrue(plan.contains("NativeWindow") && plan.contains("rankEnd=3"), plan);
        var result = nativeEnv.executeSql(sql);
        String job = result.getJobClient().orElseThrow().getJobID().toString();
        var actual = new ArrayList<>(BatchSqlTestSupport.collect(result));
        var expected = new ArrayList<>(BatchSqlTestSupport.collect(stock.executeSql(sql)));
        actual.sort(String::compareTo);
        expected.sort(String::compareTo);
        assertEquals(expected, actual);
        assertTrue(metrics.recordsIn(job, "NativeWindow") > 0);
      }
    } finally {
      tech.streamfusion.compat.FlinkTestContexts.clearForRemoteSubmission();
    }
  }
}
