package tech.streamfusion.runtime.batch.sql;

import tech.streamfusion.RecoveryExecutionMetrics;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import org.apache.flink.configuration.Configuration;
import org.junit.jupiter.api.Test;

class BatchShortCircuitSqlTest {
  @Test
  void fallibleBooleanBranchesKeepFlinkEvaluationInsideArrowCalculations() throws Exception {
    try (var metrics = new RecoveryExecutionMetrics()) {
      metrics.setAsContext(new Configuration(), 2, 128L << 20);
      var stock = BatchSqlTestSupport.environment(false, 2051, new Configuration());
      var nativeEnv = BatchSqlTestSupport.environment(true, 2051, new Configuration());
      for (String predicate :
          List.of(
              "id = 0 OR 10 / id > 0",
              "id <> 0 AND 10 / id > 0",
              "(id = 0 OR 10 / id > 0) AND (id < 100 OR 100 / (id - 99) > 0)")) {
        String sql = "SELECT id FROM input WHERE " + predicate + " ORDER BY id";
        String plan = nativeEnv.explainSql(sql);
        assertTrue(plan.contains("NativeCalc") && plan.contains("NativeSort"), plan);
        var result = nativeEnv.executeSql(sql);
        String job = result.getJobClient().orElseThrow().getJobID().toString();
        assertEquals(
            BatchSqlTestSupport.collect(stock.executeSql(sql)), BatchSqlTestSupport.collect(result));
        assertTrue(metrics.recordsIn(job, "NativeCalc") > 0);
      }
      String failed = "SELECT id FROM input WHERE id = 0 AND 10 / id > 0 ORDER BY id";
      assertThrows(Exception.class, () -> BatchSqlTestSupport.collect(stock.executeSql(failed)));
      assertThrows(Exception.class, () -> BatchSqlTestSupport.collect(nativeEnv.executeSql(failed)));
    } finally {
      tech.streamfusion.compat.FlinkTestContexts.clearForRemoteSubmission();
    }
  }
}
