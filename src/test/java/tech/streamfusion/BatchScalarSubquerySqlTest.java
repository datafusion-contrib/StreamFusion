package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import org.apache.flink.configuration.Configuration;
import org.junit.jupiter.api.Test;

class BatchScalarSubquerySqlTest {
  @Test
  void provenSingletonValuesPreserveNullWhileUnprovenInputsKeepTheHostError() throws Exception {
    try (var metrics = new RecoveryExecutionMetrics()) {
      metrics.setAsContext(new Configuration(), 2, 128L << 20);
      var stock = BatchSortSqlTest.environment(false, 9, new Configuration());
      var nativeEnv = BatchSortSqlTest.environment(true, 9, new Configuration());
      for (String filter : List.of("", " WHERE id<0")) {
        String sql =
            "SELECT (SELECT SUM(CAST(id AS DECIMAL(15,2))) * 0.0001 FROM input" + filter + ") AS v";
        String plan = nativeEnv.explainSql(sql);
        assertTrue(
            plan.contains("NativeAggregate"),
            tech.streamfusion.planner.NativePlanner.explain(nativeEnv, sql));
        var result = nativeEnv.executeSql(sql);
        String job = result.getJobClient().orElseThrow().getJobID().toString();
        assertEquals(
            BatchSortSqlTest.collect(stock.executeSql(sql)), BatchSortSqlTest.collect(result));
        assertTrue(metrics.recordsIn(job, "NativeAggregate") > 0);
      }
      String unproven = "SELECT (SELECT CAST(id AS DECIMAL(15,2)) FROM input) AS v";
      String plan = tech.streamfusion.planner.NativePlanner.explain(nativeEnv, unproven);
      assertTrue(plan.contains("SINGLE_VALUE requires a proven at-most-one-row input"), plan);
      var failure =
          assertThrows(
              Exception.class, () -> BatchSortSqlTest.collect(nativeEnv.executeSql(unproven)));
      Throwable cause = failure;
      while (cause.getCause() != null) cause = cause.getCause();
      assertTrue(
          cause.getMessage().contains("SingleValueAggFunction received more than one element"),
          cause.toString());
    } finally {
      tech.streamfusion.compat.FlinkTestContexts.clearForRemoteSubmission();
    }
  }
}
