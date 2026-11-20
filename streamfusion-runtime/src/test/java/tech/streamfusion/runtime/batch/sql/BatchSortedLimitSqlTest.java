package tech.streamfusion.runtime.batch.sql;

import tech.streamfusion.RecoveryExecutionMetrics;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import org.apache.flink.configuration.Configuration;
import org.junit.jupiter.api.Test;

class BatchSortedLimitSqlTest {
  @Test
  void localAndGlobalLimitsRetainOffsetNullsAndMultipleBatches() throws Exception {
    Configuration config = new Configuration();
    config.setString("streamfusion.transpose.batchRows", "1024");
    config.setString("pipeline.operator-chaining.enabled", "false");
    try (var metrics = new RecoveryExecutionMetrics()) {
      metrics.setAsContext(config, 2, 128L << 20);
      var nativeEnv = BatchSqlTestSupport.environment(true, 4099, config);
      var stock = BatchSqlTestSupport.environment(false, 4099, config);
      for (String suffix :
          List.of(
              "LIMIT 1",
              "LIMIT 2053",
              "OFFSET 1023 ROWS FETCH NEXT 2053 ROWS ONLY",
              "OFFSET 4100 ROWS FETCH NEXT 3 ROWS ONLY")) {
        String sql =
            "SELECT CASE WHEN MOD(id, 17) = 0 THEN CAST(NULL AS INT)"
                + " ELSE CAST(MOD(id, 71) AS INT) END AS k, id FROM input"
                + " ORDER BY k DESC NULLS FIRST, id ASC "
                + suffix;
        String plan = nativeEnv.explainSql(sql);
        assertTrue(
            plan.contains("NativeSort")
                && plan.contains("fetch=[")
                && plan.contains("global=[false]"),
            plan);
        assertTrue(plan.contains("NativeExchange"), plan);
        var result = nativeEnv.executeSql(sql);
        String job = result.getJobClient().orElseThrow().getJobID().toString();
        assertEquals(
            BatchSqlTestSupport.collect(stock.executeSql(sql)),
            BatchSqlTestSupport.collect(result),
            suffix);
        assertTrue(metrics.recordsIn(job, "NativeSort") >= 4099, suffix);
        assertTrue(
            metrics.taskNames("NativeSort").stream()
                .allMatch(name -> name.startsWith("NativeSort")));
      }
    } finally {
      tech.streamfusion.compat.FlinkTestContexts.clearForRemoteSubmission();
    }
  }

  @Test
  void disabledSortKeepsTheWholeQueryOnStock() throws Exception {
    var env = BatchSqlTestSupport.environment(true, 3, new Configuration());
    env.getConfig().set("streamfusion.operator.sort.enabled", "false");
    String plan =
        tech.streamfusion.planner.NativePlanner.explain(
            env, "SELECT id + 1 FROM input ORDER BY id LIMIT 2");
    assertFalse(plan.contains("NativeSort"), plan);
    assertFalse(plan.contains("NativeCalc"), plan);
    assertTrue(plan.contains("batch sort: disabled"), plan);
  }

  @Test
  void emptyInputAndZeroLimitComplete() throws Exception {
    try (var metrics = new RecoveryExecutionMetrics()) {
      metrics.setAsContext(new Configuration(), 2, 128L << 20);
      var nativeEnv = BatchSqlTestSupport.environment(true, 3, new Configuration());
      var stock = BatchSqlTestSupport.environment(false, 3, new Configuration());
      for (String sql :
          List.of(
              "SELECT id FROM input WHERE id < 0 ORDER BY id LIMIT 3",
              "SELECT id FROM input ORDER BY id LIMIT 0")) {
        assertEquals(
            BatchSqlTestSupport.collect(stock.executeSql(sql)),
            BatchSqlTestSupport.collect(nativeEnv.executeSql(sql)));
      }
    } finally {
      tech.streamfusion.compat.FlinkTestContexts.clearForRemoteSubmission();
    }
  }
}
