package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.List;
import org.apache.flink.configuration.Configuration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

@EnabledIfEnvironmentVariable(named = "SF_BENCHMARK", matches = "true")
class BatchGroupedAggregateJobBenchmark {
  @Test
  void compareCompleteGroupedJobs() throws Exception {
    assertEquals("false", System.getProperty("streamfusion.exchange.zeroCopyLocal"), "Use -Pbench");
    boolean sha2 = Boolean.parseBoolean(System.getProperty("sf.batch.grouped.sha2", "true"));
    boolean distinct = Boolean.getBoolean("sf.batch.grouped.distinct");
    int rows = Integer.getInteger("sf.batch.grouped.rows", 1_000_000);
    int cardinality = Integer.getInteger("sf.batch.grouped.cardinality", 64);
    int warmups = Integer.getInteger("sf.batch.grouped.warmups", 3);
    int trials = Integer.getInteger("sf.batch.grouped.trials", 7);
    Configuration config = new Configuration();
    config.setString("execution.batch.adaptive.auto-parallelism.enabled", "false");
    config.setString("table.exec.resource.default-parallelism", "2");
    config.setString("taskmanager.memory.managed.size", "1024m");
    config.setString("table.optimizer.agg-phase-strategy", "TWO_PHASE");
    try (var metrics = new RecoveryExecutionMetrics()) {
      metrics.setAsContext(config, 2, 128L << 20);
      var stock = BatchSortSqlTest.environment(false, rows, config);
      var nativeEnv = BatchSortSqlTest.environment(true, rows, config);
      for (var env : List.of(stock, nativeEnv))
        env.executeSql(
            distinct
                ? "CREATE TABLE output(k INT) WITH ('connector'='blackhole')"
                : "CREATE TABLE output(k INT,total BIGINT,n BIGINT,minimum BIGINT,maximum"
                    + " BIGINT,average_value BIGINT) WITH ('connector'='blackhole')");
      String statement =
          (distinct
                  ? "INSERT INTO output SELECT DISTINCT k"
                  : "INSERT INTO output SELECT k,SUM(v),COUNT(v),MIN(v),MAX(v),AVG(v)")
              + " FROM (SELECT CASE WHEN MOD(id,17)=0 THEN CAST(NULL AS INT) ELSE"
              + " CAST(MOD(id*48271,"
              + cardinality
              + ") AS INT) END AS k,CASE WHEN MOD(id,13)=0 THEN CAST(NULL AS BIGINT)"
              + " ELSE id END AS v FROM input"
              + (sha2 ? " WHERE SHA2(CAST(id AS STRING),256) <> ''" : "")
              + ") t"
              + (distinct ? "" : " GROUP BY k");
      String plan = nativeEnv.explainSql(statement);
      assertTrue(
          plan.contains("NativeAggregate")
              && plan.contains("NativeExchange")
              && plan.contains("RowDataToArrow")
              && plan.contains("ArrowToRowData"),
          plan);
      assertFalse(stock.explainSql(statement).contains("NativeAggregate"));
      List<Long> stockNanos = new ArrayList<>(), nativeNanos = new ArrayList<>();
      List<Long> stockRuntimeMs = new ArrayList<>(), nativeRuntimeMs = new ArrayList<>();
      for (int trial = -warmups; trial < trials; trial++) {
        for (boolean nativeRun :
            trial % 2 == 0 ? new boolean[] {false, true} : new boolean[] {true, false}) {
          long start = System.nanoTime();
          var result = (nativeRun ? nativeEnv : stock).executeSql(statement);
          result.await();
          var client = result.getJobClient().orElseThrow();
          long runtime = client.getJobExecutionResult().get().getNetRuntime();
          long nanos = System.nanoTime() - start;
          long input = metrics.recordsIn(client.getJobID().toString(), "NativeAggregate");
          if (nativeRun) assertTrue(input >= rows, "No native grouped input");
          else assertEquals(0, input);
          if (trial >= 0) {
            (nativeRun ? nativeNanos : stockNanos).add(nanos);
            (nativeRun ? nativeRuntimeMs : stockRuntimeMs).add(runtime);
          }
        }
      }
      System.out.printf(
          "[batch-grouped-job] rows=%d cardinality=%d sha2=%s distinct=%s parallelism=2 capBytes=%d"
              + " warmups=%d trials=%d stockNanos=%s nativeNanos=%s stockRuntimeMs=%s"
              + " nativeRuntimeMs=%s%n",
          rows,
          cardinality,
          sha2,
          distinct,
          128L << 20,
          warmups,
          trials,
          stockNanos,
          nativeNanos,
          stockRuntimeMs,
          nativeRuntimeMs);
    } finally {
      tech.streamfusion.compat.FlinkTestContexts.clearForRemoteSubmission();
    }
  }
}
