package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.List;
import org.apache.flink.configuration.Configuration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

/**
 * Complete global-aggregate jobs retain the row/Arrow perimeter and blocking partial-state shuffle.
 */
@EnabledIfEnvironmentVariable(named = "SF_BENCHMARK", matches = "true")
class BatchAggregateJobBenchmark {
  @Test
  void compareCompleteAggregateJobs() throws Exception {
    assertEquals("false", System.getProperty("streamfusion.exchange.zeroCopyLocal"), "Use -Pbench");
    int rows = Integer.getInteger("sf.batch.aggregate.rows", 1_000_000);
    int cardinality = Integer.getInteger("sf.batch.aggregate.cardinality", 2147483647);
    String type = System.getProperty("sf.batch.aggregate.type", "BIGINT");
    int warmups = Integer.getInteger("sf.batch.aggregate.warmups", 2);
    int trials = Integer.getInteger("sf.batch.aggregate.trials", 5);
    boolean sha2 = Boolean.parseBoolean(System.getProperty("sf.batch.aggregate.sha2", "true"));
    int batchRows = Integer.getInteger("sf.batch.aggregate.batchRows", 1024);
    Configuration config = new Configuration();
    config.setString("streamfusion.transpose.batchRows", Integer.toString(batchRows));
    config.setString("taskmanager.memory.managed.size", "1024m");
    config.setString("table.optimizer.agg-phase-strategy", "TWO_PHASE");
    try (var metrics = new RecoveryExecutionMetrics()) {
      metrics.setAsContext(config, 2, 128L << 20);
      var stock = BatchSortSqlTest.environment(false, rows, config);
      var nativeEnv = BatchSortSqlTest.environment(true, rows, config);
      for (var env : List.of(stock, nativeEnv))
        env.executeSql(
            "CREATE TABLE output(total "
                + type
                + ", nonnull BIGINT, row_count BIGINT, minimum "
                + type
                + ", maximum "
                + type
                + ", average_value "
                + type
                + ") WITH ('connector'='blackhole')");
      String statement =
          "INSERT INTO output SELECT SUM(v), COUNT(v), COUNT(*), MIN(v), MAX(v), AVG(v)"
              + " FROM (SELECT CASE WHEN MOD(id,17)=0 THEN CAST(NULL AS "
              + type
              + ") ELSE CAST(MOD(id * 48271, "
              + cardinality
              + ") AS "
              + type
              + ") END AS v FROM input"
              + (sha2 ? " WHERE SHA2(CAST(id AS STRING),256) <> ''" : "")
              + ") AS values_input";
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
          var env = nativeRun ? nativeEnv : stock;
          long start = System.nanoTime();
          var result = env.executeSql(statement);
          result.await();
          var client = result.getJobClient().orElseThrow();
          long runtime = client.getJobExecutionResult().get().getNetRuntime();
          long nanos = System.nanoTime() - start;
          assertEquals(
              nativeRun ? rows + 2 : 0,
              metrics.recordsIn(client.getJobID().toString(), "NativeAggregate"));
          if (trial >= 0) {
            (nativeRun ? nativeNanos : stockNanos).add(nanos);
            (nativeRun ? nativeRuntimeMs : stockRuntimeMs).add(runtime);
          }
        }
      }
      System.out.printf(
          "[batch-aggregate-job] rows=%d cardinality=%d type=%s sha2=%s batchRows=%d parallelism=2"
              + " capBytes=%d warmups=%d trials=%d stockNanos=%s nativeNanos=%s stockRuntimeMs=%s"
              + " nativeRuntimeMs=%s%n",
          rows,
          cardinality,
          type,
          sha2,
          batchRows,
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
