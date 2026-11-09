package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.List;
import org.apache.flink.configuration.Configuration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

/** Complete sort jobs retain datagen, both transposes, blocking shuffle, JNI and a rowwise sink. */
@EnabledIfEnvironmentVariable(named = "SF_BENCHMARK", matches = "true")
class BatchSortJobBenchmark {
  @Test
  void compareStockAndNativeSortJobs() throws Exception {
    assertEquals("false", System.getProperty("streamfusion.exchange.zeroCopyLocal"), "Use -Pbench");
    int rows = Integer.getInteger("sf.batch.sort.job.rows", 1_000_000);
    int warmups = Integer.getInteger("sf.batch.sort.job.warmups", 2);
    int trials = Integer.getInteger("sf.batch.sort.job.trials", 5);
    int cardinality = Integer.getInteger("sf.batch.sort.job.cardinality", 2147483647);
    Configuration config = new Configuration();
    config.setString("taskmanager.memory.managed.size", "1024m");
    try (var metrics = new RecoveryExecutionMetrics()) {
      metrics.setAsContext(config, 2, 128L << 20);
      var stock = BatchSortSqlTest.environment(false, rows, config);
      var nativeEnv = BatchSortSqlTest.environment(true, rows, config);
      for (var env : List.of(stock, nativeEnv)) {
        env.getConfig().set("table.exec.resource.sort.memory", "128m");
        env.executeSql(
            "CREATE TABLE output(sort_key BIGINT, payload BIGINT) WITH ('connector'='blackhole')");
      }
      String statement =
          "INSERT INTO output SELECT MOD(id * 48271, "
              + cardinality
              + ") AS sort_key, id AS payload FROM input ORDER BY sort_key, payload";
      String plan = nativeEnv.explainSql(statement);
      assertTrue(
          plan.contains("NativeSort")
              && plan.contains("NativeExchange")
              && plan.contains("RowDataToArrow")
              && plan.contains("ArrowToRowData"),
          plan);
      assertFalse(stock.explainSql(statement).contains("NativeSort"));
      List<Long> stockNanos = new ArrayList<>(), nativeNanos = new ArrayList<>();
      List<Long> stockRuntimeMs = new ArrayList<>(), nativeRuntimeMs = new ArrayList<>();
      for (int trial = -warmups; trial < trials; trial++) {
        for (boolean nativeRun :
            trial % 2 == 0 ? new boolean[] {false, true} : new boolean[] {true, false}) {
          var env = nativeRun ? nativeEnv : stock;
          long started = System.nanoTime();
          var result = env.executeSql(statement);
          result.await();
          long nanos = System.nanoTime() - started;
          var client = result.getJobClient().orElseThrow();
          long input = metrics.recordsIn(client.getJobID().toString(), "NativeSort");
          assertEquals(nativeRun ? rows : 0, input);
          long runtime = client.getJobExecutionResult().get().getNetRuntime();
          if (trial >= 0) {
            (nativeRun ? nativeNanos : stockNanos).add(nanos);
            (nativeRun ? nativeRuntimeMs : stockRuntimeMs).add(runtime);
          }
        }
      }
      System.out.printf(
          "[batch-sort-job] rows=%d cardinality=%d parallelism=2 nativeTaskCapBytes=%d"
              + " stockSortMemoryBytes=%d warmups=%d trials=%d stockNanos=%s nativeNanos=%s"
              + " stockRuntimeMs=%s nativeRuntimeMs=%s%n",
          rows,
          cardinality,
          128L << 20,
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
