package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.TableEnvironment;
import org.junit.jupiter.api.Test;

/** Complete bounded jobs retain datagen, both transposes, JNI and a rowwise blackhole sink. */
class BatchSqlBenchmark {
  @Test
  void compareStockBatchJobs() throws Exception {
    int rows = Integer.getInteger("sf.batch.bench.rows", 1_000_000);
    int warmups = Integer.getInteger("sf.batch.bench.warmups", 2);
    int trials = Integer.getInteger("sf.batch.bench.trials", 5);
    String selected = System.getProperty("sf.batch.bench.query", "all");
    List<String[]> queries =
        List.of(
            new String[] {
              "arithmetic", "SELECT id * 3 + 7 AS payload FROM input WHERE MOD(id, 2) = 0"
            },
            new String[] {
              "hash",
              "SELECT SHA2(CONCAT(CAST(id AS STRING), 'streamfusion-batch-payload'), 256) AS"
                  + " payload FROM input"
            },
            new String[] {
              "strings",
              "SELECT REGEXP_REPLACE(CONCAT('prefix-', CAST(id AS STRING), '-suffix'), '[0-9]+',"
                  + " 'number') AS payload FROM input"
            },
            new String[] {"udf", "SELECT batch_add_one(id) AS payload FROM input"});
    try (var metrics = new RecoveryExecutionMetrics()) {
      metrics.setAsContext(new Configuration(), 1, 0);
      for (String[] query : queries) {
        if (!selected.equals("all") && !selected.equals(query[0])) continue;
        var stock = BatchSqlTest.environment(false, rows);
        var nativeEnv = BatchSqlTest.environment(true, rows);
        for (TableEnvironment env : List.of(stock, nativeEnv)) {
          env.createTemporarySystemFunction("batch_add_one", AddOne.class);
          var resultType = env.sqlQuery(query[1]).getResolvedSchema().getColumnDataTypes().get(0);
          env.executeSql(
              "CREATE TABLE output (payload "
                  + resultType.getLogicalType().asSerializableString()
                  + ") WITH ('connector'='blackhole')");
        }
        String statement = "INSERT INTO output " + query[1];
        String plan = nativeEnv.explainSql(statement);
        boolean admitted = plan.contains("NativeCalc");
        if (admitted)
          assertTrue(plan.contains("RowDataToArrow") && plan.contains("ArrowToRowData"), plan);
        assertFalse(stock.explainSql(statement).contains("NativeCalc"));
        List<Long> stockTimes = new ArrayList<>();
        List<Long> nativeTimes = new ArrayList<>();
        List<Long> stockRuntimeMillis = new ArrayList<>();
        List<Long> nativeRuntimeMillis = new ArrayList<>();
        String profile = System.getProperty("sf.batch.bench.profile");
        try (var recording = new jdk.jfr.Recording()) {
          if (profile != null) {
            recording.enable("jdk.ExecutionSample").withPeriod(java.time.Duration.ofMillis(5));
            recording.enable("jdk.ObjectAllocationSample").withStackTrace();
            recording.start();
          }
          for (int iteration = -warmups; iteration < trials; iteration++) {
            for (boolean nativeRun :
                iteration % 2 == 0 ? new boolean[] {true, false} : new boolean[] {false, true}) {
              var env = nativeRun ? nativeEnv : stock;
              long started = System.nanoTime();
              var result = env.executeSql(statement);
              result.await();
              long nanos = System.nanoTime() - started;
              String job = result.getJobClient().orElseThrow().getJobID().toString();
              long consumed = metrics.recordsIn(job, "NativeCalc");
              if (nativeRun && admitted)
                assertEquals(rows, consumed, "Native batch input rows for " + job);
              else assertTrue(consumed == 0, "Stock control executed native work");
              if (iteration >= 0) (nativeRun ? nativeTimes : stockTimes).add(nanos);
              if (iteration >= 0)
                (nativeRun ? nativeRuntimeMillis : stockRuntimeMillis)
                    .add(
                        result
                            .getJobClient()
                            .orElseThrow()
                            .getJobExecutionResult()
                            .get()
                            .getNetRuntime());
            }
          }
          System.out.println(
              "BATCH_BENCH query="
                  + query[0]
                  + " rows="
                  + rows
                  + " route="
                  + (admitted ? "native" : "stock_fallback")
                  + " warmups="
                  + warmups
                  + " trials="
                  + trials
                  + " stock_ns="
                  + stockTimes
                  + " enabled_ns="
                  + nativeTimes
                  + " stock_runtime_ms="
                  + stockRuntimeMillis
                  + " enabled_runtime_ms="
                  + nativeRuntimeMillis);
          if (profile != null) {
            recording.stop();
            recording.dump(java.nio.file.Path.of(profile + "-" + query[0] + ".jfr"));
          }
        }
      }
    } finally {
      tech.streamfusion.compat.FlinkTestContexts.clearForRemoteSubmission();
    }
  }

  public static class AddOne extends org.apache.flink.table.functions.ScalarFunction {
    public Long eval(Long value) {
      return value == null ? null : value + 1;
    }
  }
}
