package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Arrays;
import java.util.Locale;
import java.util.function.BiFunction;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.TableEnvironment;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import tech.streamfusion.planner.NativePlanner;

@EnabledIfEnvironmentVariable(named = "SF_BENCHMARK", matches = "true")
class UdfTypeBoundaryBenchmark {
  @Test
  void arrayCompleteJobs() throws Exception {
    for (String type : System.getProperty("boundary.arrays", "int,decimal").split(","))
      run(
          "ARRAY_" + type,
          (rows, metrics) -> FlinkArrayScalarUdfSqlHarnessTest.environment(type, rows, metrics),
          "SELECT id,array_identity(p),CARDINALITY(array_identity(p)) FROM src");
  }

  @Test
  void dateAndTimeCompleteJobs() throws Exception {
    for (String type : System.getProperty("boundary.temporal", "DATE,TIME").split(","))
      run(
          type,
          (rows, metrics) -> FlinkDateTimeUdfSqlHarnessTest.environment(type, rows, metrics),
          "SELECT id,identity_value(v),observe_value(v),result_value(id) FROM src");
  }

  static void run(
      String type,
      BiFunction<Integer, RecoveryExecutionMetrics, TableEnvironment> factory,
      String query)
      throws Exception {
    run(type, factory, query, "NativeCalc", true);
  }

  static void run(
      String type,
      BiFunction<Integer, RecoveryExecutionMetrics, TableEnvironment> factory,
      String query,
      String operator,
      boolean nativeExpected)
      throws Exception {
    assertEquals("false", System.getProperty("streamfusion.exchange.zeroCopyLocal"), "Use -Pbench");
    int rows = Integer.getInteger("boundary.rows", 262144);
    int warmups = Integer.getInteger("boundary.warmups", 2);
    int runs = Integer.getInteger("boundary.runs", 5);
    double[][] samples = new double[2][runs];
    for (int trial = 0; trial < warmups + runs; trial++) {
      for (int order = 0; order < 2; order++) {
        int engine = (trial + order) & 1;
        try (var metrics = new RecoveryExecutionMetrics()) {
          var table = factory.apply(rows, metrics);
          table.getConfig().set("streamfusion.transpose.batchRows", "1024");
          table.createTemporaryTable(
              "sink",
              TableDescriptor.forConnector("blackhole")
                  .schema(
                      Schema.newBuilder()
                          .fromResolvedSchema(table.sqlQuery(query).getResolvedSchema())
                          .build())
                  .build());
          var scan = engine == 1 ? NativePlanner.install(table) : null;
          String insert = "INSERT INTO sink " + query;
          String plan = table.explainSql(insert);
          if (engine == 1 && nativeExpected) {
            assertTrue(
                plan.contains(operator)
                    && plan.contains("RowDataToArrow")
                    && plan.contains("ArrowToRowData"),
                plan);
            assertTrue(scan.fallbackReasons().isEmpty(), scan.explainSummary());
          } else if (engine == 1) assertEquals(0, scan.substitutions(), plan);
          long start = System.nanoTime();
          table.executeSql(insert).await();
          double seconds = (System.nanoTime() - start) / 1e9;
          System.out.printf(
              Locale.ROOT,
              "[udf-boundary-trial] type=%s engine=%s trial=%d warmup=%s rows=%d seconds=%.6f%n",
              type,
              engine == 0 ? "stock" : "native",
              trial,
              trial < warmups,
              rows,
              seconds);
          if (trial >= warmups) samples[engine][trial - warmups] = seconds;
          if (engine == 1 && nativeExpected)
            RecoveryExecutionMetrics.assertConsumed(metrics.nativeOperators(), operator);
          else assertTrue(metrics.nativeOperators().isEmpty());
        }
      }
    }
    System.out.printf(
        Locale.ROOT,
        "[udf-boundary] type=%s rows=%d stock=%s native=%s%n",
        type,
        rows,
        Arrays.toString(samples[0]),
        Arrays.toString(samples[1]));
  }
}
