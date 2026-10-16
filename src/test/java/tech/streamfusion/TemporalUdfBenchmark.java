package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Arrays;
import java.util.Locale;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableDescriptor;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import tech.streamfusion.planner.NativePlanner;

@EnabledIfEnvironmentVariable(named = "SF_BENCHMARK", matches = "true")
class TemporalUdfBenchmark {
  @Test
  void completeJobsKeepTemporalConversionsAndBothTransposes() throws Exception {
    assertEquals("false", System.getProperty("streamfusion.exchange.zeroCopyLocal"), "Use -Pbench");
    for (String shape : System.getProperty("temporal.cases", "identity,text,epoch").split(","))
      run(shape);
  }

  private static void run(String shape) throws Exception {
    int rows = Integer.getInteger("temporal.rows", 2_000_000);
    int precision = Integer.getInteger("temporal.precision", 9);
    int warmups = Integer.getInteger("temporal.warmup", 2);
    int runs = Integer.getInteger("temporal.runs", 5);
    boolean expectNative =
        shape.equals("perimeter")
            || Boolean.parseBoolean(System.getProperty("temporal.expectNative", "true"));
    String sql;
    switch (shape) {
      case "perimeter":
        sql = "SELECT t, z FROM src";
        break;
      case "identity":
        sql = "SELECT ts_identity(t), ltz_identity(z) FROM src";
        break;
      case "text":
        sql = "SELECT ts_text(t) FROM src";
        break;
      case "epoch":
        sql = "SELECT ltz_epoch(z) FROM src";
        break;
      default:
        throw new IllegalArgumentException("Unknown temporal.cases: " + shape);
    }
    double[][] elapsed = new double[2][runs];
    for (int trial = 0; trial < warmups + runs; trial++) {
      for (int order = 0; order < 2; order++) {
        try (var metrics = new RecoveryExecutionMetrics()) {
          int engine = (trial + order) % 2;
          var table = FlinkTemporalUdfSqlHarnessTest.environment(precision, "UTC", rows, metrics);
          table.getConfig().set("streamfusion.transpose.batchRows", "1024");
          var scan = engine == 1 ? NativePlanner.install(table) : null;
          var query = table.sqlQuery(sql);
          table.createTemporaryTable(
              "sink",
              TableDescriptor.forConnector("blackhole")
                  .schema(Schema.newBuilder().fromResolvedSchema(query.getResolvedSchema()).build())
                  .build());
          String insert = "INSERT INTO sink " + sql;
          String plan = table.explainSql(insert);
          if (engine == 1 && expectNative) {
            assertTrue(
                plan.contains("NativeCalc")
                    && plan.contains("RowDataToArrow")
                    && plan.contains("ArrowToRowData"),
                plan);
          } else if (engine == 1) {
            assertEquals(0, scan.substitutions(), plan);
          }
          long startMillis = System.currentTimeMillis();
          long start = System.nanoTime();
          table.executeSql(insert).await();
          double seconds = (System.nanoTime() - start) / 1e9;
          System.out.printf(
              Locale.ROOT,
              "[temporal-udf-trial] case=%s engine=%s trial=%d warmup=%s rows=%d precision=%d"
                  + " start_ms=%d seconds=%.6f%n",
              shape,
              engine == 0 ? "Flink" : "StreamFusion",
              trial,
              trial < warmups,
              rows,
              precision,
              startMillis,
              seconds);
          if (trial >= warmups) elapsed[engine][trial - warmups] = seconds;
          if (engine == 0)
            assertTrue(
                metrics.nativeOperators().isEmpty(), "stock control executed native operators");
          if (engine == 1
              && Boolean.getBoolean("streamfusion.experimental.directTemporalCalc")
              && !shape.equals("perimeter")) {
            assertTrue(
                metrics.counterValue("directTemporalRows") > 0,
                "direct batch path did not execute");
            System.out.println(
                "[temporal-direct-calc] case="
                    + shape
                    + " rows="
                    + metrics.counterValue("directTemporalRows"));
          }
          if (engine == 1
              && Boolean.getBoolean("streamfusion.experimental.temporalAliases")
              && shape.equals("identity")) {
            assertTrue(
                metrics.counterValue("directTemporalAliasedValues") > 0,
                "timestamps did not share buffers");
            System.out.println(
                "[temporal-direct-aliases] values="
                    + metrics.counterValue("directTemporalAliasedValues"));
          }
          if (engine == 1 && expectNative)
            RecoveryExecutionMetrics.assertConsumed(metrics.nativeOperators(), "NativeCalc");
        }
      }
    }
    System.out.printf(
        Locale.ROOT,
        "[temporal-udf] case=%s rows=%d precision=%d native_expected=%s Flink=%.6fs"
            + " StreamFusion=%.6fs stock_trials=%s sf_trials=%s%n",
        shape,
        rows,
        precision,
        expectNative,
        median(elapsed[0]),
        median(elapsed[1]),
        Arrays.toString(elapsed[0]),
        Arrays.toString(elapsed[1]));
  }

  private static double median(double[] values) {
    var sorted = values.clone();
    Arrays.sort(sorted);
    return sorted[sorted.length / 2];
  }
}
