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
  private enum Engine {
    FLINK,
    PREVIOUS,
    NATIVE,
    FALLBACK
  }

  @Test
  void completeJobsKeepTemporalConversionsAndBothTransposes() throws Exception {
    assertEquals("false", System.getProperty("streamfusion.exchange.zeroCopyLocal"), "Use -Pbench");
    String direct = System.getProperty("streamfusion.experimental.directTemporalCalc");
    try {
      for (String shape : System.getProperty("temporal.cases", "identity,text,epoch").split(","))
        run(shape, direct);
    } finally {
      if (direct == null) System.clearProperty("streamfusion.experimental.directTemporalCalc");
      else System.setProperty("streamfusion.experimental.directTemporalCalc", direct);
    }
  }

  private static void run(String shape, String direct) throws Exception {
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
      case "mixed":
        sql = "SELECT id, ts_identity(t), ltz_identity(z) FROM src";
        break;
      case "text":
        sql = "SELECT ts_text(t) FROM src";
        break;
      case "epoch":
        sql = "SELECT ltz_epoch(z) FROM src";
        break;
      case "pipeline":
        sql = "SELECT ts_identity(t), COUNT(*) FROM src GROUP BY ts_identity(t)";
        break;
      default:
        throw new IllegalArgumentException("Unknown temporal.cases: " + shape);
    }
    Engine[] engines =
        Arrays.stream(
                System.getProperty("temporal.engines", "FLINK,PREVIOUS,NATIVE,FALLBACK").split(","))
            .map(Engine::valueOf)
            .toArray(Engine[]::new);
    double[][] elapsed = new double[engines.length][runs];
    for (int trial = 0; trial < warmups + runs; trial++) {
      for (int order = 0; order < engines.length; order++) {
        try (var metrics = new RecoveryExecutionMetrics()) {
          int index = (trial + order) % engines.length;
          Engine engine = engines[index];
          // PREVIOUS executes the production callback used before this optimization, not fallback.
          System.setProperty(
              "streamfusion.experimental.directTemporalCalc",
              engine == Engine.PREVIOUS ? "false" : direct == null ? "true" : direct);
          var table = FlinkTemporalUdfSqlHarnessTest.environment(precision, "UTC", rows, metrics);
          if (engine == Engine.FALLBACK)
            table.getConfig().set("streamfusion.calc.temporalUdf.enabled", "false");
          table.getConfig().set("streamfusion.transpose.batchRows", "1024");
          var scan = engine != Engine.FLINK ? NativePlanner.install(table) : null;
          var query = table.sqlQuery(sql);
          table.createTemporaryTable(
              "sink",
              TableDescriptor.forConnector("blackhole")
                  .schema(Schema.newBuilder().fromResolvedSchema(query.getResolvedSchema()).build())
                  .build());
          String insert = "INSERT INTO sink " + sql;
          String plan = table.explainSql(insert);
          boolean nativeRoute =
              expectNative && (engine != Engine.FALLBACK || shape.equals("perimeter"));
          if (engine != Engine.FLINK && nativeRoute) {
            assertTrue(
                plan.contains("NativeCalc")
                    && plan.contains("RowDataToArrow")
                    && plan.contains("ArrowToRowData"),
                plan);
          } else if (engine != Engine.FLINK) {
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
              engine,
              trial,
              trial < warmups,
              rows,
              precision,
              startMillis,
              seconds);
          if (trial >= warmups) elapsed[index][trial - warmups] = seconds;
          if (engine == Engine.FLINK || !nativeRoute)
            assertTrue(
                metrics.nativeOperators().isEmpty(), "stock control executed native operators");
          if (engine == Engine.NATIVE
              && Boolean.parseBoolean(
                  System.getProperty("streamfusion.experimental.directTemporalCalc", "true"))
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
          if (engine == Engine.NATIVE
              && Boolean.parseBoolean(
                  System.getProperty("streamfusion.experimental.directTemporalCalc", "true"))
              && Boolean.parseBoolean(
                  System.getProperty("streamfusion.experimental.temporalAliases", "true"))
              && shape.equals("identity")) {
            assertTrue(
                metrics.counterValue("directTemporalAliasedValues") > 0,
                "timestamps did not share buffers");
            System.out.println(
                "[temporal-direct-aliases] values="
                    + metrics.counterValue("directTemporalAliasedValues"));
          }
          if (engine == Engine.PREVIOUS)
            assertEquals(0, metrics.counterValue("directTemporalRows"));
          if (engine != Engine.FLINK && nativeRoute)
            RecoveryExecutionMetrics.assertConsumed(metrics.nativeOperators(), "NativeCalc");
          if (engine != Engine.FLINK && nativeRoute && shape.equals("pipeline"))
            RecoveryExecutionMetrics.assertConsumed(
                metrics.nativeOperators(), "NativeColumnarGroupAggregate");
        }
      }
    }
    for (int index = 0; index < engines.length; index++)
      System.out.printf(
          Locale.ROOT,
          "[temporal-udf] case=%s rows=%d precision=%d engine=%s median=%.6fs trials=%s%n",
          shape,
          rows,
          precision,
          engines[index],
          median(elapsed[index]),
          Arrays.toString(elapsed[index]));
  }

  private static double median(double[] values) {
    var sorted = values.clone();
    Arrays.sort(sorted);
    return sorted[sorted.length / 2];
  }
}
