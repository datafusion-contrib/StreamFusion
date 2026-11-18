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
class OverBinaryIntervalCountBenchmark {
  @Test
  void completeJobsKeepProductionOverStateAndBothTransposes() throws Exception {
    assertEquals("false", System.getProperty("streamfusion.exchange.zeroCopyLocal"));
    int rows = Integer.getInteger("over.rows", 500_000);
    int warmups = Integer.getInteger("over.warmup", 2);
    int runs = Integer.getInteger("over.runs", 5);
    boolean nativeExpected = Boolean.parseBoolean(System.getProperty("over.expectNative", "true"));
    String order = System.getProperty("over.order", "rt");
    if (!java.util.Set.of("rt", "pt").contains(order)) throw new IllegalArgumentException(order);
    for (String column : System.getProperty("over.cases", "b,d,m").split(",")) {
      if (!java.util.Set.of("b", "d", "m").contains(column))
        throw new IllegalArgumentException(column);
      for (String frame : System.getProperty("over.frames", "0,1,2,3").split(",")) {
        String bounds;
        switch (Integer.parseInt(frame)) {
          case 0:
            bounds = "RANGE UNBOUNDED PRECEDING";
            break;
          case 1:
            bounds = "ROWS BETWEEN 3 PRECEDING AND CURRENT ROW";
            break;
          case 2:
            bounds = "RANGE BETWEEN INTERVAL '0.3' SECOND PRECEDING AND CURRENT ROW";
            break;
          case 3:
            bounds = "ROWS UNBOUNDED PRECEDING";
            break;
          default:
            throw new IllegalArgumentException(frame);
        }
        String sql =
            "SELECT k, COUNT("
                + column
                + ") OVER (PARTITION BY k ORDER BY "
                + order
                + " "
                + bounds
                + ") FROM src";
        double[][] elapsed = new double[2][runs];
        for (int trial = 0; trial < warmups + runs; trial++) {
          try (var metrics = new RecoveryExecutionMetrics()) {
            for (int turn = 0; turn < 2; turn++) {
              int engine = (trial + turn) & 1;
              var table = FlinkOverBinaryIntervalCountSqlHarnessTest.environment(metrics, rows);
              table.getConfig().set("streamfusion.transpose.batchRows", "1024");
              table.createTemporaryTable(
                  "sink",
                  TableDescriptor.forConnector("blackhole")
                      .schema(
                          Schema.newBuilder()
                              .fromResolvedSchema(table.sqlQuery(sql).getResolvedSchema())
                              .build())
                      .build());
              String insert = "INSERT INTO sink " + sql;
              if (engine == 1) {
                var scan = NativePlanner.install(table);
                String plan = table.explainSql(insert);
                if (nativeExpected)
                  assertTrue(
                      plan.contains("NativeOverAggregate")
                          && plan.contains("RowDataToArrow")
                          && plan.contains("ArrowToRowData"),
                      plan);
                else assertEquals(0, scan.substitutions(), plan);
              }
              long start = System.nanoTime();
              table.executeSql(insert).await();
              double seconds = (System.nanoTime() - start) / 1e9;
              System.out.printf(
                  Locale.ROOT,
                  "[over-binary-interval-trial] column=%s frame=%s order=%s engine=%s trial=%d"
                      + " warmup=%s rows=%d seconds=%.6f%n",
                  column,
                  frame,
                  order,
                  engine == 0 ? "Flink" : "StreamFusion",
                  trial,
                  trial < warmups,
                  rows,
                  seconds);
              if (trial >= warmups) elapsed[engine][trial - warmups] = seconds;
            }
            if (nativeExpected)
              RecoveryExecutionMetrics.assertConsumed(
                  metrics.nativeOperators(), "NativeOverAggregate");
          }
        }
        System.out.printf(
            Locale.ROOT,
            "[over-binary-interval] column=%s frame=%s order=%s rows=%d stock=%.6f native=%.6f"
                + " stock_trials=%s native_trials=%s%n",
            column,
            frame,
            order,
            rows,
            median(elapsed[0]),
            median(elapsed[1]),
            Arrays.toString(elapsed[0]),
            Arrays.toString(elapsed[1]));
      }
    }
  }

  private static double median(double[] values) {
    double[] sorted = values.clone();
    Arrays.sort(sorted);
    return sorted[sorted.length / 2];
  }
}
