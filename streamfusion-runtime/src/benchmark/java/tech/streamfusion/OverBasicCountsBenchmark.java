package tech.streamfusion;

import java.util.Arrays;
import java.util.Locale;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableDescriptor;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import tech.streamfusion.planner.NativePlanner;

@EnabledIfEnvironmentVariable(named = "SF_BENCHMARK", matches = "true")
class OverBasicCountsBenchmark {
  @Test
  void completeJobsKeepRuntimeBasicValuesAndBothTransposes() throws Exception {
    for (String column : System.getProperty("over.cases", "d,s,t,instant_value").split(",")) {
      if (!java.util.Set.of("d", "s", "t", "instant_value").contains(column)) {
        throw new IllegalArgumentException("Unknown over.cases entry " + column);
      }
      for (String frame : System.getProperty("over.frames", "0,1,2,3").split(",")) {
        measure(column, Integer.parseInt(frame));
      }
    }
  }

  private static void measure(String column, int frame) throws Exception {
    String bounds;
    switch (frame) {
      case 0:
        bounds = "RANGE UNBOUNDED PRECEDING";
        break;
      case 1:
        bounds = "ROWS BETWEEN 3 PRECEDING AND CURRENT ROW";
        break;
      case 2:
        bounds = "RANGE BETWEEN INTERVAL '1' SECOND PRECEDING AND CURRENT ROW";
        break;
      case 3:
        bounds = "ROWS UNBOUNDED PRECEDING";
        break;
      default:
        throw new IllegalArgumentException("Unknown over.frames entry " + frame);
    }
    int rows = Integer.getInteger("over.rows", 1_000_000);
    int warmup = Integer.getInteger("over.warmup", 2);
    int runs = Integer.getInteger("over.runs", 5);
    String sql =
        "SELECT k, COUNT(" + column + ") OVER (PARTITION BY k ORDER BY rt " + bounds + ") FROM src";
    double[][] times = new double[2][runs];
    for (int trial = 0; trial < warmup + runs; trial++) {
      for (int turn = 0; turn < 2; turn++) {
        int engine = (trial + turn) % 2;
        var table = FlinkOverBasicCountsSqlHarnessTest.environment(rows);
        var scan = engine == 1 ? NativePlanner.install(table) : null;
        var query = table.sqlQuery(sql);
        table.createTemporaryTable(
            "sink",
            TableDescriptor.forConnector("blackhole")
                .schema(Schema.newBuilder().fromResolvedSchema(query.getResolvedSchema()).build())
                .build());
        String insert = "INSERT INTO sink " + sql;
        String plan = table.explainSql(insert);
        if (engine == 1
            && (!plan.contains("NativeOverAggregate")
                || !plan.contains("RowDataToArrow")
                || !plan.contains("ArrowToRowData"))) {
          throw new IllegalStateException("Expected native OVER and both transposes: " + plan);
        }
        long start = System.nanoTime();
        table.executeSql(insert).await();
        double seconds = (System.nanoTime() - start) / 1e9;
        if (scan != null && scan.substitutions() == 0) {
          throw new IllegalStateException(scan.explainSummary());
        }
        if (trial >= warmup) times[engine][trial - warmup] = seconds;
      }
    }
    System.out.printf(
        Locale.ROOT,
        "[over-count] column=%s frame=%d rows=%d host_mean=%.6fs native_mean=%.6fs "
            + "host_trials=%s native_trials=%s%n",
        column,
        frame,
        rows,
        Arrays.stream(times[0]).average().orElseThrow(),
        Arrays.stream(times[1]).average().orElseThrow(),
        Arrays.toString(times[0]),
        Arrays.toString(times[1]));
  }
}
