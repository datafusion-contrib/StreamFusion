package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Locale;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import tech.streamfusion.planner.NativePlanner;

/** Complete jobs retain row sources/sinks, both transposes, JNI and normal IPC exchanges. */
@EnabledIfEnvironmentVariable(named = "SF_BENCHMARK", matches = "true")
class NativeBoundaryBenchmark {
  @Test
  void physicalPipelines() throws Exception {
    assertEquals("false", System.getProperty("streamfusion.exchange.zeroCopyLocal"));
    long rows = Long.getLong("boundary.rows", 1_000_000L);
    int warmups = Integer.getInteger("boundary.warmup", 2);
    int runs = Integer.getInteger("boundary.runs", 5);
    Path plans = Path.of(System.getProperty("boundary.plans", "target/boundary-plans"));
    Files.createDirectories(plans);
    for (String shape : System.getProperty("boundary.cases", "calc,aggregate,join,callback").split(",")) {
      String sql = query(shape);
      for (String size : System.getProperty("boundary.batches", "64,1024").split(",")) {
        int batch = Integer.parseInt(size);
        try (var metrics = new RecoveryExecutionMetrics()) {
          for (int trial = 0; trial < warmups + runs; trial++) {
            for (int order = 0; order < 2; order++) {
              int engine = (trial + order) % 2;
              var env = metrics.environment(new Configuration(), 1, 0);
              var table = StreamTableEnvironment.create(env);
              table.getConfig().set("streamfusion.transpose.batchRows", size);
              table.getConfig().set("table.optimizer.agg-phase-strategy", "ONE_PHASE");
              table.getConfig().set("table.exec.mini-batch.enabled", "true");
              table.getConfig().set("table.exec.mini-batch.allow-latency", "1 h");
              table.getConfig().set("table.exec.mini-batch.size", "1024");
              table.createTemporaryView("src", env.fromSequence(0, rows - 1)
                  .map(i -> Row.of(i, (int) (i % 64), i % 7 == 0 ? null : i % 1000))
                  .returns(Types.ROW_NAMED(new String[] {"id", "k", "v"}, Types.LONG, Types.INT, Types.LONG)));
              table.createTemporaryView("dim", env.fromSequence(0, 63)
                  .map(i -> Row.of(i.intValue(), i + 1))
                  .returns(Types.ROW_NAMED(new String[] {"k", "v"}, Types.INT, Types.LONG)));
              table.createTemporarySystemFunction("boundary_callback", Callback.class);
              if (engine == 1) NativePlanner.install(table);
              table.createTemporaryTable("sink", TableDescriptor.forConnector("blackhole")
                  .schema(Schema.newBuilder().fromResolvedSchema(table.sqlQuery(sql).getResolvedSchema()).build()).build());
              String insert = "INSERT INTO sink " + sql;
              String plan = table.explainSql(insert);
              String name = engine == 0 ? "Flink" : "StreamFusion";
              if (trial == 0) Files.writeString(plans.resolve(shape + "-" + batch + "-" + name + ".txt"), plan);
              if (engine == 1) {
                assertTrue(plan.contains("NativeCalc") && plan.contains("RowDataToArrow") && plan.contains("ArrowToRowData"), plan);
                if (shape.equals("aggregate")) assertTrue(plan.contains("NativeColumnarGroupAggregate"), plan);
                if (shape.equals("join")) assertTrue(plan.contains("NativeColumnarUpdatingJoin"), plan);
              }
              long start = System.nanoTime();
              var result = table.executeSql(insert);
              result.await();
              double seconds = (System.nanoTime() - start) / 1e9;
              String job = result.getJobClient().orElseThrow().getJobID().toString();
              if (engine == 1) {
                var observed = metrics.nativeOperators().stream()
                    .filter(row -> job.equals(row.get("jobId")))
                    .collect(java.util.stream.Collectors.toList());
                RecoveryExecutionMetrics.assertConsumed(observed, "NativeCalc");
                if (shape.equals("aggregate")) RecoveryExecutionMetrics.assertConsumed(observed, "NativeColumnarGroupAggregate");
                if (shape.equals("join")) assertTrue(observed.stream().anyMatch(row ->
                    row.get("operator").toString().startsWith("NativeColumnarUpdatingJoin")
                        && ((Number) row.get("recordsIn")).longValue() > 0), observed::toString);
                System.out.println("[boundary-work] case=" + shape + " batch=" + batch + " job=" + job + " operators=" + observed);
              }
              System.out.printf(Locale.ROOT,
                  "[boundary-trial] case=%s batch=%d engine=%s trial=%d warmup=%s rows=%d seconds=%.6f%n",
                  shape, batch, name, trial, trial < warmups, rows, seconds);
            }
          }
        }
      }
    }
  }

  private static String query(String shape) {
    switch (shape) {
      case "calc": return "SELECT id + 1 AS id, k, v * 3 AS v FROM src WHERE MOD(id, 5) <> 0";
      case "aggregate": return "SELECT k, SUM(v + 1) + 1 AS total FROM src WHERE v IS NOT NULL GROUP BY k";
      case "join": return "SELECT a.id, a.v + b.v AS v FROM src a JOIN dim b ON a.k = b.k WHERE a.v IS NOT NULL";
      case "callback": return "SELECT id, boundary_callback(v) AS v FROM src";
      default: throw new IllegalArgumentException("Unknown boundary case " + shape);
    }
  }

  public static class Callback extends ScalarFunction {
    public Long eval(Long value) { return value == null ? null : value + 1; }
    @Override public boolean isDeterministic() { return false; }
  }
}
