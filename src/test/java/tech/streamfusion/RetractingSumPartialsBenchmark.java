package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Arrays;
import java.util.Locale;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import tech.streamfusion.planner.NativePlanner;

@EnabledIfEnvironmentVariable(named = "SF_BENCHMARK", matches = "true")
class RetractingSumPartialsBenchmark {
  private static final long ROWS = Long.getLong("sumretract.rows", 1_000_000L);
  private static final int WARMUP = Integer.getInteger("sumretract.warmup", 2);
  private static final int RUNS = Integer.getInteger("sumretract.runs", 3);
  private static final boolean EXPECT_NATIVE =
      Boolean.parseBoolean(System.getProperty("sumretract.expectNative", "true"));

  @Test
  void completeCascadeRetainsBothTransposesAndExecutesBothHalves() throws Exception {
    assertTrue(ROWS > 0 && WARMUP >= 1 && RUNS >= 3);
    for (boolean rocks : new boolean[] {false, true}) {
      for (String type : new String[] {"INT", "BIGINT"}) {
        String query =
            "INSERT INTO sink SELECT u, SUM(mx), COUNT(mx), AVG(mx), SUM(mx) FILTER (WHERE mx > 0)"
                + " FROM (SELECT k, u, MAX(v) AS mx FROM src GROUP BY k, u) GROUP BY u";
        double[][] times = new double[2][RUNS];
        var configuration = new Configuration();
        if (rocks)
          configuration.setString(
              "state.backend", "tech.streamfusion.state.RocksDBNativeStateBackendFactory");
        try (var metrics = new RecoveryExecutionMetrics()) {
          for (int trial = 0; trial < WARMUP + RUNS; trial++) {
            for (int turn = 0; turn < 2; turn++) {
              int engine = (trial + turn) % 2;
              var env = metrics.environment(configuration, 1, 0);
              var table = StreamTableEnvironment.create(env);
              table.getConfig().set("table.optimizer.agg-phase-strategy", "TWO_PHASE");
              table.getConfig().set("table.exec.mini-batch.enabled", "true");
              table.getConfig().set("table.exec.mini-batch.allow-latency", "1 h");
              table.getConfig().set("table.exec.mini-batch.size", "512");
              var valueInfo = type.equals("INT") ? Types.INT : Types.LONG;
              table.createTemporaryView(
                  "src",
                  env.fromSequence(0L, ROWS - 1)
                      .map(
                          i -> {
                            Object value = null;
                            if (i % 7 != 0) {
                              long v = i - ROWS / 2;
                              value = type.equals("INT") ? (Object) (int) v : (Object) v;
                            }
                            return Row.of((int) (i % 128), (int) (i % 16), value);
                          })
                      .returns(
                          Types.ROW_NAMED(
                              new String[] {"k", "u", "v"}, Types.INT, Types.INT, valueInfo)));
              table.executeSql(
                  "CREATE TABLE sink (u INT, s "
                      + type
                      + ", c BIGINT, a "
                      + type
                      + ", f "
                      + type
                      + ") WITH ('connector'='blackhole')");
              table.getConfig().set("streamfusion.native.enabled", Boolean.toString(engine == 1));
              var scan = NativePlanner.install(table);
              String plan = table.explainSql(query);
              if (engine == 1 && EXPECT_NATIVE)
                assertTrue(
                    plan.contains("NativeColumnarLocalGroupAggregate")
                        && plan.contains("NativeColumnarGroupAggregate")
                        && plan.contains("RowDataToArrow")
                        && plan.contains("ArrowToRowData"),
                    plan + scan.explainSummary());
              else assertFalse(plan.contains("NativeColumnarGroupAggregate"), plan);
              System.out.println(
                  "[sum-retract-trial-begin] engine="
                      + engine
                      + " type="
                      + type
                      + " rocks="
                      + rocks
                      + " trial="
                      + trial);
              long start = System.nanoTime();
              var result = table.executeSql(query);
              result.await();
              double seconds = (System.nanoTime() - start) / 1e9;
              System.out.println(
                  "[sum-retract-trial-end] engine="
                      + engine
                      + " type="
                      + type
                      + " rocks="
                      + rocks
                      + " trial="
                      + trial
                      + " seconds="
                      + seconds);
              if (engine == 1 && EXPECT_NATIVE) {
                assertTrue(
                    scan.substitutions() > 0 && scan.fallbackReasons().isEmpty(),
                    scan.explainSummary());
                String job = result.getJobClient().orElseThrow().getJobID().toString();
                var observed =
                    metrics.nativeOperators().stream()
                        .filter(row -> job.equals(row.get("jobId")))
                        .collect(tech.streamfusion.compat.ListCollectors.toList());
                assertEquals(
                    2,
                    observed.stream()
                        .filter(
                            row ->
                                row.get("operator")
                                        .toString()
                                        .startsWith("NativeColumnarLocalGroupAgg")
                                    && ((Number) row.get("recordsIn")).longValue() > 0)
                        .count(),
                    observed.toString());
                assertEquals(
                    2,
                    observed.stream()
                        .filter(
                            row ->
                                row.get("operator").toString().startsWith("NativeColumnarGroupAgg")
                                    && ((Number) row.get("recordsIn")).longValue() > 0)
                        .count(),
                    observed.toString());
              } else {
                assertEquals(0, scan.substitutions());
                if (engine == 1)
                  assertFalse(scan.fallbackReasons().isEmpty(), scan.explainSummary());
              }
              if (trial >= WARMUP) times[engine][trial - WARMUP] = seconds;
            }
          }
        }
        System.out.printf(
            Locale.ROOT,
            "[sum-retract] rows=%d type=%s rocks=%s native_route=%s stock_trials=%s"
                + " enabled_trials=%s%n",
            ROWS,
            type,
            rocks,
            EXPECT_NATIVE,
            Arrays.toString(times[0]),
            Arrays.toString(times[1]));
      }
    }
  }
}
