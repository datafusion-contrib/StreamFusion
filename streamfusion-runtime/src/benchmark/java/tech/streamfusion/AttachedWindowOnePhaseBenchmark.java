package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.*;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.Locale;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import tech.streamfusion.planner.NativePlanner;

@EnabledIfEnvironmentVariable(named = "SF_BENCHMARK", matches = "true")
class AttachedWindowOnePhaseBenchmark {
  private static final long ROWS = Long.getLong("attached.rows", 1_000_000L);
  private static final int WARMUP = Integer.getInteger("attached.warmup", 2);
  private static final int RUNS = Integer.getInteger("attached.runs", 3);
  private static final boolean EXPECT_NATIVE =
      Boolean.parseBoolean(System.getProperty("attached.expectNative", "true"));

  @Test
  void rowwiseCountMaxCascadeRetainsNativeStateAndBothTransposes() throws Exception {
    assertTrue(ROWS > 0 && WARMUP >= 1 && RUNS >= 3);
    for (String window : new String[] {"TUMBLE", "HOP", "CUMULATE"}) {
      for (boolean rocks : new boolean[] {false, true}) {
        for (int keys : new int[] {64, 4096}) {
          if (!System.getProperty("attached.window", window).equals(window)
              || Boolean.parseBoolean(System.getProperty("attached.rocks", Boolean.toString(rocks)))
                  != rocks
              || Integer.getInteger("attached.keys", keys) != keys) continue;
          String args =
              !window.equals("TUMBLE")
                  ? ", INTERVAL '2' SECOND, INTERVAL '"
                      + Integer.getInteger("attached.sizeSeconds", 10)
                      + "' SECOND"
                  : ", INTERVAL '10' SECOND";
          String query =
              "INSERT INTO sink SELECT "
                  + (Boolean.getBoolean("attached.mixed")
                      ? "SUM(n), MIN(n), MAX(n), AVG(n), COUNT(*)"
                      : "MAX(n)")
                  + ", wstart, wend FROM (SELECT k, COUNT(*) AS n,"
                  + " window_start AS wstart, window_end AS wend FROM TABLE("
                  + window
                  + "(TABLE src, DESCRIPTOR(rt)"
                  + args
                  + "))"
                  + " GROUP BY k, window_start, window_end) counts GROUP BY wstart, wend";
          double[][] times = new double[2][RUNS];
          var configuration = new Configuration();
          if (rocks)
            configuration.setString(
                "state.backend", "tech.streamfusion.state.RocksDBNativeStateBackendFactory");
          try (var metrics = new RecoveryExecutionMetrics()) {
            for (int trial = 0; trial < WARMUP + RUNS; trial++) {
              for (int turn = 0; turn < 2; turn++) {
                int engine = (trial + turn) % 2;
                if (Boolean.getBoolean("attached.nativeOnly") && engine == 0) continue;
                var env = metrics.environment(configuration, 1, 0);
                var table = StreamTableEnvironment.create(env);
                table.getConfig().setLocalTimeZone(ZoneOffset.UTC);
                table
                    .getConfig()
                    .set(
                        "table.optimizer.agg-phase-strategy",
                        System.getProperty("attached.phase", "ONE_PHASE"));
                if (Boolean.parseBoolean(System.getProperty("attached.optIn", "false")))
                  table
                      .getConfig()
                      .set("streamfusion.window.attached.single-phase.enabled", "true");
                LocalDateTime base = LocalDateTime.of(2020, 1, 1, 0, 0);
                var stream =
                    env.fromSequence(0, ROWS - 1)
                        .map(
                            i ->
                                Row.of(
                                    i % 11 == 0 ? null : (int) (i % keys),
                                    base.plusNanos((i / 1024) * 10_000_000L)))
                        .returns(
                            Types.ROW_NAMED(
                                new String[] {"k", "rt"}, Types.INT, Types.LOCAL_DATE_TIME));
                table.createTemporaryView(
                    "src",
                    stream,
                    Schema.newBuilder()
                        .column("k", DataTypes.INT())
                        .column("rt", DataTypes.TIMESTAMP(3))
                        .watermark("rt", "rt - INTERVAL '1' SECOND")
                        .build());
                var resolved =
                    table
                        .sqlQuery(query.substring("INSERT INTO sink ".length()))
                        .getResolvedSchema();
                table.createTemporaryTable(
                    "sink",
                    org.apache.flink.table.api.TableDescriptor.forConnector("blackhole")
                        .schema(Schema.newBuilder().fromResolvedSchema(resolved).build())
                        .build());
                table.getConfig().set("streamfusion.native.enabled", Boolean.toString(engine == 1));
                var scan = NativePlanner.install(table);
                String plan = table.explainSql(query);
                if (engine == 1 && EXPECT_NATIVE)
                  assertTrue(
                      (plan.contains("NativeColumnarWindowAggregate")
                              || plan.contains("NativeColumnarGlobalWindowAggregate"))
                          && plan.contains("RowDataToArrow")
                          && plan.contains("ArrowToRowData"),
                      plan + scan.explainSummary());
                else {
                  assertFalse(plan.contains("NativeColumnarWindowAggregate"), plan);
                  assertFalse(plan.contains("NativeColumnarGlobalWindowAggregate"), plan);
                }
                System.out.printf(
                    "[attached-trial-begin] engine=%d window=%s rocks=%s keys=%d trial=%d%n",
                    engine, window, rocks, keys, trial);
                long start = System.nanoTime();
                var result = table.executeSql(query);
                result.await();
                double seconds = (System.nanoTime() - start) / 1e9;
                System.out.printf(
                    Locale.ROOT,
                    "[attached-trial-end] engine=%d window=%s rocks=%s keys=%d trial=%d"
                        + " seconds=%.9f%n",
                    engine,
                    window,
                    rocks,
                    keys,
                    trial,
                    seconds);
                if (engine == 1 && EXPECT_NATIVE) {
                  assertTrue(
                      scan.substitutions() > 0 && scan.fallbackReasons().isEmpty(),
                      scan.explainSummary());
                  String job = result.getJobClient().orElseThrow().getJobID().toString();
                  var executed =
                      metrics.nativeOperators().stream()
                          .filter(
                              row ->
                                  job.equals(row.get("jobId"))
                                      && row.get("operator").toString().contains("WindowAgg")
                                      && ((Number) row.get("recordsIn")).longValue() > 0)
                          .collect(tech.streamfusion.compat.ListCollectors.toList());
                  assertTrue(executed.size() >= 2, executed.toString());
                  assertTrue(
                      executed.stream()
                          .anyMatch(row -> ((Number) row.get("recordsIn")).longValue() == ROWS),
                      executed.toString());
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
              "[attached] rows=%d window=%s rocks=%s keys=%d phase=%s size_seconds=%d mixed=%s"
                  + " opt_in=%s native_route=%s stock_trials=%s"
                  + " enabled_trials=%s%n",
              ROWS,
              window,
              rocks,
              keys,
              System.getProperty("attached.phase", "ONE_PHASE"),
              Integer.getInteger("attached.sizeSeconds", 10),
              Boolean.getBoolean("attached.mixed"),
              Boolean.getBoolean("attached.optIn"),
              EXPECT_NATIVE,
              Arrays.toString(times[0]),
              Arrays.toString(times[1]));
        }
      }
    }
  }
}
