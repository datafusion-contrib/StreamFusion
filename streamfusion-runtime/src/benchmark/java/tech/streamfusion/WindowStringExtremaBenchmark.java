package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.stream.Stream;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import tech.streamfusion.planner.NativePlanner;

/** Complete row-source/collecting-sink jobs in one shared cluster; both transposes remain timed. */
@EnabledIfEnvironmentVariable(named = "SF_BENCHMARK", matches = "true")
class WindowStringExtremaBenchmark {
  private static final long ROWS = Long.getLong("window.string.rows", 2_000_000L);
  private static final int GROUPS = Integer.getInteger("window.string.groups", 64);
  private static final int WARMUP = Integer.getInteger("window.string.warmup", 2);
  private static final int RUNS = Integer.getInteger("window.string.runs", 5);
  private static final String SQL = "SELECT k, window_start, window_end, MIN(s), MAX(s), COUNT(*) "
      + "FROM TABLE(TUMBLE(TABLE src, DESCRIPTOR(rt), INTERVAL '10' SECOND)) "
      + "GROUP BY k, window_start, window_end";

  static Stream<Arguments> fixtures() {
    return Arrays.stream(System.getProperty("window.string.phases", "ONE_PHASE,TWO_PHASE").split(","))
        .flatMap(phase -> Arrays.stream(System.getProperty("window.string.widths", "16,256").split(","))
            .map(width -> Arguments.of(phase, Integer.parseInt(width))));
  }

  @ParameterizedTest
  @MethodSource("fixtures")
  void compareCompleteJobs(String phase, int width) throws Exception {
    if (ROWS <= 0 || GROUPS <= 0 || width < 8 || WARMUP < 1 || RUNS < 3) {
      throw new IllegalArgumentException("invalid fixture");
    }
    String[] values = new String[1024];
    for (int i = 0; i < values.length; i++) {
      values[i] = String.format(Locale.ROOT, "%08d", i) + "x".repeat(width - 8);
    }
    try (var metrics = new RecoveryExecutionMetrics()) {
      var witness = environment(metrics, phase, values);
      witness.executeSql("CREATE TABLE sink (k BIGINT, window_start TIMESTAMP(3), "
          + "window_end TIMESTAMP(3), lo STRING, hi STRING, n BIGINT) WITH ('connector' = 'blackhole')");
      String plan = NativePlanner.explain(witness, "INSERT INTO sink " + SQL);
      assertTrue(plan.contains("RowDataToArrow") && plan.contains("ArrowToRowData"), plan);
      assertTrue(plan.contains(phase.equals("TWO_PHASE")
          ? "NativeColumnarGlobalWindowAggregate" : "NativeColumnarWindowAggregate"), plan);
      if (phase.equals("TWO_PHASE")) {
        assertTrue(plan.contains("NativeColumnarLocalWindowAggregate"), plan);
      }
      double[][] samples = new double[3][RUNS];
      for (int trial = 0; trial < WARMUP + RUNS; trial++) {
        var outputs = new ArrayList<List<List<Object>>>();
        for (int engine = 0; engine < 3; engine++) outputs.add(null);
        for (int turn = 0; turn < 3; turn++) {
          int engine = (trial + turn) % 3;
          var table = environment(metrics, phase, values);
          if (engine == 2) {
            table.getConfig().set("streamfusion.operator.windowAggregate.enabled", "false");
            table.getConfig().set("streamfusion.operator.localWindowAggregate.enabled", "false");
            table.getConfig().set("streamfusion.operator.globalWindowAggregate.enabled", "false");
          }
          var scan = engine > 0 ? NativePlanner.install(table) : null;
          var output = new ArrayList<List<Object>>();
          long start = System.nanoTime();
          var result = table.executeSql(SQL);
          String jobId = result.getJobClient().orElseThrow().getJobID().toString();
          try (var rows = result.collect()) {
            while (rows.hasNext()) {
              Row row = rows.next();
              var fields = new ArrayList<Object>();
              fields.add(row.getKind());
              for (int col = 0; col < row.getArity(); col++) fields.add(row.getField(col));
              output.add(fields);
            }
          }
          double seconds = (System.nanoTime() - start) / 1e9;
          outputs.set(engine, output);
          if (scan != null) {
            assertEquals(engine == 1, scan.substitutions() > 0, scan.explainSummary());
            if (engine == 1) {
              var execution = metrics.nativeOperators().stream()
                  .filter(row -> jobId.equals(row.get("jobId")))
                  .collect(java.util.stream.Collectors.toList());
              RecoveryExecutionMetrics.assertConsumed(execution, phase.equals("TWO_PHASE")
                  ? "NativeColumnarGlobalWindowAggregate" : "NativeColumnarWindowAggregate");
              if (phase.equals("TWO_PHASE")) {
                RecoveryExecutionMetrics.assertConsumed(execution, "NativeColumnarLocalWindowAggregate");
              }
            }
          }
          if (trial >= WARMUP) samples[engine][trial - WARMUP] = seconds;
        }
        for (int engine = 1; engine < 3; engine++) {
          assertEquals(NativeParity.multiset(outputs.get(0)), NativeParity.multiset(outputs.get(engine)));
        }
      }
      System.out.printf(Locale.ROOT,
          "[string-window-job] phase=%s width=%d rows=%d groups=%d stock=%.6fs native=%.6fs fallback=%.6fs stock_native=%.3fx stock_samples=%s native_samples=%s fallback_samples=%s%n",
          phase, width, ROWS, GROUPS, median(samples[0]), median(samples[1]), median(samples[2]),
          median(samples[0]) / median(samples[1]), Arrays.toString(samples[0]),
          Arrays.toString(samples[1]), Arrays.toString(samples[2]));
    }
  }

  private static double median(double[] samples) {
    double[] sorted = samples.clone();
    Arrays.sort(sorted);
    return sorted[sorted.length / 2];
  }

  private static TableEnvironment environment(RecoveryExecutionMetrics metrics, String phase, String[] values) {
    var env = metrics.environment(new Configuration(), 2, 0);
    var table = StreamTableEnvironment.create(env);
    table.getConfig().setLocalTimeZone(java.time.ZoneOffset.UTC);
    table.getConfig().set("table.optimizer.agg-phase-strategy", phase);
    table.getConfig().set("streamfusion.window.string-extrema.single-phase.enabled", "true");
    var input = env.fromSequence(0, ROWS - 1).map(id -> Row.of(id % GROUPS,
        id % GROUPS == 0 || id % 7 == 0 ? null : values[(int) ((id + id / GROUPS) % values.length)],
        1000L + id % 10000))
        .returns(Types.ROW_NAMED(new String[] {"k", "s", "ts"}, Types.LONG, Types.STRING, Types.LONG))
        .assignTimestampsAndWatermarks(WatermarkStrategy.<Row>forBoundedOutOfOrderness(Duration.ofDays(1))
            .withTimestampAssigner((row, previous) -> (Long) row.getField(2)));
    table.createTemporaryView("src", input, Schema.newBuilder()
        .column("k", DataTypes.BIGINT()).column("s", DataTypes.STRING()).column("ts", DataTypes.BIGINT())
        .columnByMetadata("rt", DataTypes.TIMESTAMP_LTZ(3), "rowtime")
        .watermark("rt", "SOURCE_WATERMARK()").build());
    return table;
  }
}
