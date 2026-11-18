package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.stream.Stream;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
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

/** Complete row-source/collecting-sink jobs; preparation is excluded, both native transposes remain. */
@EnabledIfEnvironmentVariable(named = "SF_BENCHMARK", matches = "true")
class SharedWindowSumBenchmark {
  private static final long ROWS = Long.getLong("window.shared.rows", 1_000_000L);
  private static final int GROUPS = Integer.getInteger("window.shared.groups", 64);
  private static final int WARMUP = Integer.getInteger("window.shared.warmup", 2);
  private static final int RUNS = Integer.getInteger("window.shared.runs", 5);
  private static final boolean EXPECT_NATIVE = Boolean.parseBoolean(
      System.getProperty("window.shared.expectNative", "true"));
  private static final boolean COMPARE_FALLBACK = Boolean.getBoolean("window.shared.compareFallback");
  private static final String SQL = "SELECT k, window_start, window_end, COUNT(v), AVG(v), SUM(v) "
      + "FROM TABLE(TUMBLE(TABLE src, DESCRIPTOR(rt), INTERVAL '10' SECOND)) "
      + "GROUP BY k, window_start, window_end";

  static Stream<Arguments> fixtures() {
    return Arrays.stream(System.getProperty("window.shared.phases", "ONE_PHASE,TWO_PHASE").split(","))
        .flatMap(phase -> Arrays.stream(System.getProperty("window.shared.types", "BIGINT,DECIMAL").split(","))
            .map(type -> Arguments.of(phase, type)));
  }

  @ParameterizedTest
  @MethodSource("fixtures")
  void compareCompleteJobs(String phase, String type) throws Exception {
    if (ROWS <= 0 || GROUPS <= 0 || WARMUP < 1 || RUNS < 3) throw new IllegalArgumentException("invalid fixture");
    var witness = environment(phase, type);
    String valueType = type.equals("BIGINT") ? "BIGINT" : "DECIMAL(38,6)";
    witness.executeSql("CREATE TABLE sink (k BIGINT, window_start TIMESTAMP(3), window_end TIMESTAMP(3), "
        + "c BIGINT, av " + valueType + ", s " + (type.equals("BIGINT") ? "BIGINT" : "DECIMAL(38,2)")
        + ") WITH ('connector' = 'blackhole')");
    String plan = NativePlanner.explain(witness, "INSERT INTO sink " + SQL);
    String operator = phase.equals("ONE_PHASE") ? "NativeColumnarWindowAggregate" : "NativeColumnarGlobalWindowAggregate";
    assertEquals(EXPECT_NATIVE, plan.contains(operator), plan);
    if (EXPECT_NATIVE) {
      assertTrue(plan.contains("RowDataToArrow") && plan.contains("ArrowToRowData"), plan);
      if (phase.equals("TWO_PHASE")) assertTrue(plan.contains("NativeColumnarLocalWindowAggregate"), plan);
    }
    System.out.println("[shared-window-plan] " + phase + " " + type + " " + plan);
    int engines = COMPARE_FALLBACK ? 3 : 2;
    double[][] samples = new double[engines][RUNS];
    for (int trial = 0; trial < WARMUP + RUNS; trial++) {
      var outputs = new ArrayList<List<List<Object>>>();
      for (int engine = 0; engine < engines; engine++) outputs.add(null);
      for (int turn = 0; turn < engines; turn++) {
        int engine = (trial + turn) % engines;
        var table = environment(phase, type);
        if (engine == 2) {
          table.getConfig().set("streamfusion.operator.windowAggregate.enabled", "false");
          table.getConfig().set("streamfusion.operator.localWindowAggregate.enabled", "false");
          table.getConfig().set("streamfusion.operator.globalWindowAggregate.enabled", "false");
        }
        var scan = engine > 0 ? NativePlanner.install(table) : null;
        var output = new ArrayList<List<Object>>();
        long start = System.nanoTime();
        try (var rows = table.executeSql(SQL).collect()) {
          while (rows.hasNext()) {
            Row row = rows.next();
            var values = new ArrayList<Object>();
            values.add(row.getKind());
            for (int col = 0; col < row.getArity(); col++) values.add(row.getField(col));
            output.add(values);
          }
        }
        double seconds = (System.nanoTime() - start) / 1e9;
        outputs.set(engine, output);
        if (scan != null) {
          boolean nativeWindow = engine == 1 && EXPECT_NATIVE;
          assertEquals(nativeWindow, scan.fallbackReasons().isEmpty(), scan.explainSummary());
          if (nativeWindow) assertTrue(scan.substitutions() > 0, scan.explainSummary());
          else assertEquals(0, scan.substitutions(), scan.explainSummary());
        }
        if (trial >= WARMUP) samples[engine][trial - WARMUP] = seconds;
      }
      for (int engine = 1; engine < engines; engine++) {
        assertEquals(NativeParity.multiset(outputs.get(0)), NativeParity.multiset(outputs.get(engine)));
      }
    }
    double stock = median(samples[0]);
    double nativeTime = median(samples[1]);
    System.out.printf(Locale.ROOT,
        "[shared-window-job] phase=%s type=%s rows=%d groups=%d expected_native=%s Flink=%.6fs Native=%.6fs ratio=%.3fx stock_samples=%s native_samples=%s%n",
        phase, type, ROWS, GROUPS, EXPECT_NATIVE, stock, nativeTime, stock / nativeTime,
        Arrays.toString(samples[0]), Arrays.toString(samples[1]));
    if (COMPARE_FALLBACK) {
      double fallback = median(samples[2]);
      System.out.printf(Locale.ROOT,
          "[shared-window-fallback-control] phase=%s type=%s rows=%d groups=%d fallback=%.6fs fallback_over_native=%.3fx fallback_samples=%s%n",
          phase, type, ROWS, GROUPS, fallback, fallback / nativeTime, Arrays.toString(samples[2]));
    }
  }

  private static double median(double[] samples) {
    double[] sorted = samples.clone();
    Arrays.sort(sorted);
    return sorted[sorted.length / 2];
  }

  private static TableEnvironment environment(String phase, String type) {
    var env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(2);
    var table = StreamTableEnvironment.create(env);
    table.getConfig().setLocalTimeZone(java.time.ZoneOffset.UTC);
    table.getConfig().set("table.optimizer.agg-phase-strategy", phase);
    boolean decimal = type.equals("DECIMAL");
    var input = env.fromSequence(0, ROWS - 1).map(id -> Row.of(id % GROUPS,
        id % GROUPS == 0 || id % 7 == 0 ? null : decimal
            ? BigDecimal.valueOf(id % 1024 - 512, 2) : id % 1024 - 512,
        1000L + id % 10000))
        .returns(Types.ROW_NAMED(new String[] {"k", "v", "ts"}, Types.LONG,
            decimal ? Types.BIG_DEC : Types.LONG, Types.LONG))
        .assignTimestampsAndWatermarks(WatermarkStrategy.<Row>forBoundedOutOfOrderness(Duration.ofDays(1))
            .withTimestampAssigner((row, previous) -> (Long) row.getField(2)));
    table.createTemporaryView("src", input, Schema.newBuilder()
        .column("k", DataTypes.BIGINT()).column("v", decimal ? DataTypes.DECIMAL(12, 2) : DataTypes.BIGINT())
        .column("ts", DataTypes.BIGINT()).columnByMetadata("rt", DataTypes.TIMESTAMP_LTZ(3), "rowtime")
        .watermark("rt", "SOURCE_WATERMARK()").build());
    return table;
  }
}
