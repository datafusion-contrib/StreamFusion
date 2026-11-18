package tech.streamfusion;

import java.time.Duration;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.Locale;
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
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.planner.NativePlanner;

@EnabledIfEnvironmentVariable(named = "SF_BENCHMARK", matches = "true")
class StringWindowBenchmark {
  private static final long ROWS = Long.getLong("stringWindow.rows", 1_000_000L);
  private static final int KEYS = Integer.getInteger("stringWindow.keys", 64);
  private static final int WIDTH = Integer.getInteger("stringWindow.width", 64);
  private static final int WARMUP = Integer.getInteger("stringWindow.warmup", 2);
  private static final int RUNS = Integer.getInteger("stringWindow.runs", 5);
  private static final String SHAPE = System.getProperty("stringWindow.shape", "CUMULATE");
  private static final boolean EXPECT_NATIVE =
      Boolean.parseBoolean(System.getProperty("stringWindow.expectNative", "true"));

  @ParameterizedTest
  @ValueSource(strings = {"ONE_PHASE", "TWO_PHASE"})
  void stringExtrema(String phase) throws Exception {
    String window;
    switch (SHAPE) {
      case "TUMBLE":
        window = "TUMBLE(TABLE src, DESCRIPTOR(rt), INTERVAL '10' SECOND)";
        break;
      case "HOP":
        window = "HOP(TABLE src, DESCRIPTOR(rt), INTERVAL '2' SECOND, INTERVAL '10' SECOND)";
        break;
      case "CUMULATE":
        window = "CUMULATE(TABLE src, DESCRIPTOR(rt), INTERVAL '2' SECOND, INTERVAL '10' SECOND)";
        break;
      default:
        throw new IllegalArgumentException("Unknown window shape: " + SHAPE);
    }
    String sql =
        "INSERT INTO sink SELECT k, MIN(v), MAX(v), COUNT(*) FROM TABLE("
            + window
            + ") GROUP BY k, window_start, window_end";
    String plan = NativePlanner.explain(environment(phase), sql);
    boolean windowNative =
        plan.contains(
            phase.equals("TWO_PHASE")
                ? "NativeColumnarGlobalWindowAggregate"
                : "NativeColumnarWindowAggregate");
    if (windowNative != EXPECT_NATIVE
        || (EXPECT_NATIVE
            && (!plan.contains("RowDataToArrow")
                || !plan.contains("ArrowToRowData")
                || (phase.equals("TWO_PHASE")
                    && !plan.contains("NativeColumnarLocalWindowAggregate"))))) {
      throw new IllegalStateException("Unexpected window execution path: " + plan);
    }
    System.out.println("[string-window-plan] " + plan);
    double[][] times = new double[2][RUNS];
    for (int trial = 0; trial < WARMUP + RUNS; trial++) {
      for (int turn = 0; turn < 2; turn++) {
        int engine = (trial + turn) % 2;
        var table = environment(phase);
        var scan = engine == 1 ? NativePlanner.install(table) : null;
        long start = System.nanoTime();
        table.executeSql(sql).await();
        double seconds = (System.nanoTime() - start) / 1e9;
        if (EXPECT_NATIVE && scan != null && scan.substitutions() == 0) {
          throw new IllegalStateException(scan.explainSummary());
        }
        if (trial >= WARMUP) times[engine][trial - WARMUP] = seconds;
      }
    }
    double host = median(times[0]);
    double nativeTime = median(times[1]);
    System.out.printf(
        Locale.ROOT,
        "[string-window] revision=%s shape=%s phase=%s rows=%d keys=%d width=%d "
            + "Flink=%.6fs StreamFusion=%.6fs ratio=%.3fx host_trials=%s native_trials=%s%n",
        EXPECT_NATIVE ? "candidate" : "previous",
        SHAPE,
        phase,
        ROWS,
        KEYS,
        WIDTH,
        host,
        nativeTime,
        host / nativeTime,
        Arrays.toString(times[0]),
        Arrays.toString(times[1]));
  }

  private static double median(double[] values) {
    double[] sorted = values.clone();
    Arrays.sort(sorted);
    return sorted[sorted.length / 2];
  }

  private static TableEnvironment environment(String phase) {
    var env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(2);
    var table = StreamTableEnvironment.create(env);
    table.getConfig().setLocalTimeZone(ZoneOffset.UTC);
    table.getConfig().set("table.optimizer.agg-phase-strategy", phase);
    if (phase.equals("ONE_PHASE") && EXPECT_NATIVE) {
      table.getConfig().set("streamfusion.window.string-extrema.single-phase.enabled", "true");
    }
    table.getConfig().set("table.exec.mini-batch.enabled", "true");
    table.getConfig().set("table.exec.mini-batch.allow-latency", "1 s");
    table.getConfig().set("table.exec.mini-batch.size", "4096");
    var input =
        env.fromSequence(0, ROWS - 1)
            .map(
                id -> {
                  long scrambled = id * 0x9e3779b97f4a7c15L;
                  String value = Long.toUnsignedString(scrambled, 36);
                  value += "x".repeat(Math.max(0, WIDTH - value.length()));
                  return Row.of(id % KEYS, id % 7 == 0 ? null : value, 1000L + id % 10000);
                })
            .returns(
                Types.ROW_NAMED(
                    new String[] {"k", "v", "ts"}, Types.LONG, Types.STRING, Types.LONG))
            .assignTimestampsAndWatermarks(
                WatermarkStrategy.<Row>forBoundedOutOfOrderness(Duration.ofDays(1))
                    .withTimestampAssigner((row, previous) -> (Long) row.getField(2)));
    table.createTemporaryView(
        "src",
        input,
        Schema.newBuilder()
            .column("k", DataTypes.BIGINT())
            .column("v", DataTypes.STRING())
            .column("ts", DataTypes.BIGINT())
            .columnByMetadata("rt", DataTypes.TIMESTAMP_LTZ(3), "rowtime")
            .watermark("rt", "SOURCE_WATERMARK()")
            .build());
    table.executeSql(
        "CREATE TABLE sink (k BIGINT, mn STRING, mx STRING, cnt BIGINT) "
            + "WITH ('connector' = 'blackhole')");
    return table;
  }
}
