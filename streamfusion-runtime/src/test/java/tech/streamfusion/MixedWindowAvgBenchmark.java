package tech.streamfusion;

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
class MixedWindowAvgBenchmark {
  private static final long ROWS = Long.getLong("window.rows", 1_000_000L);
  private static final int WARMUP = Integer.getInteger("window.warmup", 2);
  private static final int RUNS = Integer.getInteger("window.runs", 5);
  private static final boolean FILTERED_EXTREMA = Boolean.getBoolean("window.filteredExtrema");
  private static final boolean FILTERED_DISTINCT = Boolean.getBoolean("window.filteredDistinct");
  private static final boolean DISTINCT = Boolean.getBoolean("window.distinct");
  private static final String SQL =
      "INSERT INTO sink SELECT k, "
          + (FILTERED_DISTINCT
              ? "COUNT(DISTINCT v) FILTER (WHERE MOD(v, 3) = 0)"
              : DISTINCT ? "COUNT(DISTINCT v)" : "COUNT(*)")
          + ", AVG(v), SUM(v), "
          + (FILTERED_EXTREMA
              ? "MIN(v) FILTER (WHERE MOD(v, 2) = 0), MAX(v) FILTER (WHERE MOD(v, 3) = 0)"
              : "MIN(v), MAX(v)")
          + ", AVG(i) FROM TABLE(HOP(TABLE src, DESCRIPTOR(rt), "
          + "INTERVAL '2' SECOND, INTERVAL '10' SECOND)) GROUP BY k, window_start, window_end";

  @ParameterizedTest
  @ValueSource(strings = {"ONE_PHASE", "TWO_PHASE"})
  void mixedWindowAvg(String phase) throws Exception {
    String plan = NativePlanner.explain(environment(phase), SQL);
    if (!plan.contains(
            phase.equals("TWO_PHASE")
                ? "NativeColumnarGlobalWindowAggregate"
                : "NativeColumnarWindowAggregate")
        || !plan.contains("RowDataToArrow")
        || !plan.contains("ArrowToRowData")) {
      throw new IllegalStateException(
          "Expected native window aggregate and both transposes: " + plan);
    }
    double[][] times = new double[2][RUNS];
    for (int trial = 0; trial < WARMUP + RUNS; trial++) {
      for (int turn = 0; turn < 2; turn++) {
        int engine = (trial + turn) % 2;
        var table = environment(phase);
        var scan = engine == 1 ? NativePlanner.install(table) : null;
        long start = System.nanoTime();
        table.executeSql(SQL).await();
        double seconds = (System.nanoTime() - start) / 1e9;
        if (scan != null && scan.substitutions() == 0) throw new IllegalStateException(scan.explainSummary());
        if (trial >= WARMUP) times[engine][trial - WARMUP] = seconds;
      }
    }
    double host = median(times[0]);
    double nativeTime = median(times[1]);
    System.out.printf(
        Locale.ROOT,
        "[mixed-window-avg] distinct=%s filteredExtrema=%s filteredDistinct=%s phase=%s rows=%d"
            + " Flink=%.6fs Native=%.6fs ratio=%.3fx host_trials=%s native_trials=%s%n",
        DISTINCT,
        FILTERED_EXTREMA,
        FILTERED_DISTINCT,
        phase,
        ROWS,
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
    table.getConfig().setLocalTimeZone(java.time.ZoneOffset.UTC);
    table.getConfig().set("table.optimizer.agg-phase-strategy", phase);
    var input = env.fromSequence(0, ROWS - 1)
        .map(id -> Row.of(id % 64, id % 5 == 0 ? null : id % 1024,
            id % 7 == 0 ? null : (int) (id % 101), 1000L + id % 10000))
        .returns(Types.ROW_NAMED(new String[] {"k", "v", "i", "ts"},
            Types.LONG, Types.LONG, Types.INT, Types.LONG))
        .assignTimestampsAndWatermarks(WatermarkStrategy.<Row>forBoundedOutOfOrderness(java.time.Duration.ofDays(1))
            .withTimestampAssigner((row, previous) -> (Long) row.getField(3)));
    table.createTemporaryView(
        "src",
        input,
        Schema.newBuilder()
            .column("k", DataTypes.BIGINT())
            .column("v", DataTypes.BIGINT())
            .column("i", DataTypes.INT())
            .column("ts", DataTypes.BIGINT())
            .columnByMetadata("rt", DataTypes.TIMESTAMP_LTZ(3), "rowtime")
            .watermark("rt", "SOURCE_WATERMARK()")
            .build());
    table.executeSql(
        "CREATE TABLE sink (k BIGINT, c BIGINT, av BIGINT, s BIGINT, mn BIGINT, mx BIGINT, ai INT) "
            + "WITH ('connector' = 'blackhole')");
    return table;
  }
}
