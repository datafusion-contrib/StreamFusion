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
class RetractingWindowBenchmark {
  private static final long ROWS = Long.getLong("window.rows", 1_000_000L);
  private static final int WARMUP = Integer.getInteger("window.warmup", 2);
  private static final int RUNS = Integer.getInteger("window.runs", 5);
  private static final boolean GROUPING_ONLY = Boolean.getBoolean("window.groupingOnly");
  private static final boolean FILTERED = Boolean.getBoolean("window.filtered");
  private static final String FILTER = FILTERED ? " FILTER (WHERE MOD(v, 2) = 0)" : "";
  private static final boolean AVERAGE = Boolean.getBoolean("window.average");
  private static final String AVERAGE_TYPE = System.getProperty("window.averageType", "BIGINT");
  private static final String AVERAGE_RESULT_TYPE = averageResultType();
  private static final String SUM_TYPE = System.getProperty("window.sumType", "BIGINT");
  private static final String SUM_RESULT_TYPE =
      SUM_TYPE.replaceFirst("DECIMAL\\(\\d+,", "DECIMAL(38,");
  private static final String SQL =
      "INSERT INTO sink SELECT k"
          + (GROUPING_ONLY
              ? ""
              : AVERAGE
                  ? ", COUNT(v)" + FILTER + ", AVG(CAST(v AS " + AVERAGE_TYPE + "))" + FILTER
                  : ", COUNT(v)" + FILTER + ", SUM(CAST(v AS " + SUM_TYPE + "))" + FILTER)
          + " FROM TABLE(HOP(TABLE ranked, DESCRIPTOR(rt), "
          + "INTERVAL '2' SECOND, INTERVAL '10' SECOND)) GROUP BY k, window_start, window_end";

  @ParameterizedTest
  @ValueSource(strings = {"ONE_PHASE", "TWO_PHASE"})
  void retractingWindow(String phase) throws Exception {
    String plan = NativePlanner.explain(environment(phase), SQL);
    if (!plan.contains("NativeColumnarTopN")
        || !plan.contains(
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
        if (scan != null && scan.substitutions() == 0)
          throw new IllegalStateException(scan.explainSummary());
        if (trial >= WARMUP) times[engine][trial - WARMUP] = seconds;
      }
    }
    double host = median(times[0]);
    double nativeTime = median(times[1]);
    System.out.printf(
        Locale.ROOT,
        "[retracting-window] groupingOnly=%s filtered=%s average=%s averageType=%s sumType=%s"
            + " phase=%s rows=%d Flink=%.6fs Native=%.6fs ratio=%.3fx host_trials=%s"
            + " native_trials=%s%n",
        GROUPING_ONLY,
        FILTERED,
        AVERAGE,
        AVERAGE_TYPE,
        SUM_TYPE,
        phase,
        ROWS,
        host,
        nativeTime,
        host / nativeTime,
        Arrays.toString(times[0]),
        Arrays.toString(times[1]));
  }

  private static String averageResultType() {
    if (!AVERAGE_TYPE.startsWith("DECIMAL(")) return AVERAGE_TYPE;
    int scale =
        Integer.parseInt(
            AVERAGE_TYPE
                .substring(AVERAGE_TYPE.indexOf(',') + 1, AVERAGE_TYPE.length() - 1)
                .trim());
    return "DECIMAL(38," + Math.max(6, scale) + ")";
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
    var input =
        env.fromSequence(0, ROWS - 1)
            .map(id -> Row.of(id % 64, id % 13 == 0 ? null : id, 1000L + id % 10000))
            .returns(
                Types.ROW_NAMED(new String[] {"k", "v", "ts"}, Types.LONG, Types.LONG, Types.LONG))
            .assignTimestampsAndWatermarks(
                WatermarkStrategy.<Row>forBoundedOutOfOrderness(java.time.Duration.ofDays(1))
                    .withTimestampAssigner((row, previous) -> (Long) row.getField(2)));
    table.createTemporaryView(
        "src",
        input,
        Schema.newBuilder()
            .column("k", DataTypes.BIGINT())
            .column("v", DataTypes.BIGINT())
            .column("ts", DataTypes.BIGINT())
            .columnByMetadata("rt", DataTypes.TIMESTAMP_LTZ(3), "rowtime")
            .watermark("rt", "SOURCE_WATERMARK()")
            .build());
    table.createTemporaryView(
        "ranked",
        table.sqlQuery(
            "SELECT k, v, rt FROM (SELECT k, v, rt, ROW_NUMBER() OVER (PARTITION BY k ORDER BY v"
                + " DESC) AS rn FROM src) WHERE rn = 1"));
    table.executeSql(
        "CREATE TABLE sink (k BIGINT"
            + (GROUPING_ONLY
                ? ""
                : ", c BIGINT, s " + (AVERAGE ? AVERAGE_RESULT_TYPE : SUM_RESULT_TYPE))
            + ") WITH ('connector' = 'blackhole')");
    return table;
  }
}
