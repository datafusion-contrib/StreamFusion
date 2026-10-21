package tech.streamfusion;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Arrays;
import java.util.Locale;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import tech.streamfusion.planner.NativePlanner;

@EnabledIfEnvironmentVariable(named = "SF_BENCHMARK", matches = "true")
class DistinctAggregateBenchmark {
  private static final long ROWS = Long.getLong("distinct.rows", 2_000_000L);
  private static final int KEYS = Integer.getInteger("distinct.keys", 64);
  private static final int WARMUP = Integer.getInteger("distinct.warmup", 2);
  private static final int RUNS = Integer.getInteger("distinct.runs", 5);
  private static final boolean FLOATING = Boolean.getBoolean("distinct.floating");
  private static final boolean FLOATING_SUM = Boolean.getBoolean("distinct.floatingSum");
  private static final boolean AVERAGE = FLOATING || Boolean.getBoolean("distinct.average");
  private static final boolean EXPECT_NATIVE =
      Boolean.parseBoolean(System.getProperty("distinct.expectNative", "true"));
  private static final boolean TWO_PHASE =
      Boolean.parseBoolean(System.getProperty("distinct.twoPhase", Boolean.toString(!AVERAGE)));
  private static final int DECIMAL_PRECISION =
      Integer.getInteger("distinct.precision", TWO_PHASE ? 19 : 20);
  private static final String SQL =
      FLOATING
          ? "INSERT INTO sink SELECT k, AVG(DISTINCT CAST(t AS FLOAT)),"
              + " AVG(DISTINCT CAST(b AS DOUBLE))"
              + (FLOATING_SUM
                  ? ", SUM(DISTINCT CAST(t AS FLOAT)), SUM(DISTINCT CAST(b AS DOUBLE))"
                  : "")
              + " FROM src GROUP BY k"
          : AVERAGE
              ? "INSERT INTO sink SELECT k, AVG(DISTINCT t), AVG(DISTINCT b), AVG(DISTINCT amount)"
                    + " FROM src GROUP BY k"
              : "INSERT INTO sink SELECT k, COUNT(DISTINCT b), COUNT(DISTINCT ts),"
                  + " SUM(DISTINCT amount) FROM src GROUP BY k";

  @Test
  void groupedDistinctTypes() throws Exception {
    String plan = NativePlanner.explain(environment(), SQL);
    if (EXPECT_NATIVE && (!plan.contains("NativeColumnarGroupAggregate")
        || !plan.contains("RowDataToArrow")
        || !plan.contains("ArrowToRowData")
        || TWO_PHASE && !plan.contains("NativeColumnarLocalGroupAggregate"))) {
      throw new IllegalStateException("Expected native aggregation and both transposes: " + plan);
    }
    if (!EXPECT_NATIVE && plan.contains("NativeColumnarGroupAggregate")) {
      throw new IllegalStateException("Expected previous aggregate fallback: " + plan);
    }
    double[][] times = new double[2][RUNS];
    for (int trial = 0; trial < WARMUP + RUNS; trial++) {
      for (int turn = 0; turn < 2; turn++) {
        int engine = (trial + turn) % 2;
        try (var metrics = new RecoveryExecutionMetrics()) {
          var table = environment(metrics);
          var scan = engine == 1 ? NativePlanner.install(table) : null;
          long start = System.nanoTime();
          var result = table.executeSql(SQL);
          String jobId = result.getJobClient().orElseThrow().getJobID().toString();
          result.await();
          double seconds = (System.nanoTime() - start) / 1e9;
          if (scan != null && EXPECT_NATIVE && scan.substitutions() == 0) {
            throw new IllegalStateException("Unexpected fallback: " + scan.fallbackReasons());
          }
          if (scan != null && !EXPECT_NATIVE && scan.substitutions() != 0) {
            throw new IllegalStateException(
                "Expected previous aggregate fallback: " + scan.explainSummary());
          }
          if (scan != null && EXPECT_NATIVE) {
            var observations =
                metrics.nativeOperators().stream()
                    .filter(row -> jobId.equals(row.get("jobId")))
                    .collect(java.util.stream.Collectors.toList());
            RecoveryExecutionMetrics.assertConsumed(observations, "NativeColumnarGroupAggregate");
            if (TWO_PHASE)
              RecoveryExecutionMetrics.assertConsumed(
                  observations, "NativeColumnarLocalGroupAggregate");
          }
          if (trial >= WARMUP) times[engine][trial - WARMUP] = seconds;
          System.out.printf(
              Locale.ROOT,
              "[distinct-aggregate-trial] floating=%s engine=%d trial=%d warmup=%s seconds=%.6f%n",
              FLOATING,
              engine,
              trial,
              trial < WARMUP,
              seconds);
        }
      }
    }
    System.out.printf(
        Locale.ROOT,
        "[distinct-aggregate] floating=%s average=%s two_phase=%s precision=%d rows=%d keys=%d"
            + " Flink=%.6fs Native=%.6fs flink_trials=%s native_trials=%s%n",
        FLOATING,
        AVERAGE,
        TWO_PHASE,
        DECIMAL_PRECISION,
        ROWS,
        KEYS,
        median(times[0]),
        median(times[1]),
        Arrays.toString(times[0]),
        Arrays.toString(times[1]));
  }

  private static double median(double[] values) {
    double[] sorted = values.clone();
    Arrays.sort(sorted);
    return sorted[sorted.length / 2];
  }

  private static TableEnvironment environment() {
    return environment(null);
  }

  private static TableEnvironment environment(RecoveryExecutionMetrics metrics) {
    var env = metrics == null ? StreamExecutionEnvironment.getExecutionEnvironment()
        : metrics.environment(new org.apache.flink.configuration.Configuration(), 1, 0);
    env.setParallelism(1);
    var table = StreamTableEnvironment.create(env);
    table
        .getConfig()
        .setIdleStateRetention(
            java.time.Duration.ofMillis(Long.getLong("distinct.retentionMillis", 0L)));
    table
        .getConfig()
        .set("table.optimizer.agg-phase-strategy", TWO_PHASE ? "TWO_PHASE" : "ONE_PHASE");
    table.getConfig().set("table.exec.mini-batch.enabled", Boolean.toString(TWO_PHASE));
    table.getConfig().set("table.exec.mini-batch.allow-latency", "1 h");
    table.getConfig().set("table.exec.mini-batch.size", "1024");
    table.createTemporaryView(
        "src",
        env.fromSequence(0, ROWS - 1)
            .map(
                i -> {
                  boolean missing = i % 7 == 0;
                  BigDecimal amount = missing ? null : BigDecimal.valueOf(i / KEYS % 128 - 64, 2);
                  return AVERAGE
                      ? Row.of(
                          (int) (i % KEYS),
                          missing ? null : (byte) (i / KEYS % 127 - 64),
                          missing ? null : i / KEYS % 1024 - 512,
                          amount)
                      : Row.of(
                          (int) (i % KEYS),
                          missing ? null : (i / KEYS) % 2 == 0,
                          missing ? null : Instant.ofEpochSecond(0, i / KEYS % 128),
                          amount);
                })
            .returns(
                AVERAGE
                    ? Types.ROW_NAMED(
                        new String[] {"k", "t", "b", "amount"},
                        Types.INT,
                        Types.BYTE,
                        Types.LONG,
                        Types.BIG_DEC)
                    : Types.ROW_NAMED(
                        new String[] {"k", "b", "ts", "amount"},
                        Types.INT,
                        Types.BOOLEAN,
                        Types.INSTANT,
                        Types.BIG_DEC)),
        AVERAGE
            ? Schema.newBuilder()
                .column("k", DataTypes.INT())
                .column("t", DataTypes.TINYINT())
                .column("b", DataTypes.BIGINT())
                .column("amount", DataTypes.DECIMAL(DECIMAL_PRECISION, 2))
                .build()
            : Schema.newBuilder()
                .column("k", DataTypes.INT())
                .column("b", DataTypes.BOOLEAN())
                .column("ts", DataTypes.TIMESTAMP_LTZ(9))
                .column("amount", DataTypes.DECIMAL(DECIMAL_PRECISION, 2))
                .build());
    table.executeSql(
        (FLOATING
                ? "CREATE TABLE sink (k INT, av_t FLOAT, ab DOUBLE"
                    + (FLOATING_SUM ? ", st FLOAT, sb DOUBLE" : "")
                    + ")"
                : AVERAGE
                    ? "CREATE TABLE sink (k INT, av_t TINYINT, ab BIGINT, ad DECIMAL(38,6))"
                    : "CREATE TABLE sink (k INT, cb BIGINT, ct BIGINT, sa DECIMAL(38,2))")
            + " WITH ('connector' = 'blackhole')");
    return table;
  }
}
