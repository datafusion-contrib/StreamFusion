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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import tech.streamfusion.planner.NativePlanner;

/** Row-fed keep-first dedup with candidates pending until the bounded source's terminal watermark. */
@EnabledIfEnvironmentVariable(named = "SF_BENCHMARK", matches = "true")
class PendingDedupBenchmark {
  private static final long ROWS = Long.getLong("dedup.rows", 262_144L);
  private static final long KEYS = Long.getLong("dedup.keys", 16_384L);
  private static final int WIDTH = Integer.getInteger("dedup.width", 8);
  private static final int WARMUP = Integer.getInteger("dedup.warmup", 2);
  private static final int RUNS = Integer.getInteger("dedup.runs", 5);
  private static final String SQL =
      "INSERT INTO sink SELECT k, payload, rt FROM (SELECT *, ROW_NUMBER() OVER"
          + " (PARTITION BY k ORDER BY rt ASC) AS rn FROM src) WHERE rn = 1";

  @Test
  void pendingKeepFirst() throws Exception {
    if (ROWS <= 0 || KEYS <= 0 || WIDTH < 0 || WARMUP < 0 || RUNS <= 0)
      throw new IllegalArgumentException("Invalid pending dedup benchmark configuration");
    NativeParity.assertKindedParity(() -> environment(4096, 2048, WIDTH),
        SQL.substring("INSERT INTO sink ".length()));
    String plan = NativePlanner.explain(environment(), SQL);
    if (!plan.contains("NativeDeduplicate")
        || !plan.contains("RowDataToArrow")
        || !plan.contains("ArrowToRowData"))
      throw new IllegalStateException("Expected native dedup and both transposes: " + plan);
    double[][] times = new double[2][RUNS];
    for (int trial = 0; trial < WARMUP + RUNS; trial++) {
      for (int turn = 0; turn < 2; turn++) {
        int engine = (trial + turn) % 2;
        TableEnvironment table = environment();
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
    System.out.printf(Locale.ROOT,
        "[pending-dedup] rows=%d keys=%d bytes=%d watermark=terminal Flink=%.6fs Native=%.6fs"
            + " ratio=%.3fx host_trials=%s native_trials=%s%n",
        ROWS, KEYS, WIDTH, host, nativeTime, host / nativeTime,
        Arrays.toString(times[0]), Arrays.toString(times[1]));
  }

  private static double median(double[] values) {
    double[] sorted = values.clone();
    Arrays.sort(sorted);
    return sorted[sorted.length / 2];
  }

  private static TableEnvironment environment() {
    return environment(ROWS, KEYS, WIDTH);
  }

  private static TableEnvironment environment(long rows, long keys, int width) {
    var env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    var table = StreamTableEnvironment.create(env);
    table.getConfig().set("table.local-time-zone", "UTC");
    String suffix = "x".repeat(width);
    table.createTemporaryView("src",
        env.fromSequence(0, rows - 1)
            .map(i -> Row.of(i % keys, i % 7 == 0 ? null : i + "/" + suffix,
                1000L + (i / keys) % 3))
            .returns(Types.ROW_NAMED(new String[] {"k", "payload", "ts"},
                Types.LONG, Types.STRING, Types.LONG))
            .assignTimestampsAndWatermarks(WatermarkStrategy.<Row>noWatermarks()
                .withTimestampAssigner((row, previous) -> (Long) row.getField(2))),
        Schema.newBuilder().column("k", DataTypes.BIGINT())
            .column("payload", DataTypes.STRING()).column("ts", DataTypes.BIGINT())
            .columnByMetadata("rt", DataTypes.TIMESTAMP_LTZ(3), "rowtime")
            .watermark("rt", "SOURCE_WATERMARK()").build());
    table.executeSql("CREATE TABLE sink (k BIGINT, payload STRING, rt TIMESTAMP_LTZ(3))"
        + " WITH ('connector' = 'blackhole')");
    return table;
  }
}
