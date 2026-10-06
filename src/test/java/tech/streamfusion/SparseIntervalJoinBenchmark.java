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

/** Complete row-fed interval joins with a large key domain and selective opposite-side probes. */
@EnabledIfEnvironmentVariable(named = "SF_BENCHMARK", matches = "true")
class SparseIntervalJoinBenchmark {
  private static final long ROWS = Long.getLong("interval.rows", 65_536L);
  private static final long KEYS = Long.getLong("interval.keys", 16_384L);
  private static final long PROBES = Long.getLong("interval.probes", 4_096L);
  private static final long MATCH_KEYS = Long.getLong("interval.match-keys", 256L);
  private static final int WIDTH = Integer.getInteger("interval.width", 264);
  private static final int WARMUP = Integer.getInteger("interval.warmup", 2);
  private static final int RUNS = Integer.getInteger("interval.runs", 5);
  private static final String SELECT = "SELECT a.k, a.payload, b.payload FROM A AS a JOIN B AS b"
      + " ON a.k = b.k AND a.rt BETWEEN b.rt - INTERVAL '1' SECOND"
      + " AND b.rt + INTERVAL '1' SECOND";
  private static final String SQL = "INSERT INTO sink " + SELECT;

  @Test
  void sparseIntervalJoin() throws Exception {
    if (ROWS <= 0 || KEYS <= 0 || PROBES <= 0 || MATCH_KEYS <= 0 || MATCH_KEYS > KEYS
        || WIDTH < 0 || WARMUP < 0 || RUNS <= 0)
      throw new IllegalArgumentException("Invalid sparse interval join configuration");
    NativeParity.assertParity(() -> environment(1024, 256, 64, 8, WIDTH), SELECT);
    String plan = NativePlanner.explain(environment(), SQL);
    if (!plan.contains("NativeIntervalJoin")
        || !plan.contains("RowDataToArrow")
        || !plan.contains("ArrowToRowData"))
      throw new IllegalStateException("Expected native interval join and both transposes: " + plan);
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
        "[sparse-interval] rows=%d keys=%d probes=%d match_keys=%d bytes=%d watermark=terminal"
            + " Flink=%.6fs Native=%.6fs ratio=%.3fx host_trials=%s native_trials=%s%n",
        ROWS, KEYS, PROBES, MATCH_KEYS, WIDTH, host, nativeTime, host / nativeTime,
        Arrays.toString(times[0]), Arrays.toString(times[1]));
  }

  private static double median(double[] values) {
    double[] sorted = values.clone();
    Arrays.sort(sorted);
    return sorted[sorted.length / 2];
  }

  private static TableEnvironment environment() {
    return environment(ROWS, KEYS, PROBES, MATCH_KEYS, WIDTH);
  }

  private static TableEnvironment environment(long rows, long keys, long probes, long matchKeys, int width) {
    var env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    var table = StreamTableEnvironment.create(env);
    table.getConfig().set("table.local-time-zone", "UTC");
    String suffix = "x".repeat(width);
    Schema schema = Schema.newBuilder().column("k", DataTypes.BIGINT())
        .column("payload", DataTypes.STRING()).column("ts", DataTypes.BIGINT())
        .columnByMetadata("rt", DataTypes.TIMESTAMP_LTZ(3), "rowtime")
        .watermark("rt", "SOURCE_WATERMARK()").build();
    for (boolean right : new boolean[] {false, true}) {
      table.createTemporaryView(right ? "B" : "A",
          env.fromSequence(0, (right ? probes : rows) - 1)
              .map(i -> Row.of(right ? (i % matchKeys) * (keys / matchKeys) : i % keys,
                  i % 7 == 0 ? null : (right ? "right/" : "left/") + i + "/" + suffix, 1000L))
              .returns(Types.ROW_NAMED(new String[] {"k", "payload", "ts"},
                  Types.LONG, Types.STRING, Types.LONG))
              .assignTimestampsAndWatermarks(WatermarkStrategy.<Row>noWatermarks()
                  .withTimestampAssigner((row, previous) -> (Long) row.getField(2))), schema);
    }
    table.executeSql("CREATE TABLE sink (k BIGINT, left_payload STRING, right_payload STRING)"
        + " WITH ('connector' = 'blackhole')");
    return table;
  }
}
