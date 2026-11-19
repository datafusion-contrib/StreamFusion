package tech.streamfusion;

import java.time.Duration;
import java.util.Arrays;
import java.util.Locale;
import java.util.stream.Collectors;
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

@EnabledIfEnvironmentVariable(named = "SF_BENCHMARK", matches = "true")
class WindowRankBenchmark {
  private static final long ROWS = Long.getLong("rank.rows", 2_000_000L);
  private static final long KEYS = Long.getLong("rank.keys", 8L);
  private static final int WIDTH = Integer.getInteger("rank.width", 264);
  private static final int LIMIT = Integer.getInteger("rank.limit", 1);
  private static final boolean NULLABLE = Boolean.getBoolean("rank.nullable");
  private static final int WARMUP = Integer.getInteger("rank.warmup", 2);
  private static final int RUNS = Integer.getInteger("rank.runs", 5);

  @Test
  void compareWindowRanking() throws Exception {
    if (ROWS <= 0 || KEYS <= 0 || WIDTH < 0 || LIMIT <= 0 || WARMUP < 0 || RUNS <= 0) {
      throw new IllegalArgumentException("Invalid window ranking benchmark dimensions");
    }
    TableEnvironment check = environment();
    String plan = NativePlanner.explain(check, prepare(check));
    for (String node : new String[] {"NativeWindowRank", "RowDataToArrow", "ArrowToRowData"}) {
      if (!plan.contains(node)) {
        throw new IllegalStateException("Missing " + node + ": " + plan);
      }
    }
    System.out.println("[window-rank-plan] " + plan);
    NativeParity.assertKindedParity(WindowRankBenchmark::environment, query());
    if (Boolean.getBoolean("rank.verifyOnly")) {
      return;
    }
    double[][] seconds = new double[2][RUNS];
    for (int trial = 0; trial < WARMUP + RUNS; trial++) {
      for (int turn = 0; turn < 2; turn++) {
        int engine = (trial + turn) % 2;
        TableEnvironment table = environment();
        String sql = prepare(table);
        var scan = engine == 1 ? NativePlanner.install(table) : null;
        long start = System.nanoTime();
        table.executeSql(sql).await();
        double elapsed = (System.nanoTime() - start) / 1e9;
        if (scan != null && scan.substitutions() == 0) {
          throw new IllegalStateException("Unexpected fallback: " + scan.fallbackReasons());
        }
        if (trial >= WARMUP) {
          seconds[engine][trial - WARMUP] = elapsed;
        }
      }
    }
    System.out.printf(
        Locale.ROOT,
        "[window-rank] rows=%d keys=%d width=%d limit=%d nullable=%s Flink=%.6fs Native=%.6fs"
            + " flink_trials=%s native_trials=%s%n",
        ROWS,
        KEYS,
        WIDTH,
        LIMIT,
        NULLABLE,
        median(seconds[0]),
        median(seconds[1]),
        Arrays.toString(seconds[0]),
        Arrays.toString(seconds[1]));
  }

  private static String query() {
    return "SELECT k, v, payload, window_start, window_end, rn FROM (SELECT *, "
        + "ROW_NUMBER() OVER (PARTITION BY window_start, window_end, k "
        + "ORDER BY v ASC NULLS LAST) AS rn FROM TABLE(TUMBLE(TABLE src, "
        + "DESCRIPTOR(rt), INTERVAL '1' SECOND))) WHERE rn <= "
        + LIMIT;
  }

  private static String prepare(TableEnvironment table) {
    String query = query();
    String columns =
        table.sqlQuery(query).getResolvedSchema().getColumns().stream()
            .map(
                c ->
                    "`"
                        + c.getName()
                        + "` "
                        + c.getDataType().getLogicalType().asSerializableString())
            .collect(Collectors.joining(", "));
    table.executeSql("CREATE TABLE sink (" + columns + ") WITH ('connector' = 'blackhole')");
    return "INSERT INTO sink " + query;
  }

  private static double median(double[] values) {
    double[] sorted = values.clone();
    Arrays.sort(sorted);
    return sorted[sorted.length / 2];
  }

  private static TableEnvironment environment() {
    var env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    env.getConfig().setAutoWatermarkInterval(0);
    var table = StreamTableEnvironment.create(env);
    table.getConfig().set("table.local-time-zone", "UTC");
    table.getConfig().set("table.exec.sink.rowtime-inserter", "DISABLED");
    String suffix = "中\0" + "x".repeat(WIDTH);
    table.createTemporaryView(
        "src",
        env.fromSequence(0, ROWS - 1)
            .map(
                i ->
                    Row.of(
                        i % KEYS,
                        NULLABLE && i % 7 == 0 ? null : i % 5,
                        NULLABLE && i % 11 == 0 ? null : i + "/" + suffix,
                        ((i / KEYS + i) % 2) * 1000 + 100))
            .returns(
                Types.ROW_NAMED(
                    new String[] {"k", "v", "payload", "ts"},
                    Types.LONG,
                    Types.LONG,
                    Types.STRING,
                    Types.LONG))
            .assignTimestampsAndWatermarks(
                WatermarkStrategy.<Row>forBoundedOutOfOrderness(Duration.ofDays(1))
                    .withTimestampAssigner((row, previous) -> (Long) row.getField(3))),
        Schema.newBuilder()
            .column("k", DataTypes.BIGINT())
            .column("v", DataTypes.BIGINT())
            .column("payload", DataTypes.STRING())
            .column("ts", DataTypes.BIGINT())
            .columnByMetadata("rt", DataTypes.TIMESTAMP_LTZ(3), "rowtime")
            .watermark("rt", "SOURCE_WATERMARK()")
            .build());
    return table;
  }
}
