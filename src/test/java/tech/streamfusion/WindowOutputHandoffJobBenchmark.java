package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Duration;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Locale;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.planner.NativePlanner;

/** High-output-cardinality complete jobs, including both transposes and the collecting sink. */
@EnabledIfEnvironmentVariable(named = "SF_BENCHMARK", matches = "true")
class WindowOutputHandoffJobBenchmark {
  private static final long ROWS = Long.getLong("handoff.job.rows", 262144L);
  private static final int GROUPS = Integer.getInteger("handoff.job.groups", 65536);
  private static final int WARMUP = Integer.getInteger("handoff.job.warmup", 2);
  private static final int RUNS = Integer.getInteger("handoff.job.runs", 5);
  private static final int PROPERTIES = Integer.getInteger("handoff.job.properties", 2);
  private static final String SQL = PROPERTIES == 0
      ? "SELECT k, COUNT(*), SUM(v) FROM src GROUP BY k, TUMBLE(rt, INTERVAL '1' SECOND)"
      : "SELECT k, COUNT(*), SUM(v), window_start, window_end "
      + "FROM TABLE(TUMBLE(TABLE src, DESCRIPTOR(rt), INTERVAL '1' SECOND)) "
      + "GROUP BY k, window_start, window_end";

  @ParameterizedTest
  @ValueSource(strings = {"BIGINT", "STRING"})
  void compareCompleteWindowJobs(String keyType) throws Exception {
    if (PROPERTIES != 0 && PROPERTIES != 2) throw new IllegalArgumentException("properties must be 0 or 2");
    if (ROWS < GROUPS || GROUPS <= 0) throw new IllegalArgumentException("rows must cover all groups");
    String plan = NativePlanner.explain(environment(keyType), "INSERT INTO handoff_witness " + SQL);
    for (String node : new String[] {"NativeColumnarWindowAggregate", "RowDataToArrow", "ArrowToRowData"}) {
      if (!plan.contains(node)) throw new IllegalStateException("Missing " + node + ": " + plan);
    }
    System.out.println("[window-output-job-plan] key=" + keyType + " " + plan);
    double[][] times = new double[2][RUNS];
    for (int trial = 0; trial < WARMUP + RUNS; trial++) {
      for (int turn = 0; turn < 2; turn++) {
        int engine = (trial + turn) % 2;
        var table = environment(keyType);
        var scan = engine == 1 ? NativePlanner.install(table) : null;
        var output = new ArrayList<Row>(GROUPS);
        long start = System.nanoTime();
        try (var results = table.executeSql(SQL).collect()) {
          while (results.hasNext()) output.add(Row.copy(results.next()));
        }
        double seconds = (System.nanoTime() - start) / 1e9;
        boolean[] seen = new boolean[GROUPS];
        for (Row row : output) {
            int key = keyType.equals("STRING")
                ? Integer.parseInt(row.getField(0).toString().split("-")[1])
                : ((Long) row.getField(0)).intValue();
            assertEquals(false, seen[key]);
            seen[key] = true;
            long count = (ROWS - 1 - key) / GROUPS + 1;
            assertEquals(count, row.getField(1));
            long sum = 0;
            long values = 0;
            for (long id = key; id < ROWS; id += GROUPS) {
              if (id % 7 != 0) { sum += id; values++; }
            }
            assertEquals(values == 0 ? null : sum, row.getField(2));
            if (PROPERTIES == 2) {
              assertEquals(java.time.LocalDateTime.of(1970, 1, 1, 0, 0), row.getField(3));
              assertEquals(java.time.LocalDateTime.of(1970, 1, 1, 0, 0, 1), row.getField(4));
            }
        }
        assertEquals(GROUPS, output.size());
        if (scan != null && scan.substitutions() == 0) throw new IllegalStateException(scan.explainSummary());
        if (trial >= WARMUP) times[engine][trial - WARMUP] = seconds;
      }
    }
    double stock = median(times[0]);
    double nativeTime = median(times[1]);
    System.out.printf(Locale.ROOT,
        "[window-output-job] properties=%d key=%s rows=%d groups=%d Flink=%.6fs Native=%.6fs ratio=%.3fx stock_samples=%s native_samples=%s%n",
        PROPERTIES, keyType, ROWS, GROUPS, stock, nativeTime, stock / nativeTime,
        Arrays.toString(times[0]), Arrays.toString(times[1]));
  }

  private static double median(double[] samples) {
    double[] sorted = samples.clone();
    Arrays.sort(sorted);
    return sorted[sorted.length / 2];
  }

  private static StreamTableEnvironment environment(String keyType) {
    var env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(2);
    var table = StreamTableEnvironment.create(env);
    table.getConfig().setLocalTimeZone(java.time.ZoneOffset.UTC);
    table.getConfig().set("table.optimizer.agg-phase-strategy", "ONE_PHASE");
    var source = env.fromSequence(0, ROWS - 1).map(id -> Row.of(
        id % GROUPS, id % 7 == 0 ? null : id, 1L))
        .returns(Types.ROW_NAMED(new String[] {"raw_key", "v", "ts"}, Types.LONG, Types.LONG, Types.LONG))
        .assignTimestampsAndWatermarks(WatermarkStrategy.<Row>forBoundedOutOfOrderness(Duration.ofDays(1))
            .withTimestampAssigner((row, previous) -> (Long) row.getField(2)));
    table.createTemporaryView("input_rows", source, Schema.newBuilder()
        .column("raw_key", DataTypes.BIGINT()).column("v", DataTypes.BIGINT()).column("ts", DataTypes.BIGINT())
        .columnByMetadata("rt", DataTypes.TIMESTAMP_LTZ(3), "rowtime")
        .watermark("rt", "SOURCE_WATERMARK()").build());
    table.executeSql("CREATE TEMPORARY VIEW src AS SELECT "
        + (keyType.equals("STRING")
            ? "'group-' || CAST(raw_key AS STRING) || '-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx'"
            : "raw_key")
        + " AS k, v, rt FROM input_rows");
    // EXPLAIN SELECT omits its implicit collecting sink. A row sink witness exposes the same
    // required Arrow-to-row boundary; the measured job still collects and validates every row.
    table.executeSql("CREATE TABLE handoff_witness (k " + (keyType.equals("STRING") ? "STRING" : "BIGINT")
        + ", c BIGINT, s BIGINT" + (PROPERTIES == 2 ? ", window_start TIMESTAMP(3), window_end TIMESTAMP(3)" : "") + ") "
        + "WITH ('connector' = 'blackhole')");
    return table;
  }
}
