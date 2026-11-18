package tech.streamfusion;

import java.time.Duration;
import java.time.Instant;
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
import tech.streamfusion.planner.PhysicalPlanScan;

/** Standalone assignment, including input fan-out and both row/Arrow transposes. */
@EnabledIfEnvironmentVariable(named = "SF_BENCHMARK", matches = "true")
class LtzWindowTvfBenchmark {
  private static final long ROWS = Long.getLong("tvf.rows", 2_000_000L);
  private static final int WARMUP = Integer.getInteger("tvf.warmup", 2);
  private static final int RUNS = Integer.getInteger("tvf.runs", 5);

  @Test
  void compareAssignment() throws Exception {
    for (String shape : new String[] {"TUMBLE", "HOP", "CUMULATE"}) {
      TableEnvironment check = environment();
      String plan = NativePlanner.explain(check, prepare(check, shape));
      for (String node :
          new String[] {"NativeWindowTableFunction", "RowDataToArrow", "ArrowToRowData"}) {
        if (!plan.contains(node)) {
          throw new IllegalStateException("Missing " + node + ": " + plan);
        }
      }
      double[][] seconds = new double[2][RUNS];
      for (int trial = 0; trial < WARMUP + RUNS; trial++) {
        for (int turn = 0; turn < 2; turn++) {
          int engine = (trial + turn) % 2;
          double elapsed = run(shape, engine == 1);
          if (trial >= WARMUP) {
            seconds[engine][trial - WARMUP] = elapsed;
          }
        }
      }
      System.out.printf(
          Locale.ROOT,
          "[ltz-tvf] %s rows=%d Flink=%.6fs Native=%.6fs flink_trials=%s native_trials=%s%n",
          shape,
          ROWS,
          median(seconds[0]),
          median(seconds[1]),
          Arrays.toString(seconds[0]),
          Arrays.toString(seconds[1]));
    }
  }

  private static String prepare(TableEnvironment table, String shape) {
    String intervals =
        shape.equals("TUMBLE")
            ? "INTERVAL '10' SECOND"
            : "INTERVAL '5' SECOND, INTERVAL '10' SECOND";
    String select =
        "SELECT id, ts, window_start, window_end, window_time FROM TABLE("
            + shape
            + "(TABLE src, DESCRIPTOR(ts), "
            + intervals
            + "))";
    String columns =
        table.sqlQuery(select).getResolvedSchema().getColumns().stream()
            .map(
                c ->
                    "`"
                        + c.getName()
                        + "` "
                        + c.getDataType().getLogicalType().asSerializableString())
            .collect(Collectors.joining(", "));
    table.executeSql("CREATE TABLE sink (" + columns + ") WITH ('connector' = 'blackhole')");
    return "INSERT INTO sink " + select;
  }

  private static double run(String shape, boolean nativeRun) throws Exception {
    TableEnvironment table = environment();
    String sql = prepare(table, shape);
    PhysicalPlanScan scan = nativeRun ? NativePlanner.install(table) : null;
    long start = System.nanoTime();
    table.executeSql(sql).await();
    double seconds = (System.nanoTime() - start) / 1e9;
    if (nativeRun && scan.substitutions() == 0) {
      throw new IllegalStateException("Unexpected fallback: " + scan.fallbackReasons());
    }
    return seconds;
  }

  private static double median(double[] values) {
    double[] sorted = values.clone();
    Arrays.sort(sorted);
    return sorted[sorted.length / 2];
  }

  private static TableEnvironment environment() {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    StreamTableEnvironment table = StreamTableEnvironment.create(env);
    table.getConfig().set("table.local-time-zone", "GMT+08:00");
    // The blackhole sink observes values, so neither of the two rowtime columns is a sink
    // timestamp.
    table.getConfig().set("table.exec.sink.rowtime-inserter", "DISABLED");
    table.createTemporaryView(
        "src",
        env.fromSequence(0, ROWS - 1)
            .map(
                i ->
                    Row.of(
                        i.intValue(),
                        i % 8 == 0
                            ? null
                            : Instant.ofEpochSecond(i % 4096 - 2048, 999_999_999)))
            .returns(Types.ROW_NAMED(new String[] {"id", "ts"}, Types.INT, Types.INSTANT))
            .assignTimestampsAndWatermarks(
                WatermarkStrategy.<Row>forBoundedOutOfOrderness(Duration.ofDays(1))
                    .withTimestampAssigner(
                        (row, previous) ->
                            row.getField(1) == null
                                ? 0
                                : ((Instant) row.getField(1)).toEpochMilli())),
        Schema.newBuilder()
            .column("id", DataTypes.INT())
            .column("ts", DataTypes.TIMESTAMP_LTZ(3))
            .watermark("ts", "SOURCE_WATERMARK()")
            .build());
    return table;
  }
}
