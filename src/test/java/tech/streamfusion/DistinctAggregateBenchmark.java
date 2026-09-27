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
  private static final int WARMUP = Integer.getInteger("distinct.warmup", 2);
  private static final int RUNS = Integer.getInteger("distinct.runs", 5);
  private static final String SQL =
      "INSERT INTO sink SELECT k, COUNT(DISTINCT b), COUNT(DISTINCT ts),"
          + " SUM(DISTINCT amount) FROM src GROUP BY k";

  @Test
  void groupedDistinctTypes() throws Exception {
    String plan = NativePlanner.explain(environment(), SQL);
    if (!plan.contains("NativeColumnarGroupAggregate")
        || !plan.contains("RowDataToArrow")
        || !plan.contains("ArrowToRowData")
        || !plan.contains("NativeColumnarLocalGroupAggregate")) {
      throw new IllegalStateException("Expected native aggregation and both transposes: " + plan);
    }
    double[][] times = new double[2][RUNS];
    for (int trial = 0; trial < WARMUP + RUNS; trial++) {
      for (int turn = 0; turn < 2; turn++) {
        int engine = (trial + turn) % 2;
        var table = environment();
        var scan = engine == 1 ? NativePlanner.install(table) : null;
        long start = System.nanoTime();
        table.executeSql(SQL).await();
        double seconds = (System.nanoTime() - start) / 1e9;
        if (scan != null && scan.substitutions() == 0) {
          throw new IllegalStateException("Unexpected fallback: " + scan.fallbackReasons());
        }
        if (trial >= WARMUP) times[engine][trial - WARMUP] = seconds;
      }
    }
    System.out.printf(
        Locale.ROOT,
        "[distinct-aggregate] phase=two rows=%d Flink=%.6fs Native=%.6fs flink_trials=%s"
            + " native_trials=%s%n",
        ROWS,
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
    var env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    var table = StreamTableEnvironment.create(env);
    table.getConfig().set("table.optimizer.agg-phase-strategy", "TWO_PHASE");
    table.getConfig().set("table.exec.mini-batch.enabled", "true");
    table.getConfig().set("table.exec.mini-batch.allow-latency", "1 h");
    table.getConfig().set("table.exec.mini-batch.size", "1024");
    table.createTemporaryView(
        "src",
        env.fromSequence(0, ROWS - 1)
            .map(
                i ->
                    Row.of(
                        (int) (i % 64),
                        i % 7 == 0 ? null : (i / 64) % 2 == 0,
                        i % 7 == 0 ? null : Instant.ofEpochSecond(0, i / 64 % 128),
                        i % 7 == 0 ? null : BigDecimal.valueOf(i / 64 % 128 - 64, 2)))
            .returns(
                Types.ROW_NAMED(
                    new String[] {"k", "b", "ts", "amount"},
                    Types.INT,
                    Types.BOOLEAN,
                    Types.INSTANT,
                    Types.BIG_DEC)),
        Schema.newBuilder()
            .column("k", DataTypes.INT())
            .column("b", DataTypes.BOOLEAN())
            .column("ts", DataTypes.TIMESTAMP_LTZ(9))
            .column("amount", DataTypes.DECIMAL(19, 2))
            .build());
    table.executeSql(
        "CREATE TABLE sink (k INT, cb BIGINT, ct BIGINT, sa DECIMAL(38,2))"
            + " WITH ('connector' = 'blackhole')");
    return table;
  }
}
