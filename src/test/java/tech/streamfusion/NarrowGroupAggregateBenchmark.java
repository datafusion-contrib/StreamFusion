package tech.streamfusion;

import java.util.Arrays;
import java.util.Locale;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import tech.streamfusion.planner.NativePlanner;

@EnabledIfEnvironmentVariable(named = "SF_BENCHMARK", matches = "true")
class NarrowGroupAggregateBenchmark {
  private static final long ROWS = Long.getLong("narrow.rows", 2_000_000L);
  private static final int WARMUP = Integer.getInteger("narrow.warmup", 2);
  private static final int RUNS = Integer.getInteger("narrow.runs", 5);
  private static final String SQL =
      "INSERT INTO sink SELECT k, SUM(b), MIN(b), MAX(b), SUM(s), MIN(s), MAX(s) FROM src GROUP BY"
          + " k";

  @Test
  void groupedNarrowIntegers() throws Exception {
    for (boolean twoPhase : new boolean[] {false, true}) {
      String plan = NativePlanner.explain(environment(twoPhase), SQL);
      if (!plan.contains("NativeColumnarGroupAggregate")
          || !plan.contains("RowDataToArrow")
          || !plan.contains("ArrowToRowData")
          || twoPhase && !plan.contains("NativeColumnarLocalGroupAggregate")) {
        throw new IllegalStateException("Expected native aggregation and both transposes: " + plan);
      }
      double[][] times = new double[2][RUNS];
      for (int trial = 0; trial < WARMUP + RUNS; trial++) {
        for (int turn = 0; turn < 2; turn++) {
          int engine = (trial + turn) % 2;
          var table = environment(twoPhase);
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
          "[narrow-aggregate] phase=%s rows=%d Flink=%.6fs Native=%.6fs flink_trials=%s"
              + " native_trials=%s%n",
          twoPhase ? "two" : "one",
          ROWS,
          median(times[0]),
          median(times[1]),
          Arrays.toString(times[0]),
          Arrays.toString(times[1]));
    }
  }

  private static double median(double[] values) {
    double[] sorted = values.clone();
    Arrays.sort(sorted);
    return sorted[sorted.length / 2];
  }

  private static TableEnvironment environment(boolean twoPhase) {
    var env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    var table = StreamTableEnvironment.create(env);
    table
        .getConfig()
        .set("table.optimizer.agg-phase-strategy", twoPhase ? "TWO_PHASE" : "ONE_PHASE");
    table.getConfig().set("table.exec.mini-batch.enabled", Boolean.toString(twoPhase));
    table.getConfig().set("table.exec.mini-batch.allow-latency", "1 h");
    table.getConfig().set("table.exec.mini-batch.size", "1024");
    table.createTemporaryView(
        "src",
        env.fromSequence(0, ROWS - 1)
            .map(
                i ->
                    Row.of(
                        (int) (i % 64),
                        i % 7 == 0 ? null : (byte) (i / 64),
                        i % 7 == 0 ? null : (short) (i / 64)))
            .returns(
                Types.ROW_NAMED(new String[] {"k", "b", "s"}, Types.INT, Types.BYTE, Types.SHORT)));
    table.executeSql(
        "CREATE TABLE sink (k INT, sb TINYINT, nb TINYINT, xb TINYINT,"
            + " ss SMALLINT, ns SMALLINT, xs SMALLINT) WITH ('connector' = 'blackhole')");
    return table;
  }
}
