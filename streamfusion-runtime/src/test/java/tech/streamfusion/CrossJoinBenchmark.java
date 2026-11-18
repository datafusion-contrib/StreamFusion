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
class CrossJoinBenchmark {
  private static final long ROWS = Long.getLong("join.rows", 100_000L);
  private static final int WARMUP = Integer.getInteger("join.warmup", 2);
  private static final int RUNS = Integer.getInteger("join.runs", 5);
  private static final String SQL = "INSERT INTO sink SELECT a.v, b.k FROM A a CROSS JOIN B b";

  @Test
  void crossJoin() throws Exception {
    String plan = NativePlanner.explain(environment(), SQL);
    if (!plan.contains("NativeColumnarUpdatingJoin") || !plan.contains("RowDataToArrow")
        || !plan.contains("ArrowToRowData") || !plan.contains("NativeColumnarExchange")) {
      throw new IllegalStateException("Expected native join and both transposes: " + plan);
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
        if (scan != null && scan.substitutions() == 0) throw new IllegalStateException(scan.explainSummary());
        if (trial >= WARMUP) times[engine][trial - WARMUP] = seconds;
      }
    }
    double host = median(times[0]);
    double nativeTime = median(times[1]);
    System.out.printf(Locale.ROOT,
        "[cross-join] rows=%d Flink=%.6fs Native=%.6fs ratio=%.3fx host_trials=%s native_trials=%s%n",
        ROWS, host, nativeTime, host / nativeTime, Arrays.toString(times[0]), Arrays.toString(times[1]));
  }

  private static double median(double[] values) {
    double[] sorted = values.clone();
    Arrays.sort(sorted);
    return sorted[sorted.length / 2];
  }

  private static TableEnvironment environment() {
    var env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(2);
    var table = StreamTableEnvironment.create(env);
    table.createTemporaryView("A", env.fromSequence(0, ROWS - 1)
        .map(i -> Row.of(i % 4096 == 0 ? null : i % 1023, i))
        .returns(Types.ROW_NAMED(new String[] {"k", "v"}, Types.LONG, Types.LONG)));
    table.createTemporaryView("B", env.fromSequence(0, 15)
        .map(i -> Row.of(i))
        .returns(Types.ROW_NAMED(new String[] {"k"}, Types.LONG)));
    table.executeSql("CREATE TABLE sink (v BIGINT, k BIGINT) WITH ('connector' = 'blackhole')");
    return table;
  }
}
