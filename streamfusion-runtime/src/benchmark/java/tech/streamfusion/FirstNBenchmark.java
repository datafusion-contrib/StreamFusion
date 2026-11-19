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
class FirstNBenchmark {
  private static final long ROWS = Long.getLong("firstn.rows", 1_000_000L);
  private static final int WARMUP = Integer.getInteger("firstn.warmup", 2);
  private static final int RUNS = Integer.getInteger("firstn.runs", 5);
  private static final String SQL =
      "INSERT INTO sink SELECT k, v, rn FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY k ORDER BY"
          + " pt ASC) AS rn FROM src) WHERE rn <= 2";

  @Test
  void firstN() throws Exception {
    String plan = NativePlanner.explain(environment(), SQL);
    if (!plan.contains("NativeFirstN")
        || !plan.contains("RowDataToArrow")
        || !plan.contains("ArrowToRowData")) {
      throw new IllegalStateException("Expected native first-N and both transposes: " + plan);
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
        if (scan != null && scan.substitutions() == 0)
          throw new IllegalStateException(scan.explainSummary());
        if (trial >= WARMUP) times[engine][trial - WARMUP] = seconds;
      }
    }
    double host = median(times[0]);
    double nativeTime = median(times[1]);
    System.out.printf(
        Locale.ROOT,
        "[first-n] rows=%d Flink=%.6fs Native=%.6fs ratio=%.3fx host_trials=%s native_trials=%s%n",
        ROWS,
        host,
        nativeTime,
        host / nativeTime,
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
    table.createTemporaryView(
        "src",
        env.fromSequence(0, ROWS - 1)
            .map(i -> Row.of(i % 4096, i))
            .returns(Types.ROW_NAMED(new String[] {"k", "v"}, Types.LONG, Types.LONG)),
        org.apache.flink.table.api.Schema.newBuilder()
            .column("k", org.apache.flink.table.api.DataTypes.BIGINT())
            .column("v", org.apache.flink.table.api.DataTypes.BIGINT())
            .columnByExpression("pt", "PROCTIME()")
            .build());
    table.executeSql(
        "CREATE TABLE sink (k BIGINT, v BIGINT, rn BIGINT) WITH ('connector' = 'blackhole')");
    return table;
  }
}
