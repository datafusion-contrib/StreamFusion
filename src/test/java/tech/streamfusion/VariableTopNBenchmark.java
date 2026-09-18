package tech.streamfusion;

import java.util.Arrays;
import java.util.Locale;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import tech.streamfusion.planner.NativePlanner;

@EnabledIfEnvironmentVariable(named = "SF_BENCHMARK", matches = "true")
class VariableTopNBenchmark {
  private static final long ROWS = Long.getLong("variabletopn.rows", 1_000_000L);
  private static final int WARMUP = Integer.getInteger("variabletopn.warmup", 2);
  private static final int RUNS = Integer.getInteger("variabletopn.runs", 5);
  private static final boolean RETRACTING = Boolean.getBoolean("variabletopn.retracting");
  private static final boolean UPDATE_FAST = Boolean.getBoolean("variabletopn.updateFast");
  private static final String SQL =
      UPDATE_FAST
          ? "INSERT INTO sink SELECT k, n AS v, rn FROM (SELECT k, v, n, MOD(k, 3) + 1 AS rank_end,"
              + " ROW_NUMBER() OVER (PARTITION BY k ORDER BY n DESC, v ASC) AS rn"
              + " FROM (SELECT k, v, COUNT(*) AS n FROM src GROUP BY k, v)) WHERE rn <= rank_end"
          : "INSERT INTO sink SELECT k, v, rn FROM (SELECT k, v, MOD(k, 3) + 1 AS rank_end,"
                + " ROW_NUMBER() OVER (PARTITION BY k ORDER BY v DESC) AS rn FROM src) WHERE rn <="
                + " rank_end";

  @Test
  void variableTopN() throws Exception {
    if (UPDATE_FAST && !environment().explainSql(SQL).contains("UpdateFastStrategy")) {
      throw new IllegalStateException("Expected Flink's update-fast strategy");
    }
    String plan = NativePlanner.explain(environment(), SQL);
    if (!plan.contains("NativeColumnarTopN")
        || !plan.contains("RowDataToArrow")
        || !plan.contains("ArrowToRowData")) {
      throw new IllegalStateException(
          "Expected native variable Top-N and both transposes: " + plan);
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
        "[variable-top-n] retracting=%s update_fast=%s rows=%d Flink=%.6fs Native=%.6fs ratio=%.3fx"
            + " host_trials=%s native_trials=%s%n",
        RETRACTING,
        UPDATE_FAST,
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
    var input =
        env.fromSequence(0, ROWS - 1)
            .map(
                i -> {
                  if (UPDATE_FAST) return Row.of(i % 4096, (i / 4096) % 16);
                  if (!RETRACTING) return Row.of(i % 4096, i);
                  long cycle = i / 16384;
                  return Row.ofKind(
                      cycle % 2 == 0 ? RowKind.INSERT : RowKind.DELETE,
                      i % 4096,
                      (cycle / 2) * 16384 + i % 16384);
                })
            .returns(Types.ROW_NAMED(new String[] {"k", "v"}, Types.LONG, Types.LONG));
    var schema =
        org.apache.flink.table.api.Schema.newBuilder()
            .column("k", org.apache.flink.table.api.DataTypes.BIGINT().notNull())
            .column("v", org.apache.flink.table.api.DataTypes.BIGINT())
            .build();
    table.createTemporaryView(
        "src",
        RETRACTING
            ? table.fromChangelogStream(input, schema)
            : table.fromDataStream(input, schema));
    table.executeSql(
        "CREATE TABLE sink (k BIGINT, v BIGINT, rn BIGINT) WITH ('connector' = 'blackhole')");
    return table;
  }
}
