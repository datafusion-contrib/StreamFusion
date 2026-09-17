package tech.streamfusion;

import java.util.Arrays;
import java.util.Locale;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import tech.streamfusion.planner.NativePlanner;

@EnabledIfEnvironmentVariable(named = "SF_BENCHMARK", matches = "true")
class SharedBinaryUdfBenchmark {
  private static final long ROWS = Long.getLong("binaryudf.rows", 1_000_000L);
  private static final int WARMUP = Integer.getInteger("binaryudf.warmup", 2);
  private static final int RUNS = Integer.getInteger("binaryudf.runs", 5);
  private static final String SQL =
      "INSERT INTO sink SELECT id, shared(id), shared(id + 1) FROM src";

  @Test
  void sharedBinaryResults() throws Exception {
    String plan = NativePlanner.explain(environment(), SQL);
    if (!plan.contains("NativeCalc")
        || !plan.contains("RowDataToArrow")
        || !plan.contains("ArrowToRowData")) {
      throw new IllegalStateException("Expected native Calc and both transposes: " + plan);
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
        if (scan != null && (scan.substitutions() == 0 || !scan.fallbackReasons().isEmpty()))
          throw new IllegalStateException(scan.explainSummary());
        if (trial >= WARMUP) times[engine][trial - WARMUP] = seconds;
      }
    }
    double host = median(times[0]);
    double nativeTime = median(times[1]);
    System.out.printf(
        Locale.ROOT,
        "[shared-binary-udf] rows=%d Flink=%.6fs Native=%.6fs ratio=%.3fx host_trials=%s"
            + " native_trials=%s%n",
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
            .map(i -> Row.of(i.intValue()))
            .returns(Types.ROW_NAMED(new String[] {"id"}, Types.INT)));
    table.createTemporarySystemFunction("shared", new SharedBinary());
    table.executeSql(
        "CREATE TABLE sink (id INT, a BYTES, b BYTES) WITH ('connector' = 'blackhole')");
    return table;
  }

  public static class SharedBinary extends ScalarFunction {
    private final byte[] buffer = new byte[4];

    public byte[] eval(Integer value) {
      if (value == null) return null;
      for (int i = 0; i < 4; i++) buffer[i] = (byte) (value >>> (8 * i));
      return buffer;
    }
  }
}
