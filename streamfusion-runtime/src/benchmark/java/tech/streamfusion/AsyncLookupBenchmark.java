package tech.streamfusion;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
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
class AsyncLookupBenchmark {
  private static final long ROWS = Long.getLong("lookup.rows", 200_000L);
  private static final int CAPACITY = Integer.getInteger("lookup.capacity", 100);
  private static final int WARMUP = Integer.getInteger("lookup.warmup", 2);
  private static final int RUNS = Integer.getInteger("lookup.runs", 5);
  private static final String SQL =
      "INSERT INTO sink SELECT B.id, D.val FROM probes AS B"
          + " JOIN dim FOR SYSTEM_TIME AS OF B.p AS D ON MOD(B.id, 5) = D.k";

  @Test
  void asynchronousLookupWithRowBoundaries() throws Exception {
    String plan = NativePlanner.explain(environment(), SQL);
    for (String operator : List.of("NativeLookupJoin", "RowDataToArrow", "ArrowToRowData")) {
      if (!plan.contains(operator)) throw new AssertionError(operator + " missing: " + plan);
    }
    double[][] times = new double[2][RUNS];
    List<String> csv = new ArrayList<>(List.of("rows,capacity,engine,trial,seconds"));
    for (int trial = 0; trial < WARMUP + RUNS; trial++) {
      for (int turn = 0; turn < 2; turn++) {
        int engine = (trial + turn) % 2;
        TableEnvironment table = environment();
        var scan = engine == 1 ? NativePlanner.install(table) : null;
        long start = System.nanoTime();
        table.executeSql(SQL).await();
        double elapsed = (System.nanoTime() - start) / 1e9;
        if (engine == 1 && scan.substitutions() == 0) {
          throw new AssertionError(scan.fallbackReasons());
        }
        if (trial >= WARMUP) {
          times[engine][trial - WARMUP] = elapsed;
          csv.add(
              String.format(
                  Locale.ROOT,
                  "%d,%d,%s,%d,%.6f",
                  ROWS,
                  CAPACITY,
                  engine == 1 ? "native" : "flink",
                  trial - WARMUP,
                  elapsed));
        }
      }
    }
    Path output = Path.of(System.getProperty("lookup.output", "target/async-lookup.csv"));
    Path parent = output.toAbsolutePath().getParent();
    if (!Files.isDirectory(parent)) Files.createDirectories(parent);
    Files.write(output, csv);
    System.out.printf(
        Locale.ROOT,
        "[async-lookup] rows=%d capacity=%d Flink=%.6fs %s Native=%.6fs %s ratio=%.3fx%n",
        ROWS,
        CAPACITY,
        median(times[0]),
        Arrays.toString(times[0]),
        median(times[1]),
        Arrays.toString(times[1]),
        median(times[0]) / median(times[1]));
  }

  private static double median(double[] samples) {
    double[] ordered = samples.clone();
    Arrays.sort(ordered);
    return ordered[ordered.length / 2];
  }

  private static TableEnvironment environment() {
    var env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    var table = StreamTableEnvironment.create(env);
    table.getConfig().set("table.exec.async-lookup.buffer-capacity", Integer.toString(CAPACITY));
    table.createTemporaryView(
        "probes",
        env.fromSequence(0, ROWS - 1)
            .map(id -> Row.of(id))
            .returns(Types.ROW_NAMED(new String[] {"id"}, Types.LONG)),
        Schema.newBuilder()
            .column("id", DataTypes.BIGINT())
            .columnByExpression("p", "PROCTIME()")
            .build());
    table.executeSql(
        "CREATE TABLE dim (k BIGINT, val STRING) WITH ('connector' = 'test-lookup-async')");
    table.executeSql("CREATE TABLE sink (id BIGINT, val STRING) WITH ('connector' = 'blackhole')");
    return table;
  }
}
