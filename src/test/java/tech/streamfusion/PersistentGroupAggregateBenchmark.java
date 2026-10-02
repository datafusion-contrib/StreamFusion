package tech.streamfusion;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.Locale;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import tech.streamfusion.planner.NativePlanner;

@EnabledIfEnvironmentVariable(named = "SF_BENCHMARK", matches = "true")
class PersistentGroupAggregateBenchmark {
  private static final long ROWS = Long.getLong("persistent.group.rows", 2_000_000L);
  private static final int KEYS = Integer.getInteger("persistent.group.keys", 16384);
  private static final int WIDTH = Integer.getInteger("persistent.group.bytes", 264);
  private static final boolean NULLABLE =
      Boolean.parseBoolean(System.getProperty("persistent.group.nullable", "true"));
  private static final int WARMUP = Integer.getInteger("persistent.group.warmup", 2);
  private static final int RUNS = Integer.getInteger("persistent.group.runs", 5);
  private static final String OFF_HEAP = System.getProperty("persistent.group.offHeap", "256m");
  private static final String ROCKS_MEMORY =
      System.getProperty("persistent.group.rocksMemory", "128m");
  private static final String SQL =
      "INSERT INTO sink SELECT k, COUNT(*), SUM(v) FROM inputs GROUP BY k";

  @Test
  void persistentCountAndSum() throws Exception {
    assertPlan();
    double[][] times = new double[2][RUNS];
    for (int trial = 0; trial < WARMUP + RUNS; trial++) {
      for (int turn = 0; turn < 2; turn++) {
        int engine = (trial + turn) % 2;
        double seconds = run(engine == 1);
        if (trial >= WARMUP) times[engine][trial - WARMUP] = seconds;
      }
    }
    System.out.printf(
        Locale.ROOT,
        "[persistent-group] rows=%d keys=%d bytes=%d nullable=%s Flink=%.6fs Native=%.6fs"
            + " flink_trials=%s native_trials=%s%n",
        ROWS,
        KEYS,
        WIDTH,
        NULLABLE,
        median(times[0]),
        median(times[1]),
        Arrays.toString(times[0]),
        Arrays.toString(times[1]));
  }

  @Test
  @EnabledIfEnvironmentVariable(named = "SF_PROFILE_PERSISTENT_GROUP", matches = "true")
  void profilePersistentCountAndSum() throws Exception {
    assertPlan();
    boolean nativeEngine =
        Boolean.parseBoolean(System.getProperty("persistent.group.native", "true"));
    for (int i = 0; i < WARMUP; i++) run(nativeEngine);
    String pid = Long.toString(ProcessHandle.current().pid());
    Path output = Path.of(System.getProperty("profile.outputDir", "target/profiles/persistent-group"));
    Files.createDirectories(output);
    Path recording = output.resolve(nativeEngine ? "native.jfr" : "flink.jfr").toAbsolutePath();
    String profiler = System.getProperty("profile.asprof", "asprof");
    profileCommand(profiler, "start", "-e", "cpu", "-i", "1ms", "-f", recording.toString(), pid);
    try {
      long deadline = System.nanoTime() + Long.getLong("persistent.group.seconds", 45L) * 1_000_000_000L;
      do {
        run(nativeEngine);
      } while (System.nanoTime() < deadline);
    } finally {
      profileCommand(profiler, "stop", pid);
    }
  }

  private static void profileCommand(String... command) throws Exception {
    int exit = new ProcessBuilder(command).inheritIO().start().waitFor();
    if (exit != 0) throw new IllegalStateException("Profiler exited " + exit);
  }

  private static void assertPlan() {
    if (ROWS <= 0 || KEYS <= 0 || WIDTH < 0 || WARMUP < 0 || RUNS <= 0) {
      throw new IllegalArgumentException("Invalid persistent group benchmark configuration");
    }
    String plan = NativePlanner.explain(environment(), SQL);
    if (!plan.contains("NativeColumnarGroupAggregate")
        || !plan.contains("RowDataToArrow")
        || !plan.contains("ArrowToRowData")) {
      throw new IllegalStateException("Expected native GROUP BY and both transposes: " + plan);
    }
  }

  private static double run(boolean nativeEngine) throws Exception {
    TableEnvironment table = environment();
    var scan = nativeEngine ? NativePlanner.install(table) : null;
    long start = System.nanoTime();
    table.executeSql(SQL).await();
    double seconds = (System.nanoTime() - start) / 1e9;
    if (scan != null && scan.substitutions() == 0) {
      throw new IllegalStateException("Unexpected fallback: " + scan.fallbackReasons());
    }
    return seconds;
  }

  private static double median(double[] values) {
    double[] sorted = values.clone();
    Arrays.sort(sorted);
    int middle = sorted.length / 2;
    return sorted.length % 2 == 0 ? (sorted[middle - 1] + sorted[middle]) / 2 : sorted[middle];
  }

  private static TableEnvironment environment() {
    Configuration configuration = new Configuration();
    configuration.setString(
        "state.backend.type", "tech.streamfusion.state.RocksDBNativeStateBackendFactory");
    configuration.setString("taskmanager.memory.task.off-heap.size", OFF_HEAP);
    configuration.setString("state.backend.rocksdb.memory.fixed-per-slot", ROCKS_MEMORY);
    var env = StreamExecutionEnvironment.getExecutionEnvironment(configuration);
    env.setParallelism(1);
    var table = StreamTableEnvironment.create(env);
    table.getConfig().setLocalTimeZone(ZoneOffset.UTC);
    table.getConfig().set("table.optimizer.agg-phase-strategy", "ONE_PHASE");
    table.getConfig().set("table.exec.mini-batch.enabled", "true");
    table.getConfig().set("table.exec.mini-batch.size", "1024");
    table.getConfig().set("table.exec.mini-batch.allow-latency", "1 h");
    String suffix = "x".repeat(WIDTH);
    table.createTemporaryView(
        "inputs",
        env.fromSequence(0, ROWS - 1)
            .map(
                i ->
                    Row.of(
                        NULLABLE && i % 11 == 0 ? null : (i % KEYS) + "/中\0" + suffix,
                        NULLABLE && (i % 7 == 0 || i % KEYS == 1) ? null : i % 101 - 50))
            .returns(Types.ROW_NAMED(new String[] {"k", "v"}, Types.STRING, Types.LONG)));
    table.executeSql(
        "CREATE TABLE sink (k STRING, n BIGINT, total BIGINT) WITH ('connector' = 'blackhole')");
    return table;
  }
}
