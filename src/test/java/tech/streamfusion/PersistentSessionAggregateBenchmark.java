package tech.streamfusion;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.Locale;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
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
class PersistentSessionAggregateBenchmark {
  private static final long ROWS = Long.getLong("persistent.session.rows", 2_000_000L);
  private static final int KEYS = Integer.getInteger("persistent.session.keys", 16384);
  private static final int WIDTH = Integer.getInteger("persistent.session.bytes", 264);
  private static final boolean NULLABLE =
      Boolean.parseBoolean(System.getProperty("persistent.session.nullable", "true"));
  private static final int WARMUP = Integer.getInteger("persistent.session.warmup", 2);
  private static final int RUNS = Integer.getInteger("persistent.session.runs", 5);
  private static final String OFF_HEAP = System.getProperty("persistent.session.offHeap", "256m");
  private static final String ROCKS_MEMORY =
      System.getProperty("persistent.session.rocksMemory", "128m");
  private static final String SQL =
      "INSERT INTO sink SELECT k, COUNT(*), SUM(v), "
          + "SESSION_START(ts, INTERVAL '1' SECOND), SESSION_END(ts, INTERVAL '1' SECOND) "
          + "FROM inputs GROUP BY k, SESSION(ts, INTERVAL '1' SECOND)";

  @Test
  void persistentSessionCountAndSum() throws Exception {
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
        "[persistent-session] rows=%d keys=%d bytes=%d nullable=%s Flink=%.6fs Native=%.6fs"
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
  @EnabledIfEnvironmentVariable(named = "SF_PROFILE_PERSISTENT_SESSION", matches = "true")
  void profilePersistentSessionCountAndSum() throws Exception {
    assertPlan();
    boolean nativeEngine =
        Boolean.parseBoolean(System.getProperty("persistent.session.native", "true"));
    for (int i = 0; i < WARMUP; i++) run(nativeEngine);
    String pid = Long.toString(ProcessHandle.current().pid());
    Path output =
        Path.of(System.getProperty("profile.outputDir", "target/profiles/persistent-session"));
    Files.createDirectories(output);
    Path recording = output.resolve(nativeEngine ? "native.jfr" : "flink.jfr").toAbsolutePath();
    String profiler = System.getProperty("profile.asprof", "asprof");
    profileCommand(profiler, "start", "-e", "cpu", "-i", "1ms", "-f", recording.toString(), pid);
    try {
      long deadline =
          System.nanoTime() + Long.getLong("persistent.session.seconds", 45L) * 1_000_000_000L;
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
      throw new IllegalArgumentException("Invalid persistent session benchmark configuration");
    }
    String plan = NativePlanner.explain(environment(), SQL);
    if (!plan.contains("NativeColumnarSessionWindowAggregate")
        || !plan.contains("RowDataToArrow")
        || !plan.contains("ArrowToRowData")) {
      throw new IllegalStateException(
          "Expected native session aggregate and both transposes: " + plan);
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

  private static long timestampSeconds(long row) {
    switch ((int) ((row / KEYS) % 3)) {
      case 1:
        return 2;
      case 2:
        return 1;
      default:
        return 0;
    }
  }

  private static TableEnvironment environment() {
    Configuration configuration = new Configuration();
    configuration.setString(
        "state.backend.type", "tech.streamfusion.state.RocksDBNativeStateBackendFactory");
    configuration.setString("taskmanager.memory.task.off-heap.size", OFF_HEAP);
    configuration.setString("state.backend.rocksdb.memory.fixed-per-slot", ROCKS_MEMORY);
    var env = StreamExecutionEnvironment.getExecutionEnvironment(configuration);
    env.setParallelism(1);
    env.getConfig().setAutoWatermarkInterval(0);
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
                        NULLABLE && (i % 7 == 0 || i % KEYS == 1) ? null : i % 101 - 50,
                        LocalDateTime.ofEpochSecond(timestampSeconds(i), 0, ZoneOffset.UTC)))
            .returns(Types.ROW_NAMED(
                new String[] {"k", "v", "ts"}, Types.STRING, Types.LONG, Types.LOCAL_DATE_TIME)),
        Schema.newBuilder()
            .column("k", DataTypes.STRING())
            .column("v", DataTypes.BIGINT())
            .column("ts", DataTypes.TIMESTAMP(3))
            .watermark("ts", "ts - INTERVAL '1' SECOND")
            .build());
    table.executeSql(
        "CREATE TABLE sink (k STRING, n BIGINT, total BIGINT, "
            + "window_start TIMESTAMP(3), window_end TIMESTAMP(3)) "
            + "WITH ('connector' = 'blackhole')");
    return table;
  }
}
