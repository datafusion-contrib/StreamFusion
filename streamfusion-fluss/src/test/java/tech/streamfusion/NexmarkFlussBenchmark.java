package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.time.Duration;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.ExplainDetail;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.flink.utils.FlinkConversions;
import org.apache.fluss.metadata.DatabaseDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.types.RowType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import tech.streamfusion.fluss.FlussTestCluster;
import tech.streamfusion.planner.NativePlanner;
import tech.streamfusion.planner.PhysicalPlanScan;

/** Same matrix queries and event corpus, with Fluss at both transport boundaries. */
@EnabledIfEnvironmentVariable(named = "SF_FLUSS_BENCH", matches = "true")
class NexmarkFlussBenchmark {
  private static final int PARALLELISM = 4;
  private static final long ROWS =
      Long.parseLong(System.getenv().getOrDefault("SF_ROWS", "1000000"));
  private static final int WARMUPS = Integer.getInteger("nexmark.warmups", 1);
  private static final int RUNS = Integer.getInteger("nexmark.runs", 3);
  private static final String BACKEND =
      System.getenv().getOrDefault("SF_FLUSS_STATE_BACKEND", "memory");
  private static final boolean MINI_BATCH =
      Boolean.parseBoolean(System.getenv().getOrDefault("SF_FLUSS_MINI_BATCH", "false"));
  private Path rocksDirectory;
  private static final ObjectMapper JSON = new ObjectMapper();

  @Test
  void flussToFlussMatrix() throws Exception {
    assertTrue(
        System.getProperty("java.library.path").contains("release"),
        "Use -Pbench for measurements");
    String previous = System.getProperty("streamfusion.fluss.enabled");
    System.setProperty("streamfusion.fluss.enabled", "true");
    rocksDirectory = Files.createTempDirectory("fluss-rocksdb");
    try (FlussTestCluster cluster = new FlussTestCluster()) {
      assertTrue(BACKEND.equals("memory") || BACKEND.equals("rocksdb"));
      cluster.start();
      var config = cluster.config();
      config.set(
          ConfigOptions.CLIENT_WRITER_BUCKET_NO_KEY_ASSIGNER,
          ConfigOptions.NoKeyAssigner.ROUND_ROBIN);
      try (Connection connection = ConnectionFactory.createConnection(config)) {
        connection.getAdmin().createDatabase("nexmark", DatabaseDescriptor.EMPTY, true).get();
        seed(cluster, connection);
        if (BACKEND.equals("rocksdb")) assertRocksDBEngages(cluster, connection);
        for (var query : NexmarkMatrixBenchmark.selectQueries()) {
          for (int warmup = 0; warmup < WARMUPS; warmup++) {
            try (Result baseline = run(cluster, connection, query, false);
                Result accelerated = run(cluster, connection, query, true)) {
              if (!query.label.equals("q12"))
                baseline.rows.assertSame(accelerated.rows, query.label);
            }
          }
          List<Double> stock = new ArrayList<>();
          List<Double> nativeTimes = new ArrayList<>();
          for (int run = 0; run < RUNS; run++) {
            try (Result baseline = run(cluster, connection, query, false);
                Result accelerated = run(cluster, connection, query, true)) {
              if (!query.label.equals("q12"))
                baseline.rows.assertSame(accelerated.rows, query.label);
              stock.add(baseline.seconds);
              nativeTimes.add(accelerated.seconds);
            }
          }
          System.out.printf(
              "[fluss] %s rows=%d parallelism=%d stock=%s native=%s primaryKeySink=%s backend=%s miniBatch=%s%n",
              query.label,
              ROWS,
              PARALLELISM,
              stock,
              nativeTimes,
              NexmarkMatrixBenchmark.UPSERT_KEYS.containsKey(query.label),
              BACKEND,
              MINI_BATCH);
        }
      }
    } finally {
      try (var files = Files.walk(rocksDirectory)) {
        for (var file : files.sorted(Comparator.reverseOrder()).toList()) Files.delete(file);
      }
      if (previous == null) System.clearProperty("streamfusion.fluss.enabled");
      else System.setProperty("streamfusion.fluss.enabled", previous);
    }
  }

  private void seed(FlussTestCluster cluster, Connection connection) throws Exception {
    RowType type = FlinkConversions.toFlussRowType(NexmarkKafkaBenchmark.nexmarkRowType());
    StreamTableEnvironment tables = environment(cluster, false);
    tables.executeSql(
        "CREATE TABLE fluss.nexmark.events ("
            + NexmarkKafkaBenchmark.SCHEMA
            + ", rowtime AS TO_TIMESTAMP_LTZ(`dateTime`, 3),"
            + " WATERMARK FOR rowtime AS rowtime - INTERVAL '4' SECOND)"
            + " WITH ('bucket.num'='4', 'table.log.format'='ARROW',"
            + " 'scan.startup.mode'='earliest', 'scan.bounded.mode'='latest-offset')");
    tables.getCatalog("fluss").orElseThrow().close();
    TablePath path = TablePath.of("nexmark", "events");
    assertFalse(
        connection.getAdmin().getTableInfo(path).get().hasPrimaryKey(),
        "The shared Nexmark input must be append-only");
    var writer = connection.getTable(path).newAppend().createWriter();
    for (long i = 0; i < ROWS; i++) {
      writer.append(row(JSON.readTree(NexmarkKafkaBenchmark.event(i)), type));
      if (i % 8192 == 8191) writer.flush();
    }
    writer.flush();
  }

  private static GenericRow row(JsonNode value, RowType type) {
    GenericRow row = new GenericRow(type.getFieldCount());
    for (int i = 0; i < type.getFieldCount(); i++) {
      JsonNode field = value.get(type.getFields().get(i).getName());
      Object decoded =
          field == null || field.isNull()
              ? null
              : switch (type.getTypeAt(i).getTypeRoot()) {
                case INTEGER -> field.intValue();
                case BIGINT -> field.longValue();
                case STRING -> BinaryString.fromString(field.textValue());
                case ROW -> row(field, (RowType) type.getTypeAt(i));
                default -> throw new IllegalArgumentException("Unexpected Nexmark physical field");
              };
      row.setField(i, decoded);
    }
    return row;
  }

  private StreamTableEnvironment environment(FlussTestCluster cluster, boolean nativeRun) {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(PARALLELISM);
    env.enableCheckpointing(1000);
    env.getConfig().enableObjectReuse();
    StreamTableEnvironment tables = StreamTableEnvironment.create(env);
    tables
        .getConfig()
        .getConfiguration()
        .setString("table.exec.mini-batch.enabled", Boolean.toString(MINI_BATCH));
    if (MINI_BATCH) {
      tables.getConfig().getConfiguration().setString("table.exec.mini-batch.allow-latency", "2 s");
      tables.getConfig().getConfiguration().setString("table.exec.mini-batch.size", "50000");
    }
    if (BACKEND.equals("rocksdb")) {
      tables
          .getConfig()
          .getConfiguration()
          .setString(
              "state.backend.type",
              nativeRun ? "tech.streamfusion.state.RocksDBNativeStateBackendFactory" : "rocksdb");
      tables
          .getConfig()
          .getConfiguration()
          .setString("state.backend.rocksdb.localdir", rocksDirectory.toString());
      tables
          .getConfig()
          .getConfiguration()
          .setString("state.backend.rocksdb.memory.fixed-per-slot", "128 mb");
    }
    tables.getConfig().setLocalTimeZone(ZoneId.of("UTC"));
    tables.executeSql(
        "CREATE CATALOG fluss WITH ('type'='fluss', 'bootstrap.servers'='"
            + cluster.config().get(ConfigOptions.BOOTSTRAP_SERVERS).get(0)
            + "')");
    return tables;
  }

  private void assertRocksDBEngages(FlussTestCluster cluster, Connection connection)
      throws Exception {
    var query =
        Arrays.stream(NexmarkMatrixBenchmark.ALL_QUERIES)
            .filter(q -> q.label.equals("q4"))
            .findFirst()
            .orElseThrow();
    for (boolean nativeRun : new boolean[] {false, true}) {
      var seen = new AtomicBoolean();
      Thread watcher =
          new Thread(
              () -> {
                while (!Thread.currentThread().isInterrupted() && !seen.get()) {
                  try {
                    if (nativeRun) seen.set(Native.liveNativeHandles().contains("Rocks"));
                    else
                      try (var files = Files.walk(rocksDirectory)) {
                        seen.set(files.anyMatch(f -> f.getFileName().toString().equals("CURRENT")));
                      }
                    Thread.sleep(50);
                  } catch (InterruptedException stop) {
                    return;
                  } catch (UncheckedIOException failure) {
                    // RocksDB renames and removes temporary files while the observer traverses.
                    if (!(failure.getCause() instanceof NoSuchFileException)) throw failure;
                  } catch (IOException failure) {
                    throw new UncheckedIOException(failure);
                  }
                }
              },
              "fluss-rocksdb-preflight");
      watcher.setDaemon(true);
      watcher.start();
      try (Result result = run(cluster, connection, query, nativeRun)) {
        assertTrue(seen.get(), (nativeRun ? "Native" : "Stock") + " RocksDB did not engage");
        System.out.println(
            "[fluss] RocksDB engagement verified: " + (nativeRun ? "native" : "stock"));
      } finally {
        watcher.interrupt();
        watcher.join();
      }
    }
  }

  private record Result(double seconds, SortedOutput rows) implements AutoCloseable {
    @Override
    public void close() throws IOException {
      rows.close();
    }
  }

  private Result run(
      FlussTestCluster cluster,
      Connection connection,
      NexmarkMatrixBenchmark.Query query,
      boolean nativeRun)
      throws Exception {
    String name = "output_" + UUID.randomUUID().toString().replace("-", "");
    StreamTableEnvironment tables = environment(cluster, nativeRun);
    tables.executeSql("CREATE TEMPORARY VIEW src AS SELECT * FROM fluss.nexmark.events");
    NexmarkMatrixBenchmark.registerEventViews(tables);
    NexmarkMatrixBenchmark.runSetup(tables, query);
    String ddl = query.sinkDdl.replace("%TS%", "TIMESTAMP_LTZ(3)").replace("%WTS%", "TIMESTAMP(3)");
    String key = NexmarkMatrixBenchmark.UPSERT_KEYS.get(query.label);
    if (key != null) ddl = ddl.replace(") WITH", ", PRIMARY KEY (" + key + ") NOT ENFORCED) WITH");
    ddl =
        ddl.replace("CREATE TABLE sink", "CREATE TABLE fluss.nexmark." + name)
            .replace(
                "WITH ('connector' = 'blackhole')",
                "WITH ('bucket.num'='4', 'table.log.format'='ARROW')");
    tables.executeSql(ddl);
    String insert =
        query.insertSql.replace("INSERT INTO sink", "INSERT INTO fluss.nexmark." + name);
    PhysicalPlanScan scan = nativeRun ? NativePlanner.install(tables) : null;
    String plan = tables.explainSql(insert, ExplainDetail.JSON_EXECUTION_PLAN);
    Path plans = Path.of("target", "fluss-plans");
    Files.createDirectories(plans);
    Files.writeString(
        plans.resolve(query.label + (nativeRun ? "-native" : "-stock") + ".txt"), plan);
    if (nativeRun) {
      assertTrue(plan.contains("native-fluss-source"), scan.explainSummary());
      if (key == null) assertTrue(plan.contains("native-fluss-append-sink"), scan.explainSummary());
      else
        assertFalse(
            plan.contains("native-fluss-append-sink"), "Primary-key production must stay stock");
      assertFalse(plan.contains("RowDataToArrow"), scan.explainSummary());
      if (key == null) assertFalse(plan.contains("ArrowToRowData"), scan.explainSummary());
      int physical = plan.indexOf("== Physical Execution Plan ==");
      JsonNode execution = JSON.readTree(plan.substring(plan.indexOf('{', physical)));
      FlussPlanAssertions.assertNativeInterior(execution, key != null, query.label);
      if (query.label.equals("q3")) {
        assertTrue(plan.contains("NativeShare(consumers=[2])"), scan.explainSummary());
        assertEquals(
            1,
            plan.split("\"contents\" : \"Source: native-fluss-source\"", -1).length - 1,
            "q3 must share one columnar source between both join branches");
      }
    }
    String profiler = System.getProperty("profile.asprof");
    String pid = Long.toString(ProcessHandle.current().pid());
    if (profiler != null) {
      Path recordings = Path.of(System.getProperty("profile.outputDir", "target/profiles/fluss"));
      Files.createDirectories(recordings);
      Path recording =
          recordings.resolve(
              (nativeRun ? "streamfusion" : "flink")
                  + "-"
                  + query.label
                  + "-"
                  + UUID.randomUUID()
                  + ".jfr");
      NexmarkMatrixBenchmark.runProfiler(
          profiler,
          "start",
          "-e",
          System.getProperty("profile.event", "cpu"),
          "-i",
          "1ms",
          "-f",
          recording.toString(),
          pid);
      System.out.println("[fluss-profile] " + recording);
    }
    long start = System.nanoTime();
    double seconds;
    try {
      tables.executeSql(insert).await();
      seconds = (System.nanoTime() - start) / 1e9;
    } finally {
      if (profiler != null) NexmarkMatrixBenchmark.runProfiler(profiler, "stop", pid);
    }
    TablePath output = TablePath.of("nexmark", name);
    try {
      var info = connection.getAdmin().getTableInfo(output).get();
      assertEquals(key != null, info.hasPrimaryKey(), query.label + " output table mode");
      var getters = InternalRow.createFieldGetters(info.getRowType());
      SortedOutput result = new SortedOutput();
      var source = connection.getTable(output).newScan();
      if (!info.hasPrimaryKey()) source = source.limit(Integer.MAX_VALUE);
      try {
        try (var scanner = source.createBatchScanner()) {
          for (; ; ) {
            var batch = scanner.pollBatch(Duration.ofSeconds(30));
            if (batch == null) break;
            try (batch) {
              while (batch.hasNext()) {
                InternalRow row = batch.next();
                List<String> fields = new ArrayList<>();
                for (var getter : getters) fields.add(String.valueOf(getter.getFieldOrNull(row)));
                result.add(fields.toString());
              }
            }
          }
        }
        return new Result(seconds, result);
      } catch (Throwable failure) {
        try {
          result.close();
        } catch (Exception closing) {
          failure.addSuppressed(closing);
        }
        throw failure;
      }
    } finally {
      try {
        connection.getAdmin().dropTable(output, false).get();
      } finally {
        tables.getCatalog("fluss").orElseThrow().close();
      }
    }
  }
}
