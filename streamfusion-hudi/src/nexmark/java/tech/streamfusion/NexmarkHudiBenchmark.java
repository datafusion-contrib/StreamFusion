package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.junit.jupiter.api.io.TempDir;
import tech.streamfusion.planner.NativePlanner;

/** The shared Nexmark queries and event corpus, with four-partition Hudi tables at both ends. */
@EnabledIfEnvironmentVariable(named = "SF_HUDI_BENCH", matches = "true")
class NexmarkHudiBenchmark {
  private static final int PARALLELISM = 4;
  private static final long ROWS =
      Long.parseLong(System.getenv().getOrDefault("SF_ROWS", "2000000"));
  private static final int WARMUPS =
      Integer.parseInt(System.getenv().getOrDefault("SF_WARMUP", "1"));
  private static final int RUNS = Integer.parseInt(System.getenv().getOrDefault("SF_RUNS", "3"));
  private static final ObjectMapper JSON = new ObjectMapper();
  @TempDir Path temporary;
  private int sequence;
  private long inputBids;
  private String status = "running";
  private final List<Map<String, Object>> observations = new ArrayList<>();
  private final List<Map<String, Object>> stateChecks = new ArrayList<>();

  @AfterEach
  void releasesNativeResources() throws Exception {
    try {
      assertTrue(
          tech.streamfusion.Native.liveNativeHandles().isBlank(),
          tech.streamfusion.Native.liveNativeHandles());
      assertTrue(
          tech.streamfusion.parquet.NativeParquet.liveNativeHandles().isBlank(),
          tech.streamfusion.parquet.NativeParquet.liveNativeHandles());
      assertEquals(0, tech.streamfusion.operator.TaskOffHeapMemory.reservedBytes());
    } catch (AssertionError failure) {
      status = "failed";
      throw failure;
    } finally {
      report();
    }
  }

  @Test
  void fourPartitionSourceAndSinkMatrix() throws Exception {
    assertTrue(
        System.getProperty("java.library.path").contains("release"),
        "Use release native libraries and -Pbench");
    report();
    try {
      seed();
      for (String backend :
          System.getenv().getOrDefault("SF_HUDI_BACKENDS", "memory,rocksdb").split(",")) {
        assertTrue(List.of("memory", "rocksdb").contains(backend));
        for (String setting :
            System.getenv().getOrDefault("SF_HUDI_MINIBATCHES", "false,true").split(",")) {
          boolean miniBatch = Boolean.parseBoolean(setting);
          for (var query : NexmarkMatrixBenchmark.selectQueries()) {
            for (int trial = -WARMUPS; trial < RUNS; trial++) {
              List<String> engines = new ArrayList<>(List.of("stock", "previous", "native"));
              if (!query.label.equals("q7"))
                java.util.Collections.rotate(engines, Math.floorMod(trial, engines.size()));
              var results = new LinkedHashMap<String, Result>();
              try {
                boolean baselineRejected = false;
                for (String engine : engines) {
                  try {
                    System.out.printf(
                        "[hudi-nexmark] start %s %s miniBatch=%s trial=%d engine=%s%n",
                        query.label, backend, miniBatch, trial, engine);
                    results.put(
                        engine,
                        run(
                            query,
                            engine,
                            backend,
                            miniBatch,
                            query.label.equals("q4")
                                && backend.equals("rocksdb")
                                && trial == -WARMUPS));
                  } catch (org.apache.flink.table.planner.codegen.CodeGenException failure) {
                    if (!engine.equals("stock")
                        || !query.label.equals("q7")
                        || !failure
                            .getMessage()
                            .equals(
                                "Incomparable types: TIMESTAMP_LTZ(3) and TIMESTAMP(3) NOT NULL"))
                      throw failure;
                    observations.add(
                        Map.of(
                            "query",
                            query.label,
                            "backend",
                            backend,
                            "mini_batch",
                            miniBatch,
                            "status",
                            "stock-planner-rejected",
                            "reason",
                            failure.getMessage()));
                    report();
                    System.out.println(
                        "[hudi-nexmark] q7 stock planner rejected unchanged SQL: "
                            + failure.getMessage());
                    baselineRejected = true;
                    break;
                  }
                }
                if (baselineRejected) break;
                for (String engine : List.of("previous", "native")) {
                  if (!query.label.equals("q12"))
                    results
                        .get("stock")
                        .rows
                        .assertSame(results.get(engine).rows, query.label + "/" + engine);
                }
                if (trial >= 0) {
                  for (var entry : results.entrySet())
                    observations.add(
                        Map.of(
                            "query",
                            query.label,
                            "engine",
                            entry.getKey(),
                            "backend",
                            backend,
                            "mini_batch",
                            miniBatch,
                            "trial",
                            trial,
                            "milliseconds",
                            entry.getValue().milliseconds,
                            "output_rows",
                            entry.getValue().outputRows,
                            "sink_partitions",
                            entry.getValue().partitions,
                            "validation",
                            query.label.equals("q12")
                                ? "processing-time bounded counts"
                                : "full-value multiset"));
                  report();
                  System.out.println(
                      "[hudi-nexmark] "
                          + query.label
                          + " "
                          + backend
                          + " miniBatch="
                          + miniBatch
                          + " "
                          + results.entrySet().stream()
                              .map(e -> e.getKey() + "=" + e.getValue().milliseconds)
                              .collect(Collectors.joining(" ")));
                }
              } finally {
                for (var result : results.values()) result.close();
              }
            }
          }
        }
      }
      status = "passed";
    } catch (Exception | AssertionError failure) {
      status = "failed";
      throw failure;
    } finally {
      report();
    }
  }

  private StreamTableEnvironment environment(String engine, String backend, boolean miniBatch) {
    return environment(engine, backend, miniBatch, PARALLELISM, true);
  }

  private StreamTableEnvironment environment(
      String engine, String backend, boolean miniBatch, int parallelism, boolean checkpoints) {
    var memory = new Configuration();
    memory.set(TaskManagerOptions.TASK_OFF_HEAP_MEMORY, MemorySize.ofMebiBytes(1024));
    var env = StreamExecutionEnvironment.getExecutionEnvironment(memory);
    env.setParallelism(parallelism);
    if (checkpoints) env.enableCheckpointing(1000);
    if (!"false".equals(System.getenv("SF_OBJECT_REUSE"))) env.getConfig().enableObjectReuse();
    var tables = StreamTableEnvironment.create(env);
    tables.getConfig().setLocalTimeZone(ZoneId.of("UTC"));
    var config = tables.getConfig().getConfiguration();
    config.setString("table.exec.mini-batch.enabled", Boolean.toString(miniBatch));
    if (miniBatch) {
      config.setString("table.exec.mini-batch.allow-latency", "2 s");
      config.setString("table.exec.mini-batch.size", "50000");
    }
    if (backend.equals("rocksdb")) {
      config.setString(
          "state.backend.type",
          engine.equals("stock")
              ? "rocksdb"
              : "tech.streamfusion.state.RocksDBNativeStateBackendFactory");
      config.setString(
          "state.backend.rocksdb.localdir", temporary.resolve("rocks-" + sequence++).toString());
      config.setString("state.backend.rocksdb.memory.fixed-per-slot", "128 mb");
    }
    if (engine.equals("previous")) {
      config.setString("streamfusion.operator.hudiSource.enabled", "false");
      config.setString("streamfusion.operator.hudiSink.enabled", "false");
    }
    return tables;
  }

  private String options(Path path, boolean updating, boolean metadata) {
    return "'connector'='hudi','path'='"
        + path.toUri()
        + "','table.type'='"
        + (updating ? "MERGE_ON_READ" : "COPY_ON_WRITE")
        + "','write.operation'='"
        + (updating ? "upsert" : "insert")
        + "','write.tasks'='4','read.tasks'='4','read.source-v2.enabled'='true','metadata.enabled'='false',"
        + "'hoodie.allow.empty.commit'='true','hoodie.populate.meta.fields'='"
        + metadata
        + "','hoodie.parquet.block.size'='8388608',"
        + "'hoodie.parquet.page.size'='1048576','hoodie.parquet.compression.codec'='snappy'"
        + (updating
            ? ",'index.type'='BUCKET','hoodie.bucket.index.num.buckets'='4','write.precombine'='true',"
                  + "'hoodie.write.record.merge.mode'='COMMIT_TIME_ORDERING',"
                  + "'hoodie.logfile.data.block.format'='parquet','compaction.async.enabled'='false','compaction.schedule.enabled'='false'"
            : "");
  }

  private String sourceDdl() {
    return "CREATE TABLE events ("
        + NexmarkKafkaBenchmark.SCHEMA
        + ",sf_partition INT,rowtime AS TO_TIMESTAMP_LTZ(`dateTime`,3),WATERMARK FOR rowtime AS"
        + " rowtime - INTERVAL '4' SECOND) PARTITIONED BY(sf_partition) WITH ("
        + options(temporary.resolve("events"), false, true)
        + ",'hoodie.datasource.write.recordkey.field'='dateTime','hoodie.parquet.max.file.size'='1073741824',"
        + "'write.insert.partitioner.class.name'='org.apache.hudi.sink.partitioner.GroupedInsertPartitioner',"
        + "'write.insert.partitioner.default_parallelism_per_partition'='1')";
  }

  private void seed() throws Exception {
    Path corpus = temporary.resolve("corpus");
    Files.createDirectories(corpus);
    try (var out = Files.newBufferedWriter(corpus.resolve("events.json"))) {
      for (long row = 0; row < ROWS; row++) {
        String event = NexmarkKafkaBenchmark.event(row);
        if (event.startsWith("{\"event_type\":2,")) inputBids++;
        out.write(event.substring(0, event.length() - 1) + ",\"sf_partition\":" + row % 4 + "}\n");
      }
    }
    var tables = environment("stock", "memory", false, 1, false);
    tables.executeSql(
        "CREATE TABLE corpus ("
            + NexmarkKafkaBenchmark.SCHEMA
            + ",sf_partition INT) WITH ('connector'='filesystem','path'='"
            + corpus.toUri()
            + "','format'='json')");
    tables.executeSql(sourceDdl());
    tables.executeSql("INSERT INTO events SELECT * FROM corpus").await();
    awaitCommits(temporary.resolve("events"));
    try (var files = payloadFiles(temporary.resolve("events"))) {
      var baseFiles =
          files.filter(path -> path.toString().endsWith(".parquet")).collect(Collectors.toList());
      assertEquals(4, baseFiles.size(), "Expected one immutable source file per partition");
      assertEquals(4, baseFiles.stream().map(Path::getParent).distinct().count());
    }
    remove(corpus);
  }

  private Result run(
      NexmarkMatrixBenchmark.Query query,
      String engine,
      String backend,
      boolean miniBatch,
      boolean verifyBackend)
      throws Exception {
    var tables = environment(engine, backend, miniBatch);
    tables.executeSql(sourceDdl());
    tables.executeSql("CREATE TEMPORARY VIEW src AS SELECT * FROM events");
    NexmarkMatrixBenchmark.registerEventViews(tables);
    NexmarkMatrixBenchmark.runSetup(tables, query);
    String original =
        query.sinkDdl.replace("%TS%", "TIMESTAMP_LTZ(3)").replace("%WTS%", "TIMESTAMP(3)");
    tables.executeSql(original);
    var schema = tables.from("sink").getResolvedSchema();
    var columns = schema.getColumnNames();
    String key = NexmarkMatrixBenchmark.UPSERT_KEYS.get(query.label);
    String partitionKey = key == null ? quote(columns.get(0)) : key.split(",")[0].trim();
    Path target = temporary.resolve("result-" + sequence++);
    try {
      tables.executeSql("DROP TABLE sink");
      String fields =
          original.substring("CREATE TABLE sink (".length(), original.lastIndexOf(") WITH"));
      String ddl =
          "CREATE TABLE sink ("
              + fields
              + ",sf_partition INT"
              + (key == null ? "" : ",PRIMARY KEY(" + key + ",sf_partition) NOT ENFORCED")
              + ") PARTITIONED BY(sf_partition) WITH ("
              + options(target, key != null, key != null)
              + (key == null
                  ? ",'hoodie.datasource.write.recordkey.field'='" + columns.get(0) + "'"
                  : "")
              + ")";
      tables.executeSql(ddl);
      String select = query.insertSql.substring("INSERT INTO sink ".length());
      String insert =
          "INSERT INTO sink SELECT results.*,CAST(MOD(LOCATE(SUBSTRING(MD5(CAST("
              + partitionKey
              + " AS STRING)),1,1),'0123456789abcdef')-1,4) AS INT) AS sf_partition FROM ("
              + select
              + ") AS results("
              + columns.stream().map(NexmarkHudiBenchmark::quote).collect(Collectors.joining(","))
              + ")";
      var scan = engine.equals("stock") ? null : NativePlanner.install(tables);
      String plan = tables.explainSql(insert);
      if (engine.equals("native")) {
        assertTrue(plan.contains("NativeHudiSource"), plan + "\n" + scan.explainSummary());
        assertTrue(plan.contains("NativeHudiSink"), plan + "\n" + scan.explainSummary());
        assertFalse(plan.contains("RowDataToArrow"), plan);
        assertFalse(plan.contains("ArrowToRow"), plan);
        if (List.of("q3", "q4", "q20", "q23").contains(query.label))
          assertTrue(plan.contains("watermarkGranularity=[batch]"), plan);
      }
      var opened = new java.util.concurrent.atomic.AtomicBoolean();
      Path stateDirectory =
          backend.equals("rocksdb")
              ? Path.of(
                  tables
                      .getConfig()
                      .getConfiguration()
                      .getString("state.backend.rocksdb.localdir", ""))
              : null;
      Thread watcher =
          verifyBackend
              ? NexmarkMatrixBenchmark.engagementWatcher(
                  opened, () -> databaseOpened(stateDirectory))
              : null;
      double milliseconds;
      long start = System.nanoTime();
      try {
        tables.executeSql(insert).await();
        awaitCommits(target);
        milliseconds = (System.nanoTime() - start) / 1e6;
      } finally {
        if (watcher != null) {
          watcher.interrupt();
          watcher.join();
        }
      }
      System.out.printf(
          "[hudi-nexmark] finished %s engine=%s %.3f ms; validating output%n",
          query.label, engine, milliseconds);
      if (verifyBackend) {
        assertTrue(opened.get(), "q4 did not open RocksDB for " + engine);
        stateChecks.add(
            Map.of(
                "query",
                query.label,
                "engine",
                engine,
                "mini_batch",
                miniBatch,
                "rocksdb_opened",
                true));
      }
      var output = new SortedOutput();
      boolean success = false;
      try {
        var reader = environment("stock", "memory", false);
        reader.executeSql(ddl);
        long processingTimeCounts = 0;
        long outputRows = 0;
        var partitions = new java.util.HashSet<Integer>();
        try (var rows =
            reader
                .executeSql(
                    "SELECT "
                        + columns.stream()
                            .map(NexmarkHudiBenchmark::quote)
                            .collect(Collectors.joining(","))
                        + ",sf_partition FROM sink")
                .collect()) {
          while (rows.hasNext()) {
            var row = rows.next();
            output.add(row.toString());
            outputRows++;
            int partition = ((Number) row.getField(columns.size())).intValue();
            assertTrue(partition >= 0 && partition < 4);
            partitions.add(partition);
            if (query.label.equals("q12")) {
              long count = ((Number) row.getField(1)).longValue();
              assertTrue(count > 0 && count <= inputBids);
              processingTimeCounts += count;
            }
          }
        }
        if (query.label.equals("q12")) assertTrue(processingTimeCounts <= inputBids);
        if (query.label.equals("q0")) {
          assertEquals(
              inputBids,
              outputRows,
              () -> query.label + "/" + engine + " physical output: " + parquetRows(target));
          if (ROWS >= 1000)
            assertEquals(4, partitions.size(), "q0 must populate all four sink partitions");
        }
        verifyEncoding(target, engine.equals("native"), outputRows);
        success = true;
        return new Result(milliseconds, output, target, outputRows, partitions);
      } finally {
        if (!success) {
          output.close();
          remove(target);
        }
      }
    } catch (Throwable failure) {
      try {
        remove(target);
      } catch (Exception cleanup) {
        failure.addSuppressed(cleanup);
      }
      throw failure;
    }
  }

  private static boolean databaseOpened(Path directory) {
    if (!Files.exists(directory)) return false;
    try (var paths = Files.walk(directory)) {
      return paths.anyMatch(path -> path.getFileName().toString().equals("CURRENT"));
    } catch (java.io.IOException | java.io.UncheckedIOException unavailable) {
      return false;
    }
  }

  private static Map<String, Long> parquetRows(Path target) {
    var counts = new java.util.TreeMap<String, Long>();
    try (var files = payloadFiles(target)) {
      for (Path file :
          files.filter(p -> p.toString().endsWith(".parquet")).collect(Collectors.toList())) {
        try (var reader =
            org.apache.parquet.hadoop.ParquetFileReader.open(
                org.apache.parquet.hadoop.util.HadoopInputFile.fromPath(
                    new org.apache.hadoop.fs.Path(file.toUri()),
                    new org.apache.hadoop.conf.Configuration()))) {
          counts.merge("physical_rows", reader.getRecordCount(), Long::sum);
          counts.merge("physical_files", 1L, Long::sum);
          counts.merge(
              "instant_"
                  + org.apache.hudi.common.fs.FSUtils.getCommitTime(file.getFileName().toString()),
              reader.getRecordCount(),
              Long::sum);
        }
      }
    } catch (Exception failure) {
      throw new IllegalStateException("Cannot inspect Hudi output", failure);
    }
    return counts;
  }

  private static void awaitCommits(Path target) {
    var conf = new Configuration();
    conf.setString("path", target.toUri().toString());
    var meta = org.apache.hudi.util.StreamerUtil.createMetaClient(conf);
    var wait =
        org.apache.hudi.sink.utils.TimeWait.builder()
            .timeout(60000)
            .interval(20)
            .action("Hudi output commits after bounded job completion")
            .build();
    while (!meta.reloadActiveTimeline().getCommitsTimeline().filterInflightsAndRequested().empty())
      wait.waitFor();
  }

  private static java.util.stream.Stream<Path> payloadFiles(Path target)
      throws java.io.IOException {
    return Files.walk(target)
        .filter(Files::isRegularFile)
        .filter(
            file ->
                !file.startsWith(
                    target.resolve(
                        org.apache.hudi.common.table.HoodieTableMetaClient.METAFOLDER_NAME)))
        .filter(
            file ->
                !file.getFileName()
                    .toString()
                    .startsWith(
                        org.apache.hudi.common.model.HoodiePartitionMetadata
                            .HOODIE_PARTITION_METAFILE_PREFIX));
  }

  private static void verifyEncoding(Path target, boolean nativeEncoder, long outputRows)
      throws Exception {
    var conf = new org.apache.flink.configuration.Configuration();
    conf.setString("path", target.toUri().toString());
    var meta = org.apache.hudi.util.StreamerUtil.createMetaClient(conf);
    var storage = meta.getStorage();
    boolean found = false;
    try (var files = payloadFiles(target)) {
      for (var file : files.filter(Files::isRegularFile).collect(Collectors.toList())) {
        if (file.toString().endsWith(".parquet")) {
          found = true;
          var footer =
              org.apache.hudi.common.util.ParquetUtils.readMetadata(
                  storage, new org.apache.hudi.storage.StoragePath(file.toUri()));
          assertEquals(
              nativeEncoder,
              footer.getFileMetaData().getCreatedBy().contains("parquet-rs"),
              file + ": " + footer.getFileMetaData().getCreatedBy());
        } else if (file.getFileName().toString().contains(".log.")
            && !file.getFileName().toString().endsWith(".crc")) {
          try (var blocks =
              org.apache.hudi.common.table.log.HoodieLogFormat.newReader(
                  storage,
                  new org.apache.hudi.common.model.HoodieLogFile(
                      new org.apache.hudi.storage.StoragePath(file.toUri())),
                  new org.apache.hudi.common.table.TableSchemaResolver(meta).getTableSchema())) {
            while (blocks.hasNext()) {
              var block = blocks.next();
              if (block.getBlockType()
                  != org.apache.hudi.common.table.log.block.HoodieLogBlock.HoodieLogBlockType
                      .PARQUET_DATA_BLOCK) continue;
              found = true;
              var location = block.getBlockContentLocation().get();
              byte[] bytes = new byte[Math.toIntExact(location.getBlockSize())];
              try (var input =
                  storage.openSeekable(
                      new org.apache.hudi.storage.StoragePath(file.toUri()), true)) {
                input.seek(location.getContentPositionInLogFile());
                input.readFully(bytes);
              }
              try (var parquet =
                  org.apache.parquet.hadoop.ParquetFileReader.open(
                      new org.apache.hudi.parquet.io.ByteBufferBackedInputFile(
                          bytes, 0, bytes.length))) {
                assertEquals(
                    nativeEncoder,
                    parquet.getFooter().getFileMetaData().getCreatedBy().contains("parquet-rs"));
              }
            }
          }
        }
      }
    }
    if (outputRows > 0) assertTrue(found, "Nonempty Hudi output has no verified Parquet payload");
  }

  private void report() throws Exception {
    Path report =
        Path.of(System.getProperty("sf.hudi.benchmark.output", "target/hudi-nexmark.json"));
    Files.createDirectories(report.toAbsolutePath().getParent());
    var data = new LinkedHashMap<String, Object>();
    data.put("status", status);
    data.put("rows", ROWS);
    data.put("input_bids", inputBids);
    data.put("parallelism", PARALLELISM);
    data.put("source_partitions", 4);
    data.put("sink_partition_modulus", 4);
    data.put("checkpoint_ms", 1000);
    data.put("warmups", WARMUPS);
    data.put("runs", RUNS);
    data.put("object_reuse", !"false".equals(System.getenv("SF_OBJECT_REUSE")));
    data.put("jdk", System.getProperty("java.runtime.version"));
    data.put("architecture", System.getProperty("os.arch"));
    data.put("available_processors", Runtime.getRuntime().availableProcessors());
    data.put("heap_max_bytes", Runtime.getRuntime().maxMemory());
    data.put(
        "native_task_off_heap_capacity_bytes",
        tech.streamfusion.operator.TaskOffHeapMemory.capacityBytes());
    data.put("state_backend_checks", stateChecks);
    data.put("observations", observations);
    JSON.writerWithDefaultPrettyPrinter().writeValue(report.toFile(), data);
  }

  private static String quote(String name) {
    return "`" + name.replace("`", "``") + "`";
  }

  private static void remove(Path root) throws Exception {
    if (!Files.exists(root)) return;
    try (var paths = Files.walk(root)) {
      for (Path path : paths.sorted(Comparator.reverseOrder()).collect(Collectors.toList()))
        Files.delete(path);
    }
  }

  private static final class Result implements AutoCloseable {
    final double milliseconds;
    final SortedOutput rows;
    final Path target;
    final long outputRows;
    final List<Integer> partitions;

    Result(
        double milliseconds,
        SortedOutput rows,
        Path target,
        long outputRows,
        java.util.Set<Integer> partitions) {
      this.milliseconds = milliseconds;
      this.rows = rows;
      this.target = target;
      this.outputRows = outputRows;
      this.partitions = partitions.stream().sorted().collect(Collectors.toList());
    }

    public void close() throws Exception {
      try (var owned = rows) {
        remove(target);
      }
    }
  }
}
