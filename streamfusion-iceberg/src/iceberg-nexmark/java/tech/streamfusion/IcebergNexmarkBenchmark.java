package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.ZoneId;
import java.util.*;
import java.util.concurrent.TimeUnit;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.types.Row;
import org.apache.iceberg.*;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericFileWriterFactory;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import tech.streamfusion.planner.NativePlanner;

/**
 * Separate streaming Iceberg workload; the headline corpus, query SQL and harness stay unchanged.
 */
@EnabledIfEnvironmentVariable(named = "SF_ICEBERG_BENCHMARK", matches = "true")
class IcebergNexmarkBenchmark {
  private static final ObjectMapper JSON = new ObjectMapper();
  private static final long EVENTS =
      Long.parseLong(System.getenv().getOrDefault("SF_ROWS", "2000000"));
  private static final int PARALLELISM =
      Integer.parseInt(System.getenv().getOrDefault("SF_PARALLELISM", "4"));
  private static final int STRIPE =
      Integer.parseInt(System.getenv().getOrDefault("SF_ICEBERG_STRIPE_EVENTS", "50000"));
  private static final int WARMUPS =
      Integer.parseInt(System.getenv().getOrDefault("SF_WARMUP", "1"));
  private static final int RUNS = Integer.parseInt(System.getenv().getOrDefault("SF_RUNS", "3"));
  private static final boolean IO_BASELINE =
      Boolean.parseBoolean(System.getenv().getOrDefault("SF_ICEBERG_IO_BASELINE", "false"));
  private static final boolean SNAPSHOT_COMPRESSION =
      Boolean.parseBoolean(
          System.getenv().getOrDefault("SF_ICEBERG_SNAPSHOT_COMPRESSION", "false"));
  private static final Map<String, String> FILE_SETTINGS =
      Map.of(
          "format-version",
          "2",
          "write.format.default",
          "parquet",
          "write.parquet.compression-codec",
          "zstd",
          "write.parquet.compression-level",
          "3",
          "write.parquet.row-group-size-bytes",
          "134217728",
          "write.parquet.page-size-bytes",
          "1048576",
          "write.parquet.dict-size-bytes",
          "2097152",
          "write.target-file-size-bytes",
          "536870912",
          "write.distribution-mode",
          System.getenv().getOrDefault("SF_ICEBERG_DISTRIBUTION", "hash"));
  private static final boolean KAFKA_SOURCE =
      "kafka".equals(System.getenv().getOrDefault("SF_ICEBERG_SOURCE", "iceberg"));
  private String brokers;
  private final Map<Long, Long> bidCounts = new TreeMap<>();
  private Map<Long, Long> processingTimeCounts = Map.of();
  private Path artifacts;
  private Path warehouse;
  private HadoopCatalog catalog;
  private Set<Long> snapshots;
  private final Map<String, org.apache.iceberg.Schema> outputSchemas = new HashMap<>();

  @Test
  void compare() throws Exception {
    assertFalse(
        Boolean.getBoolean("streamfusion.exchange.zeroCopyLocal"),
        "benchmark requires Arrow IPC shuffles");
    assertEquals(
        "true",
        System.getenv("SF_ICEBERG_RELEASE_CONFIRMED"),
        "use bin/bench-iceberg.sh to stage release+mimalloc libraries");
    artifacts =
        Path.of(System.getenv().getOrDefault("SF_ICEBERG_ARTIFACTS", "target/iceberg-nexmark"))
            .toAbsolutePath();
    Files.createDirectories(artifacts);
    warehouse = artifacts.resolve("warehouse");
    try (var kafka =
            KAFKA_SOURCE
                ? new org.testcontainers.containers.KafkaContainer(
                    org.testcontainers.utility.DockerImageName.parse("confluentinc/cp-kafka:7.6.1"))
                : null;
        var loaded =
            new HadoopCatalog(
                new org.apache.hadoop.conf.Configuration(), warehouse.toUri().toString())) {
      catalog = loaded;
      catalog.createNamespace(org.apache.iceberg.catalog.Namespace.of("bench"));
      if (kafka != null) {
        kafka.start();
        brokers = kafka.getBootstrapServers();
        NexmarkKafkaBenchmark.produce(brokers, "nexmark", "json", EVENTS, 4);
      }
      seed();
      var metadata = new LinkedHashMap<String, Object>();
      metadata.put("events", EVENTS);
      metadata.put("parallelism", PARALLELISM);
      metadata.put("partitions", 4);
      metadata.put("stripeEvents", STRIPE);
      metadata.put("source", KAFKA_SOURCE ? "kafka-json" : "iceberg");
      metadata.put(
          "inputDescription",
          KAFKA_SOURCE
              ? "four-partition Kafka JSON topic; earliest to captured latest offsets"
              : "four-bucket Iceberg input tables");
      metadata.put("runtimeMode", "STREAMING");
      metadata.put("sessionTimeZone", "UTC");
      metadata.put(
          "watermarks",
          KAFKA_SOURCE
              ? "TO_TIMESTAMP_LTZ(dateTime, 3) minus 4 seconds, unchanged Kafka harness"
              : "Iceberg ordered file lower bounds via SOURCE_WATERMARK()");
      metadata.put("watermarkDelayMillis", IcebergCorpusCompletion.WATERMARK_DELAY_MILLIS);
      metadata.put("warmups", WARMUPS);
      metadata.put("runs", RUNS);
      metadata.put("baselineEngine", IO_BASELINE ? "streamfusion-stock-io" : "flink");
      metadata.put("baselineLabel", IO_BASELINE ? "StreamFusion with stock Iceberg IO" : "Flink");
      metadata.put(
          "completionProtocol",
          KAFKA_SOURCE
              ? "finite Kafka offsets, streaming end-of-input and final Iceberg commit"
              : "configured checkpoint then draining native savepoint");
      metadata.put("checkpointStorage", "shared MiniCluster filesystem default");
      metadata.put("objectReuse", true);
      metadata.put(
          "managedMemory",
          System.getenv().getOrDefault("SF_BENCH_MANAGED_MEMORY", "cluster default"));
      metadata.put(
          "clusterSlots", System.getenv("SF_BENCH_MANAGED_MEMORY") == null ? 8 : PARALLELISM);
      metadata.put("snapshotCompression", SNAPSHOT_COMPRESSION);
      metadata.put(
          "profileEvent", System.getenv().getOrDefault("SF_ICEBERG_PROFILE_EVENT", "none"));
      metadata.put("fileSettings", FILE_SETTINGS);
      metadata.put("snapshots", snapshots);
      metadata.put("java", System.getProperty("java.version"));
      metadata.put("maxJavaHeapBytes", Runtime.getRuntime().maxMemory());
      metadata.put("os", System.getProperty("os.name") + " " + System.getProperty("os.arch"));
      metadata.put("cpuCount", Runtime.getRuntime().availableProcessors());
      Set<String> modes = selected("SF_ICEBERG_MODES", "memory-off,memory-on,disk-off,disk-on");
      Set<String> queries =
          selected(
              "SF_MATRIX_QUERIES",
              String.join(
                  ",",
                  Arrays.stream(NexmarkMatrixBenchmark.ALL_QUERIES).map(q -> q.label).toList()));
      var expected = new ArrayList<List<String>>();
      for (var query : NexmarkMatrixBenchmark.ALL_QUERIES) {
        if (!queries.contains(query.label)) continue;
        for (String mode : modes) {
          expected.add(List.of(query.label, mode, "exact"));
          if (query.nativeVariantProps != null
              && !"false".equals(System.getenv("SF_ICEBERG_VARIANTS")))
            expected.add(List.of(query.label, mode, query.nativeVariantLabel));
        }
      }
      metadata.put("expectedCells", expected);
      var completed = new HashSet<List<String>>();
      String completedPath = System.getenv("SF_ICEBERG_COMPLETED_CELLS");
      if (completedPath != null)
        for (String line : Files.readAllLines(Path.of(completedPath)))
          completed.add(Arrays.asList(JSON.readValue(line, String[].class)));
      assertTrue(expected.containsAll(completed), "resume cells must belong to this matrix");
      metadata.put("resumedCells", completed);
      Files.writeString(
          artifacts.resolve("method.json"),
          JSON.writerWithDefaultPrettyPrinter().writeValueAsString(metadata));
      for (var query : NexmarkMatrixBenchmark.ALL_QUERIES) {
        if (!queries.contains(query.label)) continue;
        var variants = new LinkedHashMap<String, Map<String, String>>();
        variants.put("exact", Map.of());
        if (query.nativeVariantProps != null
            && !"false".equals(System.getenv("SF_ICEBERG_VARIANTS")))
          variants.put(query.nativeVariantLabel, query.nativeVariantProps);
        for (String mode : List.of("memory-off", "memory-on", "disk-off", "disk-on")) {
          if (!modes.contains(mode)) continue;
          for (var variant : variants.entrySet()) {
            if (completed.contains(List.of(query.label, mode, variant.getKey()))) continue;
            for (int trial = -WARMUPS; trial < RUNS; trial++) {
              var pair = new HashMap<Boolean, Fingerprint>();
              for (boolean nativeMode :
                  trial % 2 == 0 ? List.of(false, true) : List.of(true, false)) {
                pair.put(
                    nativeMode,
                    run(query, mode, variant.getKey(), variant.getValue(), trial, nativeMode));
              }
              assertEquals(
                  pair.get(false),
                  pair.get(true),
                  query.label + " " + mode + " " + variant.getKey() + " final committed state");
              appendJson(
                  Map.of(
                      "kind",
                      "parity",
                      "query",
                      query.label,
                      "mode",
                      mode,
                      "variant",
                      variant.getKey(),
                      "trial",
                      trial,
                      "matched",
                      true));
            }
            String acknowledgement = System.getenv("SF_ICEBERG_CELL_ACK_DIRECTORY");
            if (acknowledgement != null) {
              Path ack =
                  Path.of(acknowledgement, query.label + "-" + mode + "-" + variant.getKey());
              long deadline = System.nanoTime() + TimeUnit.MINUTES.toNanos(5);
              while (!Files.exists(ack) && System.nanoTime() < deadline) Thread.sleep(100);
              assertTrue(Files.exists(ack), "evidence checkpoint acknowledgement timed out");
            }
          }
        }
      }
    }
  }

  private static Set<String> selected(String name, String defaults) {
    return new LinkedHashSet<>(
        Arrays.asList(System.getenv().getOrDefault(name, defaults).split(",")));
  }

  private void seed() throws Exception {
    var template = (StreamTableEnvironment) NexmarkBenchmark.environment(EVENTS);
    if (!KAFKA_SOURCE) seedIcebergInputs(template);
    if (KAFKA_SOURCE) {
      snapshots = Set.of();
      for (long index = 0; index < EVENTS; index++) {
        int position = (int) (index % 50);
        if (position >= 4) bidCounts.merge((long) position, 1L, Long::sum);
      }
    }
    for (var query : NexmarkMatrixBenchmark.ALL_QUERIES) {
      template.executeSql("DROP TABLE IF EXISTS sink");
      template.executeSql(
          query
              .sinkDdl
              .replace("%TS%", KAFKA_SOURCE ? "TIMESTAMP_LTZ(3)" : "TIMESTAMP(3)")
              .replace("%WTS%", "TIMESTAMP(3)"));
      var schema = FlinkSchemaUtil.convert(template.from("sink").getResolvedSchema());
      String keys = NexmarkMatrixBenchmark.UPSERT_KEYS.get(query.label);
      if (keys != null) {
        Set<String> names =
            new HashSet<>(Arrays.asList(keys.replace("`", "").replace(" ", "").split(",")));
        var columns =
            schema.columns().stream()
                .map(
                    field ->
                        names.contains(field.name())
                            ? Types.NestedField.required(
                                field.fieldId(), field.name(), field.type())
                            : field)
                .toList();
        var identifiers =
            columns.stream()
                .filter(field -> names.contains(field.name()))
                .map(Types.NestedField::fieldId)
                .collect(java.util.stream.Collectors.toSet());
        schema = new org.apache.iceberg.Schema(columns, identifiers);
      }
      outputSchemas.put(query.label, schema);
    }
    appendJson(
        Map.of("kind", "seed", "events", EVENTS, "snapshots", snapshots, "bidCounts", bidCounts));
  }

  private void seedIcebergInputs(StreamTableEnvironment template) throws Exception {
    String[] kinds = {"person", "auction", "bid"};
    var tables = new LinkedHashMap<String, Table>();
    var appends = new HashMap<String, AppendFiles>();
    for (String kind : kinds) {
      var rowType =
          (RowType)
              template.from(kind).getResolvedSchema().toPhysicalRowDataType().getLogicalType();
      var schema = FlinkSchemaUtil.convert(FlinkSchemaUtil.toResolvedSchema(rowType));
      String key = kind.equals("bid") ? "auction" : "id";
      var spec = PartitionSpec.builderFor(schema).bucket(key, 4).build();
      var table =
          catalog.createTable(
              TableIdentifier.of("bench", "input_" + kind), schema, spec, FILE_SETTINGS);
      tables.put(kind, table);
      appends.put(kind, table.newAppend());
    }
    var event = NexmarkBenchmark.class.getDeclaredMethod("event", long.class);
    event.setAccessible(true);
    for (long start = 0; start < EVENTS; start += STRIPE) {
      var writers = new HashMap<String, Map<Integer, DataWriter<org.apache.iceberg.data.Record>>>();
      try {
        for (long index = start; index < Math.min(EVENTS, start + STRIPE); index++) {
          Row outer = (Row) event.invoke(null, index);
          int kindIndex = (Integer) outer.getField(0);
          String kind = kinds[kindIndex];
          Row value = (Row) outer.getField(kindIndex + 1);
          Table table = tables.get(kind);
          var record = GenericRecord.create(table.schema());
          for (int field = 0; field < value.getArity(); field++)
            record.set(field, value.getField(field));
          if (kind.equals("bid")) bidCounts.merge((Long) record.getField("bidder"), 1L, Long::sum);
          var partition = new PartitionKey(table.spec(), table.schema());
          partition.partition(record);
          int bucket = partition.get(0, Integer.class);
          var destination = writers.computeIfAbsent(kind, ignored -> new HashMap<>());
          var writer = destination.get(bucket);
          if (writer == null) {
            var factory =
                new GenericFileWriterFactory.Builder(table)
                    .dataFileFormat(FileFormat.PARQUET)
                    .build();
            writer =
                factory.newDataWriter(
                    table
                        .encryption()
                        .encrypt(
                            table
                                .io()
                                .newOutputFile(
                                    table.location()
                                        + "/seed-"
                                        + start
                                        + "-"
                                        + bucket
                                        + ".parquet")),
                    table.spec(),
                    partition.copy());
            destination.put(bucket, writer);
          }
          writer.write(record);
        }
      } finally {
        for (var byKind : writers.entrySet())
          for (var writer : byKind.getValue().values()) {
            writer.close();
            appends.get(byKind.getKey()).appendFile(writer.toDataFile());
          }
      }
    }
    snapshots = new HashSet<>();
    for (String kind : kinds) {
      appends.get(kind).commit();
      var table = tables.get(kind);
      snapshots.add(table.currentSnapshot().snapshotId());
      var occupied = new HashSet<Integer>();
      try (var tasks = table.newScan().planFiles()) {
        for (var task : tasks) occupied.add(task.file().partition().get(0, Integer.class));
      }
      assertEquals(
          Set.of(0, 1, 2, 3), occupied, "input must occupy four physical partitions: " + kind);
    }
  }

  private Fingerprint run(
      NexmarkMatrixBenchmark.Query query,
      String mode,
      String variant,
      Map<String, String> extra,
      int trial,
      boolean nativeMode)
      throws Exception {
    boolean nativeQuery = nativeMode || IO_BASELINE;
    for (String operator : List.of("icebergSource", "icebergSink")) {
      String property = "streamfusion.operator." + operator + ".enabled";
      if (nativeMode) System.clearProperty(property);
      else System.setProperty(property, "false");
    }
    String token =
        query.label
            + "_"
            + mode.replace('-', '_')
            + "_"
            + (nativeMode ? "sf" : IO_BASELINE ? "sf_stock_io" : "flink")
            + "_"
            + UUID.randomUUID().toString().replace("-", "");
    Path trialPath = artifacts.resolve(token);
    Files.createDirectories(trialPath);
    var schema = outputSchemas.get(query.label);
    String bucket =
        schema.identifierFieldIds().isEmpty()
            ? schema.columns().get(0).name()
            : schema.identifierFieldNames().stream().sorted().findFirst().orElseThrow();
    var settings = new HashMap<>(FILE_SETTINGS);
    if (!schema.identifierFieldIds().isEmpty()) settings.put("write.upsert.enabled", "true");
    var output =
        catalog.createTable(
            TableIdentifier.of("bench", token),
            schema,
            PartitionSpec.builderFor(schema).bucket(bucket, 4).build(),
            settings);
    if (FILE_SETTINGS.get("write.distribution-mode").equals("range")
        && schema.identifierFieldIds().isEmpty()) output.replaceSortOrder().asc(bucket).commit();
    var config =
        Configuration.fromMap(
            Map.of(
                "restart-strategy.type",
                "none",
                "state.backend.type",
                mode.startsWith("disk")
                    ? (nativeQuery
                        ? "tech.streamfusion.state.RocksDBNativeStateBackendFactory"
                        : "rocksdb")
                    : "hashmap"));
    config.setString("state.backend.rocksdb.localdir", trialPath.resolve("rocksdb").toString());
    config.set(
        org.apache.flink.configuration.ExecutionOptions.SNAPSHOT_COMPRESSION, SNAPSHOT_COMPRESSION);
    var env = StreamExecutionEnvironment.getExecutionEnvironment(config);
    env.setRuntimeMode(org.apache.flink.api.common.RuntimeExecutionMode.STREAMING);
    env.setParallelism(PARALLELISM);
    env.setMaxParallelism(128);
    env.enableCheckpointing(1000);
    env.getConfig().enableObjectReuse();
    var sql = StreamTableEnvironment.create(env);
    sql.getConfig().setLocalTimeZone(ZoneId.of("UTC"));
    var tableConfig = sql.getConfig().getConfiguration();
    tableConfig.setString("state.backend.type", config.getString("state.backend.type", "hashmap"));
    tableConfig.setString(
        "state.backend.rocksdb.localdir", trialPath.resolve("rocksdb").toString());
    tableConfig.setString("table.exec.mini-batch.enabled", Boolean.toString(mode.endsWith("on")));
    tableConfig.setString("table.exec.mini-batch.allow-latency", "2 s");
    tableConfig.setString("table.exec.mini-batch.size", "50000");
    extra.forEach(tableConfig::setString);
    if (KAFKA_SOURCE) kafkaInput(sql);
    else {
      for (String kind : List.of("person", "auction", "bid")) input(sql, kind);
      sql.createTemporarySystemFunction("count_char", NexmarkMatrixBenchmark.CountChar.class);
    }
    NexmarkMatrixBenchmark.runSetup(sql, query);
    var sinkSchema = Schema.newBuilder();
    var type = FlinkSchemaUtil.convert(schema);
    for (int field = 0; field < type.getFieldCount(); field++)
      sinkSchema.column(
          type.getFieldNames().get(field),
          TypeConversions.fromLogicalToDataType(type.getTypeAt(field)));
    if (!schema.identifierFieldIds().isEmpty())
      sinkSchema.primaryKey(schema.identifierFieldNames().toArray(String[]::new));
    sql.createTemporaryTable("sink", descriptor(token, sinkSchema.build(), settings));
    var scan = nativeQuery ? NativePlanner.install(sql) : null;
    System.out.println(
        "ICEBERG_TRIAL "
            + query.label
            + " "
            + mode
            + " "
            + variant
            + " "
            + trial
            + " "
            + nativeMode);
    var resources = new IcebergBenchmarkResources();
    profile("start", trialPath);
    long start = System.nanoTime();
    sql.createStatementSet().addInsertSql(query.insertSql).attachAsDataStream();
    var graph = env.getStreamGraph();
    graph.setJobName("iceberg-nexmark-" + token);
    var completion = IcebergCorpusCompletion.install(graph, snapshots, KAFKA_SOURCE);
    String plan =
        (scan == null ? "stock Flink" : scan.explainSummary())
            + (KAFKA_SOURCE ? "\nNative Kafka sources=" : "\nNative Iceberg sources=")
            + completion.nativeSourceCount()
            + ", writer factories="
            + completion.nativeWriterCount();
    if (IO_BASELINE && !nativeMode && !query.label.equals("q0")) {
      String operators = graph.getStreamingPlanAsJSON();
      assertTrue(operators.contains("RowDataToArrow"), operators);
      assertTrue(operators.contains("ArrowToRowData"), operators);
    }
    if (nativeMode) {
      assertEquals(completion.sourceCount(), completion.nativeSourceCount(), plan);
      assertTrue(completion.nativeWriterCount() > 0, plan);
    } else {
      assertEquals(0, completion.nativeSourceCount(), plan);
      assertEquals(0, completion.nativeWriterCount(), plan);
    }
    if (nativeMode && KAFKA_SOURCE) {
      String execution = graph.getStreamingPlanAsJSON();
      assertTrue(execution.contains("native-kafka-source"), execution);
      assertFalse(execution.contains("RowDataToArrow"), execution);
      assertFalse(execution.contains("ArrowToRowData"), execution);
    }
    var job = env.executeAsync(graph);
    boolean finished = false;
    boolean backendObserved = false;
    double progressSeconds = 0;
    double checkpointSeconds = 0;
    long settledCheckpointId = -1;
    Map<String, Long> checkpointObservation = Map.of();
    try {
      if (KAFKA_SOURCE) {
        long deadline = System.nanoTime() + TimeUnit.MINUTES.toNanos(10);
        while (!job.getJobStatus().get(10, TimeUnit.SECONDS).isGloballyTerminalState()
            && System.nanoTime() < deadline) {
          resources.sample();
          backendObserved |= backendObserved(mode, nativeQuery, trialPath);
          Thread.sleep(10);
        }
        job.getJobExecutionResult().get(10, TimeUnit.SECONDS);
        progressSeconds = (System.nanoTime() - start) / 1e9;
        if (mode.startsWith("disk") && query.label.equals("q4"))
          assertTrue(backendObserved, "q4 did not engage its requested RocksDB backend");
      } else {
        long deadline = System.nanoTime() + TimeUnit.MINUTES.toNanos(10);
        while ((!completion.complete() || !allTasksRunning(job)) && System.nanoTime() < deadline) {
          resources.sample();
          backendObserved |= backendObserved(mode, nativeQuery, trialPath);
          var status = job.getJobStatus().get(10, TimeUnit.SECONDS);
          if (status.isGloballyTerminalState()) {
            job.getJobExecutionResult().get();
            fail("job ended before corpus completion");
          }
          Thread.sleep(10);
        }
        assertTrue(completion.complete() && allTasksRunning(job), "corpus/task readiness timeout");
        backendObserved |= backendObserved(mode, nativeQuery, trialPath);
        if (mode.startsWith("disk") && query.label.equals("q4"))
          assertTrue(backendObserved, "q4 did not engage its requested RocksDB backend");
        if (query.label.equals("q12")) {
          long earliest = System.currentTimeMillis() + (mode.endsWith("on") ? 2500 : 0);
          long boundary = (earliest / 10000 + 1) * 10000 + 500;
          while (System.currentTimeMillis() < boundary) {
            resources.sample();
            Thread.sleep(10);
          }
        }
        progressSeconds = (System.nanoTime() - start) / 1e9;
        long checkpointStart = System.nanoTime();
        settledCheckpointId =
            cluster(job)
                .triggerCheckpoint(
                    job.getJobID(), org.apache.flink.core.execution.CheckpointType.CONFIGURED)
                .get(120, TimeUnit.SECONDS);
        checkpointSeconds = (System.nanoTime() - checkpointStart) / 1e9;
        var checkpointStats =
            cluster(job)
                .getExecutionGraph(job.getJobID())
                .get(10, TimeUnit.SECONDS)
                .getCheckpointStatsSnapshot()
                .getHistory()
                .getCheckpointById(settledCheckpointId);
        checkpointObservation =
            !(checkpointStats
                    instanceof org.apache.flink.runtime.checkpoint.CompletedCheckpointStats)
                ? Map.of("id", settledCheckpointId)
                : Map.of(
                    "id", settledCheckpointId,
                    "stateBytes", checkpointStats.getStateSize(),
                    "checkpointedBytes", checkpointStats.getCheckpointedSize(),
                    "durationMillis", checkpointStats.getEndToEndDuration());
        job.stopWithSavepoint(
                true, trialPath.resolve("savepoint").toUri().toString(), SavepointFormatType.NATIVE)
            .get(120, TimeUnit.SECONDS);
        job.getJobExecutionResult().get(120, TimeUnit.SECONDS);
      }
      finished = true;
    } finally {
      if (!finished) {
        job.cancel().get(30, TimeUnit.SECONDS);
        job.getJobExecutionResult().handle((result, failure) -> null).get(30, TimeUnit.SECONDS);
      }
    }
    double seconds = (System.nanoTime() - start) / 1e9;
    profile("stop", trialPath);
    var resourceObservation = resources.finish();
    output.refresh();
    var fingerprint = fingerprint(output, query.label);
    var occupied = new HashSet<Integer>();
    var dataFiles = new HashSet<String>();
    var deletes = new HashMap<String, DeleteFile>();
    try (var tasks = output.newScan().planFiles()) {
      for (var task : tasks) {
        occupied.add(task.file().partition().get(0, Integer.class));
        dataFiles.add(task.file().location());
        for (var delete : task.deletes()) deletes.put(delete.location(), delete);
      }
    }
    if (nativeMode) {
      if (!KAFKA_SOURCE)
        assertTrue(completion.nativeTasks() > 0, "no native Parquet task executed");
      assertEquals(
          0, completion.fallbackTasks(), "benchmark must expose a native Parquet source boundary");
      for (String file : dataFiles)
        assertTrue(createdBy(file).contains("parquet-rs"), "data file did not use Rust: " + file);
      for (String file : deletes.keySet())
        assertTrue(createdBy(file).contains("parquet-mr"), "delete file did not use Java: " + file);
    }
    if (!nativeMode)
      for (String file : dataFiles)
        assertTrue(
            createdBy(file).contains("parquet-mr"), "stock data file did not use Java: " + file);
    Files.writeString(trialPath.resolve("plan.txt"), plan);
    Files.writeString(trialPath.resolve("graph.json"), graph.getStreamingPlanAsJSON());
    var observation = new LinkedHashMap<String, Object>();
    observation.put("kind", "measurement");
    observation.put("query", query.label);
    observation.put("mode", mode);
    observation.put("variant", variant);
    observation.put("trial", trial);
    observation.put("warmup", trial < 0);
    observation.put(
        "engine", nativeMode ? "streamfusion" : IO_BASELINE ? "streamfusion-stock-io" : "flink");
    observation.put("seconds", seconds);
    observation.put("settledCheckpointId", settledCheckpointId);
    observation.put("settlingCheckpoint", checkpointObservation);
    observation.put("checkpointStatsPublished", checkpointObservation.containsKey("stateBytes"));
    observation.put("nativeQuery", nativeQuery);
    observation.put("nativeIcebergIo", nativeMode);
    observation.put("source", KAFKA_SOURCE ? "kafka-json" : "iceberg");
    observation.put(
        "timingPhases",
        Map.of(
            "submissionToCorpusAndWindowSeconds", progressSeconds,
            "settleCheckpointSeconds", checkpointSeconds,
            "drainCommitSeconds", seconds - progressSeconds - checkpointSeconds));
    observation.put("resources", resourceObservation);
    observation.put("rocksDBObserved", backendObserved);
    observation.put("fingerprint", fingerprint);
    if (query.label.equals("q12")) observation.put("processingTimeCounts", processingTimeCounts);
    observation.put("sourceCount", completion.sourceCount());
    observation.put("nativeSourceCount", completion.nativeSourceCount());
    observation.put("nativeWriterCount", completion.nativeWriterCount());
    observation.put("nativeParquetTasks", completion.nativeTasks());
    observation.put("stockFallbackTasks", completion.fallbackTasks());
    observation.put("dataFiles", dataFiles.size());
    observation.put("deleteFiles", deletes.size());
    observation.put("occupiedPartitions", occupied);
    observation.put("table", output.location());
    observation.put("fallbackReasons", scan == null ? List.of() : scan.fallbackReasons());
    appendJson(observation);
    SharedFlinkCluster.assertNativeMemoryReleased(token);
    return fingerprint;
  }

  private static boolean allTasksRunning(org.apache.flink.core.execution.JobClient job)
      throws Exception {
    var graph = cluster(job).getExecutionGraph(job.getJobID()).get(10, TimeUnit.SECONDS);
    for (var vertex : graph.getAllExecutionVertices()) {
      if (vertex.getExecutionState() != org.apache.flink.runtime.execution.ExecutionState.RUNNING)
        return false;
    }
    return true;
  }

  private static org.apache.flink.runtime.minicluster.MiniCluster cluster(
      org.apache.flink.core.execution.JobClient job) throws Exception {
    if (!(job instanceof org.apache.flink.runtime.minicluster.MiniClusterJobClient))
      throw new IllegalStateException("comparison requires its test MiniCluster");
    var field =
        org.apache.flink.runtime.minicluster.MiniClusterJobClient.class.getDeclaredField(
            "miniCluster");
    field.setAccessible(true);
    return (org.apache.flink.runtime.minicluster.MiniCluster) field.get(job);
  }

  private static void profile(String action, Path trial) throws Exception {
    String tool = System.getenv("SF_ICEBERG_PROFILE_TOOL");
    if (tool == null) return;
    String event = System.getenv().getOrDefault("SF_ICEBERG_PROFILE_EVENT", "itimer");
    var command = new ArrayList<>(List.of(tool, action));
    if (action.equals("start")) command.addAll(List.of("-e", event, "-t", "--cstack", "dwarf"));
    else
      command.addAll(
          List.of("-o", "collapsed", "-f", trial.resolve(event + ".collapsed").toString()));
    command.add(Long.toString(ProcessHandle.current().pid()));
    var process = new ProcessBuilder(command).inheritIO().start();
    assertTrue(process.waitFor(30, TimeUnit.SECONDS), "profiler command timed out");
    assertEquals(0, process.exitValue(), "profiler command failed");
  }

  private static boolean backendObserved(String mode, boolean nativeMode, Path directory)
      throws Exception {
    if (!mode.startsWith("disk")) return false;
    if (nativeMode) return Native.liveNativeHandles().contains("Rocks");
    Path rocks = directory.resolve("rocksdb");
    if (!Files.isDirectory(rocks)) return false;
    try (var files = Files.list(rocks)) {
      return files.findAny().isPresent();
    }
  }

  // Same schema, wire format, offsets, poll size and views as the headline Kafka harness.
  private void kafkaInput(StreamTableEnvironment sql) {
    sql.executeSql(
        "CREATE TABLE src ("
            + NexmarkKafkaBenchmark.SCHEMA
            + ", rowtime AS TO_TIMESTAMP_LTZ(`dateTime`, 3), WATERMARK FOR rowtime AS rowtime -"
            + " INTERVAL '4' SECOND) WITH ('connector' = 'kafka', 'topic' = 'nexmark',"
            + " 'properties.bootstrap.servers' = '"
            + brokers
            + "', 'properties.group.id' = 'nexmark', 'properties.max.poll.records' = '8192',"
            + " 'scan.startup.mode' = 'earliest-offset',"
            + " 'scan.bounded.mode' = 'latest-offset', 'format' = 'json')");
    NexmarkMatrixBenchmark.registerEventViews(sql);
  }

  private void input(StreamTableEnvironment sql, String kind) {
    var table = catalog.loadTable(TableIdentifier.of("bench", "input_" + kind));
    var type = FlinkSchemaUtil.convert(table.schema());
    var schema = Schema.newBuilder();
    for (int field = 0; field < type.getFieldCount(); field++)
      schema.column(
          type.getFieldNames().get(field),
          TypeConversions.fromLogicalToDataType(type.getTypeAt(field)));
    schema
        .columnByExpression("_sf_rowtime", "CAST(`dateTime` AS TIMESTAMP(3))")
        .watermark("_sf_rowtime", "SOURCE_WATERMARK()");
    sql.createTemporaryTable(
        "input_" + kind,
        descriptor(
            "input_" + kind,
            schema.build(),
            Map.of(
                "streaming",
                "true",
                "monitor-interval",
                "100ms",
                "starting-strategy",
                "TABLE_SCAN_THEN_INCREMENTAL",
                "split-size",
                "134217728",
                "split-file-open-cost",
                "134217728",
                "watermark-column",
                "dateTime")));
    String columns =
        String.join(
            ", ",
            table.schema().columns().stream()
                .map(
                    field ->
                        field.name().equals("dateTime")
                            ? "_sf_rowtime AS `dateTime`"
                            : field.name().equals("expires")
                                ? "CAST(expires AS TIMESTAMP(3)) AS expires"
                                : "`" + field.name() + "`")
                .toList());
    sql.executeSql(
        "CREATE TEMPORARY VIEW " + kind + " AS SELECT " + columns + " FROM input_" + kind);
  }

  private TableDescriptor descriptor(String table, Schema schema, Map<String, String> settings) {
    var builder =
        TableDescriptor.forConnector("iceberg")
            .schema(schema)
            .option("catalog-type", "hadoop")
            .option("catalog-name", "iceberg-benchmark")
            .option("warehouse", warehouse.toUri().toString())
            .option("catalog-database", "bench")
            .option("catalog-table", table);
    settings.forEach(builder::option);
    return builder.build();
  }

  private record Fingerprint(long count, List<String> sums) {}

  private Fingerprint fingerprint(Table table, String query) throws Exception {
    long count = 0;
    long[] sums = new long[4];
    var digest = java.security.MessageDigest.getInstance("SHA-256");
    var proctime = new TreeMap<Long, Long>();
    try (var records = IcebergGenerics.read(table).build()) {
      for (var record : records) {
        count++;
        if (query.equals("q12")) {
          proctime.merge(
              (Long) record.getField("bidder"), (Long) record.getField("bid_count"), Long::sum);
          continue;
        }
        var payload =
            table.schema().columns().stream()
                .map(
                    field -> {
                      Object value = record.getField(field.name());
                      return value == null ? null : value.toString();
                    })
                .toList();
        byte[] hash = digest.digest(JSON.writeValueAsBytes(payload));
        var buffer = java.nio.ByteBuffer.wrap(hash);
        for (int index = 0; index < 4; index++) sums[index] += buffer.getLong();
      }
    }
    if (query.equals("q12")) {
      processingTimeCounts = Map.copyOf(proctime);
      if (KAFKA_SOURCE) {
        // Finite-offset streaming leaves unfinished processing-time windows open, as stock Flink
        // does.
        for (var entry : proctime.entrySet())
          assertTrue(
              entry.getValue() >= 0
                  && entry.getValue() <= bidCounts.getOrDefault(entry.getKey(), 0L),
              "processing-time output exceeds its bidder's input count");
      } else
        assertEquals(
            bidCounts, proctime, "PROCTIME windows must conserve each bidder's complete corpus");
      return new Fingerprint(
          bidCounts.values().stream().mapToLong(Long::longValue).sum(),
          List.of(
              KAFKA_SOURCE
                  ? "processing-time emitted counts bounded by input; unfinished windows excluded"
                  : "processing-time count conservation"));
    }
    return new Fingerprint(
        count, Arrays.stream(sums).mapToObj(value -> Long.toUnsignedString(value, 16)).toList());
  }

  private static String createdBy(String file) throws Exception {
    try (var parquet =
        org.apache.iceberg.shaded.org.apache.parquet.hadoop.ParquetFileReader.open(
            org.apache.iceberg.shaded.org.apache.parquet.hadoop.util.HadoopInputFile.fromPath(
                new org.apache.hadoop.fs.Path(file), new org.apache.hadoop.conf.Configuration()))) {
      return parquet.getFooter().getFileMetaData().getCreatedBy();
    }
  }

  private void appendJson(Object row) throws Exception {
    Files.writeString(
        artifacts.resolve("observations.jsonl"),
        JSON.writeValueAsString(row) + "\n",
        java.nio.file.StandardOpenOption.CREATE,
        java.nio.file.StandardOpenOption.APPEND);
  }
}
