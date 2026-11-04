package org.example.canonical;

import java.io.DataInputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.metrics.Counter;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.MiniClusterConfiguration;
import org.apache.flink.runtime.testutils.CommonTestUtils;
import org.apache.flink.streaming.util.TestStreamEnvironment;
import org.apache.flink.table.api.CompiledPlan;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.PlanReference;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/** Custom source to the real Kafka sink, runnable without StreamFusion on the stock leg. */
public final class CanonicalKafkaSinkJobWorker {
  private CanonicalKafkaSinkJobWorker() {}

  public static void main(String[] args) throws Exception {
    try {
      run(args);
    } catch (Throwable failure) {
      failure.printStackTrace();
      System.exit(1);
    }
  }

  private static void run(String[] args) throws Exception {
    boolean nativeRun = args[0].equals("native");
    Path root = Path.of(args[1]);
    int limit = Integer.parseInt(args[2]);
    String restore = args[3], stage = args[4];
    boolean checkpoint = Files.exists(root.resolve(stage + "-native-checkpoint"));
    int sinkParallelism =
        Files.exists(root.resolve(stage + "-parallelism"))
            ? Integer.parseInt(Files.readString(root.resolve(stage + "-parallelism")).trim())
            : 1;
    Path ready = root.resolve(stage + "-ready");
    boolean kafkaInput = Files.exists(root.resolve("kafka-input"));
    if (!nativeRun) {
      try {
        Class.forName(
            "tech.streamfusion.Native", false, CanonicalKafkaSinkJobWorker.class.getClassLoader());
        throw new AssertionError("StreamFusion on stock classpath");
      } catch (ClassNotFoundException correct) {
        Files.writeString(
            root.resolve(stage + "-stock-classpath-verified"),
            System.getProperty("java.class.path"));
      }
    }
    var config = new Configuration();
    config.setString("restart-strategy.type", "none");
    config.setString("rest.port", "0");
    config.setString("execution.checkpointing.dir", root.resolve("checkpoints").toUri().toString());
    if (checkpoint)
      config.setString(
          "execution.checkpointing.externalized-checkpoint-retention", "RETAIN_ON_CANCELLATION");
    config.setString("metrics.reporter.sink.factory.class", ReporterFactory.class.getName());
    if (!restore.equals("none")) {
      config.setString(
          "execution.savepoint.path",
          Files.readString(root.resolve(restore + "-savepoint")).trim());
      config.setString("execution.savepoint.ignore-unclaimed-state", "false");
      config.setString("execution.savepoint.restore-mode", "NO_CLAIM");
    }
    var cluster =
        new MiniCluster(
            new MiniClusterConfiguration.Builder()
                .setConfiguration(config)
                .setNumTaskManagers(1)
                .setNumSlotsPerTaskManager(Math.max(2, sinkParallelism))
                .build());
    cluster.start();
    ClassLoader previous = Thread.currentThread().getContextClassLoader();
    var loader = nativeRun ? CanonicalJobWorker.nativePlannerLoader(previous, root) : previous;
    Thread.currentThread().setContextClassLoader(loader);
    try (cluster) {
      var environment = new TestStreamEnvironment(cluster, config, 1, List.of(), List.of());
      environment.setParallelism(1);
      environment.setMaxParallelism(128);
      if (Files.exists(root.resolve("disable-chaining"))) environment.disableOperatorChaining();
      environment.enableCheckpointing(TimeUnit.HOURS.toMillis(1));
      var table =
          StreamTableEnvironment.create(
              environment,
              EnvironmentSettings.newInstance().inStreamingMode().withClassLoader(loader).build());
      table.getConfig().set("streamfusion.native.enabled", Boolean.toString(nativeRun));
      String uidPolicy = Files.exists(root.resolve("stable-uids")) ? "ALWAYS" : "DISABLED";
      table.getConfig().set("table.exec.uid.generation", uidPolicy);
      Files.writeString(root.resolve(stage + "-uid-policy"), uidPolicy);
      Files.writeString(
          root.resolve(stage + "-chaining"), Boolean.toString(environment.isChainingEnabled()));
      table.getConfig().set("streamfusion.transpose.batchRows", args[5]);
      table.getConfig().set("table.plan.compile.catalog-objects", "SCHEMA");
      table.getConfig().set("table.plan.restore.catalog-objects", "IDENTIFIER");
      if (kafkaInput) table.executeSql(Files.readString(root.resolve("input.sql")));
      else
        table.executeSql(
            "CREATE TABLE input_table(k BIGINT,n BIGINT,total BIGINT) WITH"
                + " ('connector'='canonical-fixture','input'='"
                + sql(root.resolve("input.csv"))
                + "','ready'='"
                + sql(ready)
                + "','limit'='"
                + limit
                + "','insert-only'='true')");
      table.executeSql(
          Files.readString(root.resolve("output.sql"))
              .replace("%SINK_PARALLELISM%", Integer.toString(sinkParallelism)));
      String query = "INSERT INTO output_table SELECT k,n,total FROM input_table";
      boolean compiled = !Files.exists(root.resolve("ordinary-sql"));
      Path planFile = root.resolve("plan.json");
      CompiledPlan plan = null;
      if (compiled) {
        plan =
            Files.exists(planFile)
                ? table.loadPlan(PlanReference.fromFile(planFile.toString()))
                : table.compilePlanSql(query);
        String portable = plan.asJsonString();
        if (portable.contains("tech.streamfusion")) throw new AssertionError("Nonportable plan");
        if (!Files.exists(planFile)) Files.writeString(planFile, portable);
      }
      var result = compiled ? plan.execute() : table.executeSql(query);
      var job = result.getJobClient().orElseThrow();
      long deadline = System.nanoTime() + Duration.ofSeconds(90).toNanos();
      if (kafkaInput) awaitKafkaOutput(root, limit);
      while (!kafkaInput && !Files.exists(ready)) {
        if (job.getJobExecutionResult().isDone()) {
          job.getJobExecutionResult().get();
          throw new AssertionError("Source ended before staged input boundary");
        }
        if (System.nanoTime() > deadline) throw new AssertionError("Source boundary not reached");
        Thread.sleep(10);
      }
      if (!kafkaInput && !Files.readString(ready).trim().equals(Integer.toString(limit)))
        throw new AssertionError("Wrong custom source offset");
      CommonTestUtils.waitForAllTaskRunning(cluster, job.getJobID(), false);
      var actualGraph = cluster.getExecutionGraph(job.getJobID()).get(30, TimeUnit.SECONDS);
      String actualPlan =
          actualGraph.getAllVertices().values().stream()
              .map(vertex -> vertex.getName())
              .collect(java.util.stream.Collectors.joining("\n"));
      Files.writeString(root.resolve(stage + "-plan.txt"), actualPlan);
      if (nativeRun && !actualPlan.contains("native-kafka-serialization"))
        throw new AssertionError("Submitted job has no native Kafka serializer: " + actualPlan);
      if (nativeRun
          && kafkaInput
          && (!actualPlan.contains("NativeSource")
              || actualPlan.contains("RowDataToArrow")
              || actualPlan.contains("ArrowToRowData")))
        throw new AssertionError(
            "Submitted Kafka pipeline is not entirely columnar: " + actualPlan);
      Files.write(
          root.resolve(stage + "-vertices.tsv"),
          actualGraph.getAllVertices().values().stream()
              .map(
                  vertex ->
                      vertex.getName()
                          + "\t"
                          + vertex.getParallelism()
                          + "\t"
                          + vertex.getMaxParallelism())
              .collect(java.util.stream.Collectors.toList()));

      String saved;
      if (checkpoint) {
        saved = cluster.triggerCheckpoint(job.getJobID()).get(90, TimeUnit.SECONDS);
        job.cancel().get(30, TimeUnit.SECONDS);
        try {
          job.getJobExecutionResult().get(30, TimeUnit.SECONDS);
          throw new AssertionError("Cancelled sink job completed successfully");
        } catch (java.util.concurrent.ExecutionException cancelled) {
          if (job.getJobStatus().get(30, TimeUnit.SECONDS)
              != org.apache.flink.api.common.JobStatus.CANCELED) throw cancelled;
        }
      } else {
        saved =
            job.stopWithSavepoint(
                    false,
                    root.resolve("savepoints").toUri().toString(),
                    SavepointFormatType.CANONICAL)
                .get(90, TimeUnit.SECONDS);
        job.getJobExecutionResult().get(30, TimeUnit.SECONDS);
      }
      Files.writeString(root.resolve(stage + "-savepoint"), saved);
      Files.writeString(root.resolve(stage + "-native-rows"), Long.toString(Reporter.rows()));
      Files.writeString(
          root.resolve(stage + "-native-source-output"), Long.toString(Reporter.sourceOutput()));
      saveInventory(root, stage, saved);
    } finally {
      Thread.currentThread().setContextClassLoader(previous);
    }
  }

  private static void awaitKafkaOutput(Path root, int limit) throws Exception {
    var properties = new Properties();
    properties.setProperty("bootstrap.servers", Files.readString(root.resolve("brokers")));
    properties.setProperty("group.id", java.util.UUID.randomUUID().toString());
    properties.setProperty("enable.auto.commit", "false");
    properties.setProperty("isolation.level", "read_uncommitted");
    properties.setProperty(
        "key.deserializer",
        org.apache.kafka.common.serialization.StringDeserializer.class.getName());
    properties.setProperty(
        "value.deserializer",
        org.apache.kafka.common.serialization.StringDeserializer.class.getName());
    String topic = Files.readString(root.resolve("output-topic"));
    try (var consumer =
        new org.apache.kafka.clients.consumer.KafkaConsumer<String, String>(properties)) {
      var partitions =
          consumer.partitionsFor(topic).stream()
              .map(
                  partition ->
                      new org.apache.kafka.common.TopicPartition(topic, partition.partition()))
              .collect(java.util.stream.Collectors.toList());
      consumer.assign(partitions);
      consumer.seekToBeginning(partitions);
      var observed = new java.util.HashSet<Integer>();
      long deadline = System.nanoTime() + Duration.ofSeconds(90).toNanos();
      var json = new org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper();
      while (observed.size() < limit) {
        for (var record : consumer.poll(Duration.ofMillis(100))) {
          int id = json.readTree(record.value()).get("k").asInt();
          if (id >= 0 && id < limit) observed.add(id);
        }
        if (System.nanoTime() > deadline)
          throw new AssertionError("Kafka input did not reach output boundary");
      }
    }
  }

  private static void saveInventory(Path root, String stage, String pointer) throws Exception {
    Path location = Path.of(java.net.URI.create(pointer));
    var writerSerializer =
        org.apache.flink.connector.kafka.sink.KafkaSink.<String>builder()
            .setBootstrapServers("unused:9092")
            .setRecordSerializer(
                org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema.builder()
                    .setTopic("unused")
                    .setValueSerializationSchema(
                        new org.apache.flink.api.common.serialization.SimpleStringSchema())
                    .build())
            .build()
            .getWriterStateSerializer();
    try (var input =
        new DataInputStream(
            Files.newInputStream(
                Files.isDirectory(location) ? location.resolve("_metadata") : location))) {
      var metadata =
          org.apache.flink.runtime.checkpoint.Checkpoints.loadCheckpointMetadata(
              input, CanonicalKafkaSinkJobWorker.class.getClassLoader(), pointer);
      var inventory = new ArrayList<String>();
      for (var state : metadata.getOperatorStates()) {
        var sourceOffsets = new ArrayList<String>();
        var writerPrefixes = new ArrayList<String>();
        var ownership = new ArrayList<String>();
        int pendingTransactions = 0;
        var names = new java.util.TreeSet<String>();
        for (var subtask : state.getStates())
          for (var handle : subtask.getManagedOperatorState())
            for (var entry : handle.getStateNameToPartitionOffsets().entrySet()) {
              names.add(entry.getKey());
              if (!entry.getKey().equals("SourceReaderState")
                  && !entry.getKey().equals("writer_raw_states")) continue;
              try (var stream = handle.openInputStream()) {
                for (long offset : entry.getValue().getOffsets()) {
                  stream.seek(offset);
                  byte[] bytes =
                      org.apache.flink.api.common.typeutils.base.array.BytePrimitiveArraySerializer
                          .INSTANCE
                          .deserialize(
                              new org.apache.flink.core.memory.DataInputViewStreamWrapper(stream));
                  if (entry.getKey().equals("SourceReaderState")) {
                    if (Files.exists(root.resolve("kafka-input"))) {
                      var split =
                          org.apache.flink.core.io.SimpleVersionedSerialization
                              .readVersionAndDeSerialize(
                                  new org.apache.flink.connector.kafka.source.split
                                      .KafkaPartitionSplitSerializer(),
                                  bytes);
                      sourceOffsets.add(split.getPartition() + "=" + split.getStartingOffset());
                    } else
                      sourceOffsets.add(
                          Integer.toString(
                              org.apache.flink.core.io.SimpleVersionedSerialization
                                  .readVersionAndDeSerialize(
                                      new CanonicalFixtureFactory.OffsetSerializer(), bytes)
                                  .offset));
                  } else {
                    var writer =
                        org.apache.flink.core.io.SimpleVersionedSerialization
                            .readVersionAndDeSerialize(writerSerializer, bytes);
                    writerPrefixes.add(writer.getTransactionalIdPrefix());
                    ownership.add(
                        writer.getOwnedSubtaskId()
                            + "/"
                            + writer.getTotalNumberOfOwnedSubtasks()
                            + "/"
                            + writer.getTransactionOwnership());
                    pendingTransactions += writer.getPrecommittedTransactionalIds().size();
                  }
                }
              }
            }
        java.util.Collections.sort(ownership);
        java.util.Collections.sort(sourceOffsets);
        inventory.add(
            state.getOperatorID()
                + "\t"
                + String.join(",", names)
                + "\t"
                + sourceOffsets
                + "\t"
                + writerPrefixes
                + "\t"
                + state.getParallelism()
                + "\t"
                + state.getMaxParallelism()
                + "\t"
                + ownership
                + "\t"
                + pendingTransactions
                + "\t"
                + (state.getCoordinatorState() == null
                    ? 0
                    : state.getCoordinatorState().getStateSize()));
      }
      java.util.Collections.sort(inventory);
      Files.write(root.resolve(stage + "-inventory.tsv"), inventory);
    }
  }

  private static String sql(Path path) {
    return path.toString().replace("'", "''");
  }

  public static final class ReporterFactory
      implements org.apache.flink.metrics.reporter.MetricReporterFactory {
    public org.apache.flink.metrics.reporter.MetricReporter createMetricReporter(
        Properties options) {
      return new Reporter();
    }
  }

  public static final class Reporter implements org.apache.flink.metrics.reporter.MetricReporter {
    static final Map<String, Counter> counters = new ConcurrentHashMap<>();
    static final Map<String, Counter> sources = new ConcurrentHashMap<>();

    public void open(org.apache.flink.metrics.MetricConfig config) {}

    public void close() {}

    public void notifyOfAddedMetric(
        org.apache.flink.metrics.Metric metric,
        String name,
        org.apache.flink.metrics.MetricGroup group) {
      if (name.equals("nativeKafkaSerializationRows") && metric instanceof Counter)
        counters.put(group.getMetricIdentifier(name), (Counter) metric);
      if (name.equals("numRecordsOut")
          && group.getAllVariables().getOrDefault("<operator_name>", "").contains("NativeSource")
          && metric instanceof Counter)
        sources.put(group.getMetricIdentifier(name), (Counter) metric);
    }

    public void notifyOfRemovedMetric(
        org.apache.flink.metrics.Metric metric,
        String name,
        org.apache.flink.metrics.MetricGroup group) {}

    static long rows() {
      return counters.values().stream().mapToLong(Counter::getCount).sum();
    }

    static long sourceOutput() {
      return sources.values().stream().mapToLong(Counter::getCount).sum();
    }
  }
}
