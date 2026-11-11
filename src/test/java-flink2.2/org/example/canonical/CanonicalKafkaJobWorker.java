package org.example.canonical;

import java.nio.file.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.runtime.minicluster.*;
import org.apache.flink.runtime.testutils.CommonTestUtils;
import org.apache.flink.streaming.util.TestStreamEnvironment;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/** Isolated Kafka handoff using only normal connector/user dependencies on the stock leg. */
public final class CanonicalKafkaJobWorker {
  private CanonicalKafkaJobWorker() {}

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
    int expected = Integer.parseInt(args[2]);
    String restore = args[3], stage = args[4];
    int batchRows = Integer.parseInt(args[5]);
    int parallelism =
        Files.exists(root.resolve(stage + "-parallelism"))
            ? Integer.parseInt(Files.readString(root.resolve(stage + "-parallelism")).trim())
            : 1;
    boolean checkpoint = Files.exists(root.resolve(stage + "-native-checkpoint"));
    Path witness = root.resolve(stage + "-witness");
    if (!nativeRun) {
      try {
        Class.forName(
            "tech.streamfusion.Native", false, CanonicalKafkaJobWorker.class.getClassLoader());
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
    config.setString("metrics.reporter.kafka.factory.class", ReporterFactory.class.getName());
    if (checkpoint)
      config.setString(
          "execution.checkpointing.externalized-checkpoint-retention", "RETAIN_ON_CANCELLATION");
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
                .setNumSlotsPerTaskManager(Math.max(2, parallelism))
                .build());
    cluster.start();
    ClassLoader previous = Thread.currentThread().getContextClassLoader();
    var loader = nativeRun ? CanonicalJobWorker.nativePlannerLoader(previous, root) : previous;
    Thread.currentThread().setContextClassLoader(loader);
    try (cluster) {
      var environment =
          new TestStreamEnvironment(cluster, config, parallelism, List.of(), List.of());
      environment.setParallelism(parallelism);
      if (Files.exists(root.resolve("disable-chaining"))) environment.disableOperatorChaining();
      if (checkpoint) {
        environment.enableCheckpointing(TimeUnit.HOURS.toMillis(1));
      }
      environment.setMaxParallelism(128);
      var table =
          StreamTableEnvironment.create(
              environment,
              EnvironmentSettings.newInstance().inStreamingMode().withClassLoader(loader).build());
      table.getConfig().set("streamfusion.native.enabled", Boolean.toString(nativeRun));
      table.getConfig().set("table.exec.uid.generation", "DISABLED");
      table.getConfig().set("streamfusion.transpose.batchRows", Integer.toString(batchRows));
      table.getConfig().set("table.plan.compile.catalog-objects", "SCHEMA");
      table.getConfig().set("table.plan.restore.catalog-objects", "IDENTIFIER");
      table.executeSql(Files.readString(root.resolve("input.sql")));
      Files.createFile(Path.of(witness + "-count-enabled"));
      table.executeSql(
          "CREATE TABLE output_table(k BIGINT,n BIGINT,total BIGINT) WITH"
              + " ('connector'='canonical-fixture','output'='"
              + sql(root.resolve("committed"))
              + "','witness'='"
              + sql(witness)
              + "','parallelism'='1')");
      String query = "INSERT INTO output_table SELECT k,n,total FROM input_table";
      boolean compiled = !Files.exists(root.resolve("ordinary-sql"));
      var planFile = root.resolve("plan.json");
      CompiledPlan plan = null;
      if (compiled) {
        plan =
            Files.exists(planFile)
                ? table.loadPlan(PlanReference.fromFile(planFile.toString()))
                : table.compilePlanSql(query);
        String portable = plan.asJsonString();
        if (portable.contains("tech.streamfusion"))
          throw new AssertionError("Nonportable compiled plan");
        if (!Files.exists(planFile)) Files.writeString(planFile, portable);
      }
      String explain =
          compiled
              ? plan.explain(ExplainDetail.JSON_EXECUTION_PLAN)
              : table.explainSql(query, ExplainDetail.JSON_EXECUTION_PLAN);
      Files.writeString(root.resolve(stage + "-plan.txt"), explain);
      if (nativeRun && !explain.contains("NativeSource"))
        throw new AssertionError("Kafka source not native: " + explain);
      var outputType =
          org.apache.flink.table.types.logical.RowType.of(
              new org.apache.flink.table.types.logical.BigIntType(),
              new org.apache.flink.table.types.logical.BigIntType(),
              new org.apache.flink.table.types.logical.BigIntType());
      Path committed = root.resolve("committed");
      int committedBefore =
          checkpoint && Files.isDirectory(committed)
              ? CanonicalFixtureFactory.committedRows(committed, outputType).size()
              : 0;
      var result = compiled ? plan.execute() : table.executeSql(query);
      var job = result.getJobClient().orElseThrow();
      Path count = Path.of(witness + "-sink-count");
      long deadline = System.nanoTime() + Duration.ofSeconds(90).toNanos();
      while (true) {
        if (Files.exists(count)
            && Files.readString(count).trim().equals(Integer.toString(expected))) break;
        if (job.getJobExecutionResult().isDone()) {
          job.getJobExecutionResult().get();
          throw new AssertionError("Job ended before input boundary");
        }
        if (System.nanoTime() > deadline)
          throw new AssertionError(
              "Kafka boundary not reached: "
                  + (Files.exists(count) ? Files.readString(count) : "missing"));
        Thread.sleep(10);
      }
      CommonTestUtils.waitForAllTaskRunning(cluster, job.getJobID(), false);
      if (nativeRun && expected > 0 && Reporter.nativeOutput() == 0)
        throw new AssertionError("Native Kafka source emitted no batches: " + Reporter.counters);
      Files.writeString(
          root.resolve(stage + "-native-output"), Long.toString(Reporter.nativeOutput()));
      String saved;
      if (checkpoint) {
        saved = cluster.triggerCheckpoint(job.getJobID()).get(90, TimeUnit.SECONDS);
        // Coordinator completion precedes the sink's asynchronous external commit notification.
        awaitCommittedRows(
            committed, outputType, committedBefore + expected, Duration.ofSeconds(30));
        job.cancel().get(30, TimeUnit.SECONDS);
        try {
          job.getJobExecutionResult().get(30, TimeUnit.SECONDS);
          throw new AssertionError("Cancelled checkpoint source completed successfully");
        } catch (ExecutionException cancelled) {
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
      saveInventory(root, stage, saved);
    } finally {
      Thread.currentThread().setContextClassLoader(previous);
    }
  }

  static void awaitCommittedRows(
      Path directory,
      org.apache.flink.table.types.logical.RowType type,
      int expected,
      Duration timeout)
      throws Exception {
    long deadline = System.nanoTime() + timeout.toNanos();
    while (true) {
      int rows =
          Files.isDirectory(directory)
              ? CanonicalFixtureFactory.committedRows(directory, type).size()
              : 0;
      if (rows > expected)
        throw new AssertionError("Duplicate committed source rows: " + rows + " > " + expected);
      if (rows == expected) return;
      if (System.nanoTime() >= deadline)
        throw new AssertionError("External sink commit not reached: " + rows + " != " + expected);
      Thread.sleep(10);
    }
  }

  private static void saveInventory(Path root, String stage, String pointer) throws Exception {
    Path location = Path.of(java.net.URI.create(pointer));
    Path file = Files.isDirectory(location) ? location.resolve("_metadata") : location;
    try (var input = new java.io.DataInputStream(Files.newInputStream(file))) {
      var metadata =
          org.apache.flink.runtime.checkpoint.Checkpoints.loadCheckpointMetadata(
              input, CanonicalKafkaJobWorker.class.getClassLoader(), pointer);
      var inventory = new ArrayList<String>();
      for (var state : metadata.getOperatorStates()) {
        long splits = 0;
        var positions = new java.util.TreeMap<Integer, Long>();
        for (var subtask : state.getStates())
          for (var handle : subtask.getManagedOperatorState())
            for (var entry : handle.getStateNameToPartitionOffsets().entrySet())
              if (entry.getKey().equals("SourceReaderState")) {
                splits += entry.getValue().getOffsets().length;
                try (var stream = handle.openInputStream()) {
                  for (long offset : entry.getValue().getOffsets()) {
                    stream.seek(offset);
                    byte[] bytes =
                        org.apache.flink.api.common.typeutils.base.array
                            .BytePrimitiveArraySerializer.INSTANCE
                            .deserialize(
                                new org.apache.flink.core.memory.DataInputViewStreamWrapper(
                                    stream));
                    var split =
                        org.apache.flink.core.io.SimpleVersionedSerialization
                            .readVersionAndDeSerialize(
                                new org.apache.flink.connector.kafka.source.split
                                    .KafkaPartitionSplitSerializer(),
                                bytes);
                    if (split.getStoppingOffset().isPresent())
                      throw new AssertionError("Unexpected bounded Kafka split");
                    if (positions.put(split.getPartition(), split.getStartingOffset()) != null)
                      throw new AssertionError("Duplicated Kafka partition state");
                  }
                }
              }
        long coordinator =
            state.getCoordinatorState() == null ? 0 : state.getCoordinatorState().getStateSize();
        inventory.add(
            state.getOperatorID()
                + "\t"
                + state.getOperatorName().orElse("")
                + "\t"
                + state.getParallelism()
                + "\t"
                + state.getMaxParallelism()
                + "\t"
                + coordinator
                + "\t"
                + splits
                + "\t"
                + positions.entrySet().stream()
                    .map(entry -> entry.getKey() + "=" + entry.getValue())
                    .collect(java.util.stream.Collectors.joining(",")));
      }
      Collections.sort(inventory);
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
    static final Map<String, org.apache.flink.metrics.Counter> counters = new ConcurrentHashMap<>();

    public void open(org.apache.flink.metrics.MetricConfig config) {}

    public void close() {}

    public void notifyOfAddedMetric(
        org.apache.flink.metrics.Metric metric,
        String name,
        org.apache.flink.metrics.MetricGroup group) {
      String operator = group.getAllVariables().getOrDefault("<operator_name>", "");
      if (name.equals("numRecordsOut")
          && operator.contains("NativeSource")
          && metric instanceof org.apache.flink.metrics.Counter)
        counters.put(group.getMetricIdentifier(name), (org.apache.flink.metrics.Counter) metric);
    }

    public void notifyOfRemovedMetric(
        org.apache.flink.metrics.Metric metric,
        String name,
        org.apache.flink.metrics.MetricGroup group) {}

    static long nativeOutput() {
      return counters.values().stream().mapToLong(org.apache.flink.metrics.Counter::getCount).sum();
    }
  }
}
