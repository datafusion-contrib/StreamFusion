package org.example.canonical;

import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchService;
import java.time.Duration;
import java.util.Collections;
import java.util.Enumeration;
import java.util.LinkedHashSet;
import java.util.concurrent.TimeUnit;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.MiniClusterConfiguration;
import org.apache.flink.runtime.testutils.CommonTestUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.util.TestStreamEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.ExplainDetail;
import org.apache.flink.table.api.PlanReference;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/** Runs a job in a process containing only its normal user connector JAR on the stock leg. */
public final class CanonicalJobWorker {
  private CanonicalJobWorker() {}

  public static void main(String[] arguments) throws Exception {
    try {
      run(arguments);
    } catch (Throwable failure) {
      failure.printStackTrace(System.err);
      System.exit(1);
    }
  }

  private static void run(String[] arguments) throws Exception {
    boolean nativeRun = arguments[0].equals("native");
    Path root = Path.of(arguments[1]);
    int limit = Integer.parseInt(arguments[2]);
    String restore = arguments[3];
    String stage = arguments[4];
    int batchRows = Integer.parseInt(arguments[5]);
    String backend = arguments[6];
    boolean nativeCheckpoint = Files.exists(root.resolve(stage + "-native-checkpoint"));
    boolean expectCommitFailure = Files.exists(root.resolve(stage + "-expect-commit-failure"));
    int restoredLimit =
        restore.equals("none")
            ? 0
            : Integer.parseInt(Files.readString(root.resolve(restore + "-limit")).trim());
    Files.createDirectories(root);
    Path ready = root.resolve(stage + "-ready");
    Path rightReady = root.resolve(stage + "-right-ready");
    boolean twoInputs = Files.exists(root.resolve("right-input.csv"));
    Path witness = root.resolve(stage + "-native-witness");
    if (!nativeRun) {
      try {
        Class.forName("tech.streamfusion.Native", false, CanonicalJobWorker.class.getClassLoader());
        throw new AssertionError("StreamFusion is present on the stock classpath");
      } catch (ClassNotFoundException expected) {
        Files.writeString(
            root.resolve(stage + "-stock-classpath-verified"),
            System.getProperty("java.class.path"));
      }
    }
    System.setProperty("streamfusion.transpose.flushLatencyMs", "0");
    System.setProperty("streamfusion.exchange.coalesceRows", "1");
    int parallelism =
        Files.exists(root.resolve(stage + "-parallelism"))
            ? Integer.parseInt(Files.readString(root.resolve(stage + "-parallelism")).trim())
            : 1;
    String fixedConnectorParallelism =
        Files.exists(root.resolve("rescale")) ? ",'parallelism'='1'" : "";
    Configuration config = new Configuration();
    config.setString("execution.target", "local");
    config.setString("restart-strategy.type", "none");
    config.setString("execution.attached", "true");
    config.setString("rest.port", "0");
    config.setString("parallelism.default", Integer.toString(parallelism));
    config.setString(
        "state.backend.type",
        nativeRun && backend.equals("rocksdb")
            ? "tech.streamfusion.state.RocksDBNativeStateBackendFactory"
            : backend);
    config.setString(
        "metrics.reporter.interop.factory.class", EvidenceReporterFactory.class.getName());
    config.setString("restart-strategy.type", "none");
    config.setString("execution.checkpointing.dir", root.resolve("checkpoints").toUri().toString());
    if (nativeCheckpoint) {
      config.setString(
          "execution.checkpointing.externalized-checkpoint-retention", "RETAIN_ON_CANCELLATION");
    }
    if (!restore.equals("none")) {
      config.setString(
          "execution.savepoint.path",
          Files.readString(root.resolve(restore + "-savepoint")).trim());
      config.setString("execution.savepoint.ignore-unclaimed-state", "false");
      config.setString("execution.savepoint.restore-mode", "NO_CLAIM");
    }
    MiniCluster cluster =
        new MiniCluster(
            new MiniClusterConfiguration.Builder()
                .setConfiguration(config)
                .setNumTaskManagers(1)
                .setNumSlotsPerTaskManager(Math.max(2, parallelism))
                .build());
    cluster.start();
    StreamExecutionEnvironment environment =
        new TestStreamEnvironment(
            cluster, config, parallelism, java.util.List.of(), java.util.List.of());
    environment.setParallelism(parallelism);
    environment.setMaxParallelism(128);
    if (Files.exists(root.resolve("disable-chaining"))) environment.disableOperatorChaining();
    if (Files.exists(root.resolve("eager-watermarks")))
      environment.getConfig().setAutoWatermarkInterval(1);
    if (nativeCheckpoint) environment.enableCheckpointing(TimeUnit.HOURS.toMillis(1));
    ClassLoader original = Thread.currentThread().getContextClassLoader();
    ClassLoader loader = nativeRun ? nativePlannerLoader(original, root) : original;
    Thread.currentThread().setContextClassLoader(loader);
    try (cluster;
        WatchService watcher = root.getFileSystem().newWatchService()) {
      root.register(watcher, StandardWatchEventKinds.ENTRY_CREATE);
      var table =
          StreamTableEnvironment.create(
              environment,
              EnvironmentSettings.newInstance().inStreamingMode().withClassLoader(loader).build());
      table.getConfig().set("streamfusion.native.enabled", Boolean.toString(nativeRun));
      if (Files.exists(root.resolve("uid-generation")))
        table
            .getConfig()
            .set(
                "table.exec.uid.generation",
                Files.readString(root.resolve("uid-generation")).trim());
      if (Files.exists(root.resolve("time-zone")))
        table
            .getConfig()
            .setLocalTimeZone(
                java.time.ZoneId.of(Files.readString(root.resolve("time-zone")).trim()));
      table.getConfig().set("table.optimizer.agg-phase-strategy", "ONE_PHASE");
      boolean sinkMaterializer = Files.exists(root.resolve("sink-materializer-strategy"));
      if (sinkMaterializer) {
        String strategy = Files.readString(root.resolve("sink-materializer-strategy")).trim();
        table.getConfig().set("table.exec.sink.upsert-materialize", "FORCE");
        table
            .getConfig()
            .set(
                "table.exec.sink.upsert-materialize-strategy.type",
                strategy.startsWith("ADAPTIVE") ? "ADAPTIVE" : strategy);
        if (strategy.startsWith("ADAPTIVE")) {
          table
              .getConfig()
              .set(
                  "table.exec.sink.upsert-materialize-strategy.adaptive.threshold.high",
                  strategy.endsWith("MAP") ? "1" : "100");
          table
              .getConfig()
              .set(
                  "table.exec.sink.upsert-materialize-strategy.adaptive.threshold.low",
                  strategy.endsWith("MAP") ? "0" : "90");
        }
      }
      if (Files.exists(root.resolve("window-two-phase")))
        table.getConfig().set("table.optimizer.agg-phase-strategy", "TWO_PHASE");
      if (Files.exists(root.resolve("window-split-distinct")))
        table.getConfig().set("table.optimizer.distinct-agg.split.enabled", "true");
      if (Files.exists(root.resolve("state-ttl"))) {
        table
            .getConfig()
            .set("table.exec.state.ttl", Files.readString(root.resolve("state-ttl")).trim());
      }
      boolean twoPhase = Files.exists(root.resolve("two-phase-mini-batch"));
      boolean miniBatch = twoPhase || Files.exists(root.resolve("one-phase-mini-batch"));
      table.getConfig().set("table.exec.mini-batch.enabled", Boolean.toString(miniBatch));
      if (miniBatch) {
        if (twoPhase) table.getConfig().set("table.optimizer.agg-phase-strategy", "TWO_PHASE");
        table
            .getConfig()
            .set(
                "table.exec.mini-batch.size",
                Files.readString(
                        root.resolve(twoPhase ? "two-phase-mini-batch" : "one-phase-mini-batch"))
                    .trim());
        table.getConfig().set("table.exec.mini-batch.allow-latency", "10 s");
      }
      table.getConfig().set("streamfusion.transpose.batchRows", Integer.toString(batchRows));
      table.getConfig().set("table.plan.compile.catalog-objects", "SCHEMA");
      table.getConfig().set("table.plan.restore.catalog-objects", "IDENTIFIER");
      if (Files.exists(root.resolve("history-aggregate"))) {
        table.executeSql(
            "CREATE TEMPORARY SYSTEM FUNCTION fixture_history AS '"
                + CanonicalFixtureFactory.HistoryAggregate.class.getName()
                + "'");
      }
      registerInput(
          table, root, "input_table", "input.csv", ready, limit, fixedConnectorParallelism);
      if (twoInputs)
        registerInput(
            table,
            root,
            "right_input_table",
            "right-input.csv",
            rightReady,
            limit,
            fixedConnectorParallelism);
      table.executeSql(
          "CREATE TABLE output_table (k BIGINT, n BIGINT, total BIGINT"
              + (sinkMaterializer ? ",PRIMARY KEY(k) NOT ENFORCED" : "")
              + ") WITH ("
              + "'connector'='canonical-fixture','output'='"
              + sql(root.resolve("committed"))
              + "','witness'='"
              + sql(witness)
              + "'"
              + fixedConnectorParallelism
              + ")");
      if (Files.exists(root.resolve("query-2.sql"))) {
        table.executeSql(
            "CREATE TABLE output_table_two (k BIGINT, n BIGINT, total BIGINT) WITH ("
                + "'connector'='canonical-fixture','output'='"
                + sql(root.resolve("committed-two"))
                + "','witness'='"
                + sql(root.resolve(stage + "-native-witness-two"))
                + "')");
      }
      Path planFile = root.resolve("plan.json");
      if (Files.exists(root.resolve("views.sql"))) {
        table.executeSql(Files.readString(root.resolve("views.sql")));
      }
      boolean ordinarySql = Files.exists(root.resolve("ordinary-sql"));
      org.apache.flink.table.api.CompiledPlan plan = null;
      String portablePlan = "";
      String explain = "";
      if (!ordinarySql) {
        if (Files.exists(planFile)) {
          plan = table.loadPlan(PlanReference.fromFile(planFile.toFile()));
        } else if (Files.exists(root.resolve("query-2.sql"))) {
          var statements = table.createStatementSet();
          statements.addInsertSql(Files.readString(root.resolve("query.sql")));
          statements.addInsertSql(Files.readString(root.resolve("query-2.sql")));
          plan = statements.compilePlan();
        } else {
          plan = table.compilePlanSql(Files.readString(root.resolve("query.sql")));
        }
        if (!Files.exists(planFile)) {
          String json = plan.asJsonString();
          if (json.contains("tech.streamfusion") || json.contains("stream-exec-native")) {
            throw new AssertionError("compiled plan contains native-only nodes");
          }
          Files.writeString(planFile, json);
        }
        portablePlan = plan.asJsonString();
        if (sinkMaterializer) {
          String selected = Files.readString(root.resolve("sink-materializer-strategy")).trim();
          String expected = selected.startsWith("ADAPTIVE") ? "ADAPTIVE" : selected;
          var nodes =
              new org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper()
                  .readTree(portablePlan)
                  .path("nodes");
          boolean matched = false;
          for (var node : nodes)
            if (node.path("type").asText().startsWith("stream-exec-sink")) {
              String actual = node.path("upsertMaterializeStrategy").asText("LEGACY");
              if (!actual.equals(expected))
                throw new AssertionError(
                    "Fixture selected the wrong materializer strategy: " + portablePlan);
              matched = true;
            }
          if (!matched) throw new AssertionError("Fixture has no stock sink");
        }
        if (Files.exists(root.resolve("required-plan-node"))
            && !portablePlan.contains(
                Files.readString(root.resolve("required-plan-node")).trim())) {
          throw new AssertionError("Fixture compiled a different stock operator: " + portablePlan);
        }
        if (Files.exists(root.resolve("require-rank-map-state"))) {
          var graph =
              new org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper()
                  .readTree(portablePlan);
          boolean retainedMap = false;
          for (var node : graph.path("nodes"))
            if (node.path("type").asText().startsWith("stream-exec-rank_"))
              retainedMap |= node.path("outputType").toString().contains("MAP");
          if (!retainedMap)
            throw new AssertionError("Fixture did not retain a MAP payload in rank state");
        }
        explain = plan.explain(ExplainDetail.JSON_EXECUTION_PLAN);
        Files.writeString(root.resolve(stage + "-plan.txt"), explain);
      }
      String expectedOperator =
          Files.exists(root.resolve("native-operator"))
              ? Files.readString(root.resolve("native-operator")).trim()
              : "NativeColumnarGroupAggregate";
      if (!ordinarySql) verifyNativePlan(root, nativeRun, expectedOperator, explain);
      else if (Files.exists(planFile))
        throw new AssertionError("ordinary SQL fixture must not load a compiled plan");
      var result =
          ordinarySql
              ? table.executeSql(Files.readString(root.resolve("query.sql")))
              : plan.execute();
      var job = result.getJobClient().orElseThrow();
      var completion = job.getJobExecutionResult();
      if (limit == Integer.MAX_VALUE) {
        job.getJobExecutionResult().get(90, TimeUnit.SECONDS);
      } else {
        long deadline = System.nanoTime() + Duration.ofSeconds(60).toNanos();
        while (!Files.exists(ready)
            || twoInputs && !Files.exists(rightReady)
            || !Files.exists(Path.of(witness + "-sink-ready"))
            || Files.exists(root.resolve("wait-sink-output"))
                && !Files.exists(Path.of(witness + "-sink-output-ready"))
            || nativeRun
                && limit > restoredLimit
                && Files.exists(root.resolve("wait-native-input"))
                && !nativeFamiliesProcessed(root)
            || Files.exists(root.resolve("finished-branch"))
                && !completedBranchFinished(cluster, job.getJobID(), root, stage)) {
          if (System.nanoTime() >= deadline) {
            var execution = cluster.getExecutionGraph(job.getJobID()).get(5, TimeUnit.SECONDS);
            if (execution.getFailureInfo() != null)
              System.err.println(execution.getFailureInfo().getExceptionAsString());
            for (var vertex : execution.getAllExecutionVertices()) {
              System.err.println(
                  vertex.getTaskNameWithSubtaskIndex() + ": " + vertex.getExecutionState());
            }
            Thread.getAllStackTraces()
                .forEach(
                    (thread, stack) -> {
                      System.err.println(thread.getName() + ": " + thread.getState());
                      for (StackTraceElement frame : stack) System.err.println("  at " + frame);
                    });
            throw new AssertionError("source did not reach the handoff boundary");
          }
          if (completion.isDone()) {
            completion.get(5, TimeUnit.SECONDS);
            throw new AssertionError("job ended before the handoff boundary");
          }
          var key = watcher.poll(1, TimeUnit.SECONDS);
          if (key != null) {
            key.pollEvents();
            key.reset();
          }
        }
        CommonTestUtils.waitForAllTaskRunning(
            cluster, job.getJobID(), Files.exists(root.resolve("finished-branch")));
        String savepoint;
        try {
          if (nativeCheckpoint) {
            savepoint = cluster.triggerCheckpoint(job.getJobID()).get(90, TimeUnit.SECONDS);
            if (!expectCommitFailure) job.cancel().get(30, TimeUnit.SECONDS);
          } else
            savepoint =
                job.stopWithSavepoint(
                        false,
                        root.resolve("savepoints").toUri().toString(),
                        SavepointFormatType.CANONICAL)
                    .get(90, TimeUnit.SECONDS);
        } catch (Exception checkpointFailure) {
          var execution = cluster.getExecutionGraph(job.getJobID()).get(5, TimeUnit.SECONDS);
          if (execution.getFailureInfo() != null)
            System.err.println(execution.getFailureInfo().getExceptionAsString());
          if (job.getJobStatus().get(5, TimeUnit.SECONDS).isGloballyTerminalState()) {
            try {
              job.getJobExecutionResult().get(10, TimeUnit.SECONDS);
            } catch (Exception jobFailure) {
              jobFailure.addSuppressed(checkpointFailure);
              throw jobFailure;
            }
          }
          throw checkpointFailure;
        }
        Files.writeString(root.resolve(stage + "-savepoint"), savepoint);
        Files.writeString(root.resolve(stage + "-limit"), Integer.toString(limit));
        try {
          job.getJobExecutionResult().get(30, TimeUnit.SECONDS);
          if (nativeCheckpoint) throw new AssertionError("checkpointed job was not cancelled");
        } catch (java.util.concurrent.ExecutionException expectedCancellation) {
          if (!nativeCheckpoint
              || job.getJobStatus().get(5, TimeUnit.SECONDS)
                  != (expectCommitFailure
                      ? org.apache.flink.api.common.JobStatus.FAILED
                      : org.apache.flink.api.common.JobStatus.CANCELED)) throw expectedCancellation;
          if (expectCommitFailure) {
            var failure =
                cluster.getExecutionGraph(job.getJobID()).get(5, TimeUnit.SECONDS).getFailureInfo();
            if (failure == null
                || !failure.getExceptionAsString().contains("fixture commit failure"))
              throw expectedCancellation;
            Files.writeString(
                root.resolve(stage + "-commit-failure-verified"), failure.getExceptionAsString());
          }
        }
      }
      if (ordinarySql) {
        var execution = cluster.getExecutionGraph(job.getJobID()).get(30, TimeUnit.SECONDS);
        var actualPlan = new StringBuilder(String.valueOf(execution.getStreamGraphJson()));
        for (var vertex : execution.getAllExecutionVertices())
          actualPlan.append('\n').append(vertex.getTaskNameWithSubtaskIndex());
        explain = actualPlan.toString();
        Files.writeString(root.resolve(stage + "-plan.txt"), explain);
        verifyNativePlan(root, nativeRun, expectedOperator, explain);
        Files.writeString(
            root.resolve(stage + "-ordinary-sql-verified"),
            Files.readString(root.resolve("query.sql")));
      }
      if (nativeRun
          && limit > restoredLimit
          && !Files.exists(witness)
          && EvidenceReporter.inputCounters.stream().noneMatch(counter -> counter.getCount() > 0)) {
        throw new AssertionError("native operator processed no records");
      }
      if (nativeRun && Files.exists(root.resolve("history-aggregate"))) {
        var execution = cluster.getExecutionGraph(job.getJobID()).get(5, TimeUnit.SECONDS);
        String stateBackend =
            execution.getStateBackendName().orElseThrow().toLowerCase(java.util.Locale.ROOT);
        String expectedBackend = backend.equals("rocksdb") ? "rocksdb" : "hashmap";
        if (!stateBackend.contains(expectedBackend))
          throw new AssertionError(
              "JVM aggregate did not use its stock managed backend: " + stateBackend);
      } else if (nativeRun) {
        String expectedStorage = backend.equals("rocksdb") ? "rocksdb" : "memory";
        if (Files.exists(root.resolve("native-storage"))) {
          expectedStorage = Files.readString(root.resolve("native-storage")).trim();
          if (!backend.equals("rocksdb") || !expectedStorage.equals("rocksdb-snapshot"))
            throw new AssertionError(
                "Unexpected native fixture storage contract: " + expectedStorage);
        }
        if (!EvidenceReporter.observedStorage.contains(expectedStorage))
          throw new AssertionError(
              "native state backend did not execute: " + EvidenceReporter.observedStorage);
      }
      if (nativeRun && Files.exists(root.resolve("rescale"))) {
        String families =
            Files.exists(root.resolve("native-rescale-family"))
                ? Files.readString(root.resolve("native-rescale-family")).trim()
                : "GroupAgg";
        for (String family : families.split(",")) {
          long activeTasks =
              EvidenceReporter.inputsByOperator.entrySet().stream()
                  .filter(entry -> entry.getKey().contains(family))
                  .flatMap(entry -> entry.getValue().stream())
                  .filter(counter -> counter.getCount() > 0)
                  .count();
          if (activeTasks != parallelism)
            throw new AssertionError(
                "Expected "
                    + parallelism
                    + " active native "
                    + family
                    + " tasks, observed "
                    + activeTasks);
        }
      }
      if (!ordinarySql && !portablePlan.equals(plan.asJsonString())) {
        throw new AssertionError("runtime lowering mutated the portable compiled plan");
      }
      if (nativeRun
          && limit > restoredLimit
          && Files.exists(root.resolve("native-input-families"))) {
        for (String family :
            Files.readString(root.resolve("native-input-families")).trim().split(",")) {
          boolean processed =
              EvidenceReporter.inputsByOperator.entrySet().stream()
                  .filter(entry -> entry.getKey().contains(family))
                  .flatMap(entry -> entry.getValue().stream())
                  .anyMatch(counter -> counter.getCount() > 0);
          if (!processed)
            throw new AssertionError(
                "native operator family processed no input: "
                    + family
                    + "; observed="
                    + EvidenceReporter.inputsByOperator.entrySet().stream()
                        .map(
                            entry ->
                                entry.getKey()
                                    + "="
                                    + entry.getValue().stream()
                                        .mapToLong(org.apache.flink.metrics.Counter::getCount)
                                        .sum())
                        .collect(java.util.stream.Collectors.toList()));
        }
      }
      if (nativeRun && Files.exists(root.resolve("toggle-native-off"))) {
        int registeredNativeCounters = EvidenceReporter.inputCounters.size();
        table.getConfig().set("streamfusion.native.enabled", "false");
        table
            .getConfig()
            .set(
                "execution.savepoint.path",
                Files.readString(root.resolve(stage + "-savepoint")).trim());
        table.getConfig().set("execution.savepoint.restore-mode", "NO_CLAIM");
        table.executeSql("ALTER TABLE input_table SET ('limit'='" + Integer.MAX_VALUE + "')");
        var stockPlan = table.loadPlan(PlanReference.fromFile(planFile.toFile()));
        if (stockPlan.explain().contains("NativeColumnar")) {
          throw new AssertionError("native execution remained enabled");
        }
        stockPlan.execute().await();
        if (registeredNativeCounters != EvidenceReporter.inputCounters.size()) {
          throw new AssertionError("stock execution registered native operators");
        }
      }
    } finally {
      Thread.currentThread().setContextClassLoader(original);
    }
  }

  private static void verifyNativePlan(
      Path root, boolean nativeRun, String expectedOperator, String explain) throws Exception {
    if (nativeRun
        && java.util.Arrays.stream(expectedOperator.split(","))
            .anyMatch(expected -> !explain.contains(expected))) {
      throw new AssertionError("expected native operators: " + explain);
    }
    if (nativeRun && Files.exists(root.resolve("native-absent-operators"))) {
      for (String absent :
          Files.readString(root.resolve("native-absent-operators")).trim().split(","))
        if (explain.contains(absent))
          throw new AssertionError("Unexpected native admission: " + explain);
    }
  }

  private static boolean completedBranchFinished(
      MiniCluster cluster, org.apache.flink.api.common.JobID job, Path root, String stage)
      throws Exception {
    var execution = cluster.getExecutionGraph(job).get(5, TimeUnit.SECONDS);
    for (var vertex : execution.getAllExecutionVertices()) {
      if (vertex.getTaskNameWithSubtaskIndex().contains("completed_input")
          && vertex.getExecutionState()
              == org.apache.flink.runtime.execution.ExecutionState.FINISHED) {
        Files.writeString(
            root.resolve(stage + "-finished-branch"), vertex.getTaskNameWithSubtaskIndex());
        return true;
      }
    }
    return false;
  }

  private static String sql(Path path) {
    return path.toString().replace("'", "''");
  }

  private static void registerInput(
      StreamTableEnvironment table,
      Path root,
      String name,
      String input,
      Path ready,
      int limit,
      String parallelism)
      throws IOException {
    if (Files.exists(root.resolve("rescale-materializer"))) parallelism = "";
    Path columnsFile = root.resolve(name + "-columns.sql");
    if (!Files.exists(columnsFile)) columnsFile = root.resolve("input-columns.sql");
    String columns =
        Files.exists(columnsFile) ? Files.readString(columnsFile) : "k BIGINT, v BIGINT";
    table.executeSql(
        "CREATE TABLE "
            + name
            + " ("
            + columns
            + (Files.exists(root.resolve("insert-only")) ? ", pt AS PROCTIME()" : "")
            + ") WITH ('connector'='canonical-fixture','input'='"
            + sql(root.resolve(input))
            + "','limit'='"
            + limit
            + "','ready'='"
            + sql(ready)
            + "','insert-only'='"
            + Files.exists(root.resolve("insert-only"))
            + "','upsert'='"
            + Files.exists(root.resolve("upsert-source"))
            + "'"
            + parallelism
            + ")");
  }

  private static boolean nativeFamiliesProcessed(Path root) throws Exception {
    Path families = root.resolve("native-ready-families");
    if (!Files.exists(families)) families = root.resolve("native-input-families");
    for (String family : Files.readString(families).trim().split(","))
      if (EvidenceReporter.inputsByOperator.entrySet().stream()
          .filter(entry -> entry.getKey().contains(family))
          .flatMap(entry -> entry.getValue().stream())
          .noneMatch(counter -> counter.getCount() > 0)) return false;
    return true;
  }

  public static final class EvidenceReporterFactory
      implements org.apache.flink.metrics.reporter.MetricReporterFactory {
    @Override
    public org.apache.flink.metrics.reporter.MetricReporter createMetricReporter(
        java.util.Properties properties) {
      return new EvidenceReporter();
    }
  }

  public static final class EvidenceReporter
      implements org.apache.flink.metrics.reporter.MetricReporter {
    static final java.util.Set<String> observedStorage =
        java.util.concurrent.ConcurrentHashMap.newKeySet();
    static final java.util.List<org.apache.flink.metrics.Counter> inputCounters =
        new java.util.concurrent.CopyOnWriteArrayList<>();
    static final java.util.Map<String, java.util.List<org.apache.flink.metrics.Counter>>
        inputsByOperator = new java.util.concurrent.ConcurrentHashMap<>();

    @Override
    public void open(org.apache.flink.metrics.MetricConfig config) {}

    @Override
    public void close() {}

    @Override
    public void notifyOfAddedMetric(
        org.apache.flink.metrics.Metric metric,
        String name,
        org.apache.flink.metrics.MetricGroup group) {
      if (name.equals("nativeStateStorage")) {
        observedStorage.add(
            String.valueOf(((org.apache.flink.metrics.Gauge<?>) metric).getValue()));
      }
      if (name.equals("numRecordsIn")
          && metric instanceof org.apache.flink.metrics.Counter
          && group.getAllVariables().getOrDefault("<operator_name>", "").startsWith("Native")) {
        inputCounters.add((org.apache.flink.metrics.Counter) metric);
        inputsByOperator
            .computeIfAbsent(
                group.getAllVariables().get("<operator_name>"),
                ignored -> new java.util.concurrent.CopyOnWriteArrayList<>())
            .add((org.apache.flink.metrics.Counter) metric);
      }
    }

    @Override
    public void notifyOfRemovedMetric(
        org.apache.flink.metrics.Metric metric,
        String name,
        org.apache.flink.metrics.MetricGroup group) {}
  }

  private static ClassLoader nativePlannerLoader(ClassLoader parent, Path directory)
      throws IOException {
    String service = "META-INF/services/org.apache.flink.table.factories.Factory";
    var providers = new LinkedHashSet<String>();
    for (URL resource : Collections.list(parent.getResources(service))) {
      try (var input = resource.openStream()) {
        new String(input.readAllBytes(), java.nio.charset.StandardCharsets.UTF_8)
            .lines()
            .map(line -> line.split("#", 2)[0].trim())
            .filter(line -> !line.isEmpty())
            .filter(line -> !line.endsWith(".DefaultPlannerFactory"))
            .filter(line -> !line.endsWith(".DelegatePlannerFactory"))
            .forEach(providers::add);
      }
    }
    providers.add("tech.streamfusion.planner.StreamFusionPlannerFactory");
    Path file = directory.resolve("native-planner-services.txt");
    Files.write(file, providers);
    return new FactoryLoader(parent, file.toUri().toURL());
  }

  private static final class FactoryLoader extends ClassLoader {
    private final URL services;

    FactoryLoader(ClassLoader parent, URL services) {
      super(parent);
      this.services = services;
    }

    @Override
    public Enumeration<URL> getResources(String name) throws IOException {
      return name.equals("META-INF/services/org.apache.flink.table.factories.Factory")
          ? Collections.enumeration(java.util.List.of(services))
          : super.getResources(name);
    }
  }
}
