package tech.streamfusion;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import tech.streamfusion.compat.SourceFunction;

/** Fail only after a checkpoint containing a nonempty aggregate prefix has completed. */
final class PortableSqlRecovery implements Supplier<TableEnvironment>, AutoCloseable {
  private static final Map<String, Proof> PROOFS = new ConcurrentHashMap<>();
  private final String stateBackend;
  private final List<Row> input;
  private final TypeInformation<Row> inputType;
  private final Schema schema;
  private final int[] boundaries;
  private final boolean changelog;
  private Map<Integer, Long> watermarks = Map.of();
  private long taskOffHeapBytes;
  private boolean holdAfterInput;
  private int parallelism = 1;
  private java.nio.file.Path checkpoints;
  private java.nio.file.Path restoreFrom;
  private StreamExecutionEnvironment executionEnvironment;
  private final List<String> runIds = new java.util.ArrayList<>();

  PortableSqlRecovery() {
    this("hashmap");
  }

  PortableSqlRecovery(String stateBackend) {
    this(
        stateBackend,
        java.util.stream.IntStream.range(0, 96).mapToObj(i -> Row.of(i % 3, (long) i)).toList(),
        Types.ROW_NAMED(new String[] {"k", "v"}, Types.INT, Types.LONG),
        Schema.newBuilder().column("k", DataTypes.INT()).column("v", DataTypes.BIGINT()).build(),
        false,
        32);
  }

  PortableSqlRecovery(
      String backend,
      List<Row> input,
      TypeInformation<Row> inputType,
      Schema schema,
      boolean changelog,
      int... boundaries) {
    this.stateBackend = backend;
    this.input = List.copyOf(input);
    this.inputType = inputType;
    this.schema = schema;
    this.changelog = changelog;
    this.boundaries = boundaries.clone();
    int previous = 0;
    for (int boundary : boundaries) {
      if (boundary <= previous || boundary >= input.size())
        throw new IllegalArgumentException(
            "checkpoint boundaries must be ascending input prefixes");
      previous = boundary;
    }
  }

  PortableSqlRecovery withWatermarks(Map<Integer, Long> afterOffsets) {
    if (!runIds.isEmpty()) throw new IllegalStateException("configure watermarks before execution");
    watermarks = Map.copyOf(afterOffsets);
    return this;
  }

  PortableSqlRecovery withTaskOffHeapBytes(long bytes) {
    if (!runIds.isEmpty() || bytes < 0)
      throw new IllegalArgumentException("configure a nonnegative budget before execution");
    taskOffHeapBytes = bytes;
    return this;
  }

  PortableSqlRecovery holdAfterInput() {
    if (!runIds.isEmpty())
      throw new IllegalStateException("configure source hold before execution");
    holdAfterInput = true;
    return this;
  }

  void awaitCompletedInput() throws InterruptedException {
    Proof proof = PROOFS.get(runIds.get(runIds.size() - 1));
    long deadline = System.nanoTime() + Duration.ofSeconds(60).toNanos();
    while (!proof.completedOffsets.contains(input.size()) && System.nanoTime() < deadline)
      Thread.sleep(10);
    org.junit.jupiter.api.Assertions.assertTrue(
        proof.completedOffsets.contains(input.size()), "full input checkpoint never completed");
    org.junit.jupiter.api.Assertions.assertEquals(1, proof.activeSources.get());
  }

  PortableSqlRecovery withRescaleDeployment(
      int parallelism, java.nio.file.Path checkpoints, java.nio.file.Path restoreFrom) {
    if (!runIds.isEmpty()) throw new IllegalStateException("configure deployment before execution");
    this.parallelism = parallelism;
    this.checkpoints = checkpoints;
    this.restoreFrom = restoreFrom;
    return this;
  }

  StreamExecutionEnvironment executionEnvironment() {
    return executionEnvironment;
  }

  long taskOffHeapBytes() {
    return taskOffHeapBytes;
  }

  static final class Proof {
    final AtomicBoolean failed = new AtomicBoolean();
    final AtomicInteger restoredOffset = new AtomicInteger(-1);
    final AtomicInteger completions = new AtomicInteger();
    final java.util.Set<Integer> injected = ConcurrentHashMap.newKeySet();
    final java.util.Set<Integer> completedOffsets = ConcurrentHashMap.newKeySet();
    final List<Integer> restoredOffsets = new java.util.concurrent.CopyOnWriteArrayList<>();
    final AtomicInteger activeSources = new AtomicInteger();
    final List<Long> emittedWatermarks = new java.util.concurrent.CopyOnWriteArrayList<>();
  }

  @Override
  public TableEnvironment get() {
    return environment(true);
  }

  TableEnvironment uninterrupted() {
    return environment(false);
  }

  private TableEnvironment environment(boolean recover) {
    String runId = UUID.randomUUID().toString();
    runIds.add(runId);
    PROOFS.put(runId, new Proof());
    Configuration config = new Configuration();
    config.setString("state.backend.type", stateBackend);
    if (checkpoints != null) {
      config.set(
          org.apache.flink.configuration.CheckpointingOptions.CHECKPOINTS_DIRECTORY,
          checkpoints.toUri().toString());
      tech.streamfusion.compat.CheckpointTestConfig.retainOnCancellation(config);
      if (restoreFrom != null)
        tech.streamfusion.compat.CheckpointTestConfig.restore(
            config, restoreFrom.toUri().toString());
    }
    if (taskOffHeapBytes > 0)
      config.set(
          org.apache.flink.configuration.TaskManagerOptions.TASK_OFF_HEAP_MEMORY,
          new org.apache.flink.configuration.MemorySize(taskOffHeapBytes));
    config.set(RestartStrategyOptions.RESTART_STRATEGY, "fixed-delay");
    config.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS, boundaries.length);
    config.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_DELAY, Duration.ofMillis(10));
    StreamExecutionEnvironment env;
    if (taskOffHeapBytes > 0) {
      // The shared test cluster owns its TaskManager configuration; use a private deployment
      // when the scenario must change that process-wide budget.
      config.set(org.apache.flink.configuration.DeploymentOptions.TARGET, "local");
      config.set(org.apache.flink.configuration.DeploymentOptions.ATTACHED, true);
      env = new StreamExecutionEnvironment(config);
    } else {
      env = StreamExecutionEnvironment.getExecutionEnvironment(config);
    }
    executionEnvironment = env;
    env.setParallelism(parallelism);
    if (checkpoints != null) env.setMaxParallelism(128);
    env.enableCheckpointing(50);
    var table = StreamTableEnvironment.create(env);
    table.getConfig().set("table.optimizer.agg-phase-strategy", "ONE_PHASE");
    var stream =
        env.addSource(
                new RecoveringSource(
                    runId, input, recover ? boundaries : new int[0], watermarks, holdAfterInput))
            .returns(inputType)
            .uid("portable-recovery-source")
            .setMaxParallelism(128);
    table.createTemporaryView(
        "recovery_input",
        changelog
            ? table.fromChangelogStream(stream, schema)
            : table.fromDataStream(stream, schema));
    return table;
  }

  void verify() {
    org.junit.jupiter.api.Assertions.assertEquals(2, runIds.size(), "both engines must execute");
    for (String runId : runIds) {
      Proof proof = PROOFS.get(runId);
      org.junit.jupiter.api.Assertions.assertTrue(
          proof.failed.get(), "failure injection never fired");
      org.junit.jupiter.api.Assertions.assertEquals(
          32, proof.restoredOffset.get(), "did not restore the nonempty source checkpoint");
      org.junit.jupiter.api.Assertions.assertTrue(
          proof.completions.get() > 0, "checkpoint never completed");
    }
  }

  void verifyRepeatedRecovery() {
    org.junit.jupiter.api.Assertions.assertEquals(2, runIds.size());
    Proof reference = PROOFS.get(runIds.get(0));
    Proof recovered = PROOFS.get(runIds.get(1));
    List<Integer> expected = java.util.Arrays.stream(boundaries).boxed().toList();
    org.junit.jupiter.api.Assertions.assertTrue(reference.injected.isEmpty());
    org.junit.jupiter.api.Assertions.assertEquals(expected, recovered.restoredOffsets);
    org.junit.jupiter.api.Assertions.assertEquals(
        java.util.Set.copyOf(expected), recovered.injected);
    org.junit.jupiter.api.Assertions.assertTrue(recovered.completedOffsets.containsAll(expected));
    for (String id : runIds) {
      org.junit.jupiter.api.Assertions.assertEquals(
          new java.util.TreeMap<>(watermarks).values().stream().toList(),
          PROOFS.get(id).emittedWatermarks,
          "controlled watermark sequence");
      org.junit.jupiter.api.Assertions.assertEquals(
          0, PROOFS.get(id).activeSources.get(), "source still running after result collection");
    }
  }

  List<Map<String, Object>> observations() {
    return runIds.stream()
        .map(
            id -> {
              Proof proof = PROOFS.get(id);
              return Map.<String, Object>of(
                  "failedAfterCheckpoint",
                  proof.failed.get(),
                  "restoredSourceOffset",
                  proof.restoredOffset.get(),
                  "completedCheckpoints",
                  proof.completions.get(),
                  "completedOffsets",
                  new java.util.TreeSet<>(proof.completedOffsets),
                  "restoredOffsets",
                  List.copyOf(proof.restoredOffsets),
                  "injectedOffsets",
                  java.util.Set.copyOf(proof.injected),
                  "activeSources",
                  proof.activeSources.get(),
                  "emittedWatermarks",
                  List.copyOf(proof.emittedWatermarks));
            })
        .toList();
  }

  @Override
  public void close() {
    runIds.forEach(PROOFS::remove);
  }

  private static final class RecoveringSource
      implements SourceFunction<Row>, CheckpointedFunction, CheckpointListener {
    private final String runId;
    private volatile boolean running = true;
    private volatile int completedOffset;
    private final List<Row> input;
    private final int[] boundaries;
    private final Map<Integer, Long> watermarks;
    private final boolean holdAfterInput;
    private transient ListState<Integer> state;
    private transient Map<Long, Integer> snapshots;
    private int next;

    private RecoveringSource(
        String runId,
        List<Row> input,
        int[] boundaries,
        Map<Integer, Long> watermarks,
        boolean holdAfterInput) {
      this.runId = runId;
      this.input = input;
      this.boundaries = boundaries;
      this.watermarks = watermarks;
      this.holdAfterInput = holdAfterInput;
    }

    @Override
    public void run(SourceContext<Row> context) throws Exception {
      Proof proof = PROOFS.get(runId);
      proof.activeSources.incrementAndGet();
      try {
        for (int boundary : boundaries) {
          if (next >= boundary) continue;
          emitThrough(context, boundary);
          long deadline = System.nanoTime() + Duration.ofSeconds(60).toNanos();
          while (running && completedOffset < boundary && System.nanoTime() < deadline)
            Thread.sleep(5);
          if (!running) return;
          if (completedOffset < boundary)
            throw new IllegalStateException("no completed checkpoint captured prefix " + boundary);
          if (proof.injected.add(boundary)) {
            proof.failed.set(true);
            throw new IllegalStateException(
                "portable source failure after checkpoint at " + boundary);
          }
        }
        emitThrough(context, input.size());
        while (running && holdAfterInput) Thread.sleep(5);
      } finally {
        proof.activeSources.decrementAndGet();
      }
    }

    private void emitThrough(SourceContext<Row> context, int end) {
      synchronized (context.getCheckpointLock()) {
        while (running && next < end) {
          context.collect(input.get(next));
          next++;
          Long watermark = watermarks.get(next);
          if (watermark != null) {
            context.emitWatermark(
                new org.apache.flink.streaming.api.watermark.Watermark(watermark));
            PROOFS.get(runId).emittedWatermarks.add(watermark);
          }
        }
      }
    }

    @Override
    public void cancel() {
      running = false;
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
      state.update(List.of(next));
      snapshots.put(context.getCheckpointId(), next);
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
      state =
          context
              .getOperatorStateStore()
              .getListState(new ListStateDescriptor<>("source-offset", Integer.class));
      snapshots = new ConcurrentHashMap<>();
      if (context.isRestored()) {
        for (int offset : state.get()) next = offset;
        PROOFS.get(runId).restoredOffset.set(next);
        PROOFS.get(runId).restoredOffsets.add(next);
      }
    }

    @Override
    public void notifyCheckpointComplete(long id) {
      int offset = snapshots.getOrDefault(id, 0);
      if (offset > 0) {
        PROOFS.get(runId).completions.incrementAndGet();
        PROOFS.get(runId).completedOffsets.add(offset);
        completedOffset = Math.max(completedOffset, offset);
      }
      snapshots.keySet().removeIf(checkpoint -> checkpoint <= id);
    }
  }
}
