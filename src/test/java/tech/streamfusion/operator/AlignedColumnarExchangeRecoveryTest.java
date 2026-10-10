package tech.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.runtime.checkpoint.metadata.CheckpointMetadata;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.transformations.PartitionTransformation;
import org.apache.flink.streaming.api.transformations.StreamExchangeMode;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import tech.streamfusion.compat.ListCollectors;
import tech.streamfusion.planner.ColumnarKeyGroupPartitioner;

class AlignedColumnarExchangeRecoveryTest {
  private static final java.util.Map<String, org.apache.flink.runtime.jobgraph.OperatorID>
      OPERATOR_IDS = new java.util.HashMap<>();

  private static final int ROWS = 12_000;
  private static final int MAX_PARALLELISM = 257;
  private static final AtomicBoolean FAILED_ONCE = new AtomicBoolean();
  private static final RowType ROW_TYPE =
      RowType.of(new LogicalType[] {new BigIntType(), new BigIntType()}, new String[] {"k", "id"});

  @Test
  void restoresAfterAlignedExchangeCheckpointAndRescale(
      @TempDir Path checkpoints, @TempDir Path output) throws Exception {
    FAILED_ONCE.set(false);
    try {
      runJob(2, checkpoints, output, null, false);
    } catch (Exception expectedFailure) {
      // Restart is disabled so the intentional post-checkpoint failure leaves a retained checkpoint.
    }
    assertTrue(FAILED_ONCE.get(), "first job never failed after a completed checkpoint");

    Path retained = latestRetainedCheckpoint(checkpoints);
    CheckpointMetadata metadata =
        org.apache.flink.test.util.TestUtils.loadCheckpointMetadata(retained.toString());
    assertTrue(
        metadata.getOperatorStates().stream()
            .filter(
                operator ->
                    operator.getOperatorID().equals(OPERATOR_IDS.get("key-group-split"))
                        || operator.getOperatorID().equals(OPERATOR_IDS.get("arrow-to-row")))
            .flatMap(operator -> operator.getStates().stream())
            .noneMatch(
                state ->
                    !state.getInputChannelState().isEmpty()
                        || !state.getResultSubpartitionState().isEmpty()),
        "aligned Arrow exchange retained topology-specific channel state");

    runJob(3, checkpoints, output, retained, false);

    List<String> lines;
    try (var paths = Files.walk(output)) {
      lines =
          paths
              .filter(Files::isRegularFile)
              .filter(path -> !path.getFileName().toString().startsWith("."))
              .flatMap(
                  path -> {
                    try {
                      return Files.readAllLines(path).stream();
                    } catch (java.io.IOException e) {
                      throw new java.io.UncheckedIOException(e);
                    }
                  })
              .collect(ListCollectors.toList());
    }
    assertEquals(ROWS, lines.size(), "aligned restore lost or duplicated rows");
    Set<String> unique = new HashSet<>(lines);
    assertEquals(ROWS, unique.size(), "every source id must appear exactly once");
    for (int id = 0; id < ROWS; id++) {
      assertTrue(unique.contains(Integer.toString(id)), "missing id " + id);
    }
  }

  @Test
  void restoresCapturedArrowChannelStateAfterUnalignedRescale(
      @TempDir Path checkpoints, @TempDir Path output) throws Exception {
    assertUnalignedRestore(2, 3, checkpoints, output);
  }

  @Test
  void restoresCapturedArrowChannelStateAfterUnalignedDownscale(
      @TempDir Path checkpoints, @TempDir Path output) throws Exception {
    assertUnalignedRestore(3, 2, checkpoints, output);
  }

  private static void assertUnalignedRestore(
      int beforeParallelism, int afterParallelism, Path checkpoints, Path output) throws Exception {
    FAILED_ONCE.set(false);
    try {
      runJob(beforeParallelism, checkpoints, output, null, true);
    } catch (Exception expectedFailure) {
      // The completed externalized checkpoint is the restore input below.
    }
    assertTrue(FAILED_ONCE.get(), "first job never failed after a completed checkpoint");

    Path retained = latestRetainedCheckpoint(checkpoints);
    CheckpointMetadata metadata =
        org.apache.flink.test.util.TestUtils.loadCheckpointMetadata(retained.toString());
    assertTrue(
        metadata.getOperatorStates().stream()
            .filter(
                operator ->
                    operator.getOperatorID().equals(OPERATOR_IDS.get("key-group-split"))
                        || operator
                            .getOperatorID()
                            .equals(OPERATOR_IDS.get("key-group-reassemble")))
            .flatMap(operator -> operator.getStates().stream())
            .anyMatch(
                state ->
                    !state.getInputChannelState().isEmpty()
                        || !state.getResultSubpartitionState().isEmpty()),
        "unaligned checkpoint did not capture Arrow exchange channel state");

    runJob(afterParallelism, checkpoints, output, retained, true);
    assertExactlyOnceOutput(output, "unaligned restore");
  }

  private static void runJob(
      int parallelism,
      Path checkpoints,
      Path output,
      Path restoreFrom,
      boolean recoverable)
      throws Exception {
    Configuration configuration = new Configuration();
    configuration.set(RestartStrategyOptions.RESTART_STRATEGY, "disable");
    configuration.set(
        CheckpointingOptions.CHECKPOINTS_DIRECTORY, checkpoints.toUri().toString());
    tech.streamfusion.compat.CheckpointTestConfig.retainOnCancellation(configuration);
    if (restoreFrom != null) {
      tech.streamfusion.compat.CheckpointTestConfig.restore(
          configuration, restoreFrom.toUri().toString());
    }
    StreamExecutionEnvironment env =
        StreamExecutionEnvironment.getExecutionEnvironment(configuration);
    env.setParallelism(parallelism);
    env.setMaxParallelism(MAX_PARALLELISM);
    env.enableCheckpointing(50);
    tech.streamfusion.compat.CheckpointTestCapabilities.configure(env, recoverable);

    DataStream<RowData> rows =
        env.fromSequence(0, ROWS - 1)
            .uid("aligned-source")
            .map(value -> (RowData) GenericRowData.of(value % 97, value))
            .returns(InternalTypeInfo.of(ROW_TYPE))
            .uid("aligned-rows");
    DataStream<ArrowBatch> columnar =
        rows.transform(
                "row-to-arrow",
                ArrowBatchTypeInformation.INSTANCE,
                new RowDataToArrowOperator(ROW_TYPE, 64, false, null))
            .uid("aligned-row-to-arrow")
            .setMaxParallelism(MAX_PARALLELISM);
    DataStream<ArrowBatch> split =
        columnar
            .transform(
                "key-group-split",
                ArrowBatchTypeInformation.INSTANCE,
                new SplitByKeyGroupOperator(
                    new int[] {0},
                    new int[] {-1},
                    MAX_PARALLELISM,
                    parallelism,
                    recoverable))
            .uid("aligned-key-group-split")
            .setMaxParallelism(MAX_PARALLELISM);
    PartitionTransformation<ArrowBatch> partition =
        new PartitionTransformation<>(
            split.getTransformation(),
            new ColumnarKeyGroupPartitioner(MAX_PARALLELISM, recoverable),
            StreamExchangeMode.PIPELINED);
    partition.setParallelism(parallelism);
    partition.setMaxParallelism(MAX_PARALLELISM);
    DataStream<ArrowBatch> shuffled = new DataStream<>(env, partition);
    if (recoverable) {
      shuffled =
          shuffled
              .transform(
                  "key-group-reassemble",
                  ArrowBatchTypeInformation.INSTANCE,
                  new OrderedKeyGroupReassembler(MAX_PARALLELISM))
              .uid("aligned-key-group-reassemble")
              .setMaxParallelism(MAX_PARALLELISM);
    }
    DataStream<RowData> restoredRows =
        shuffled
            .transform(
                "arrow-to-row",
                InternalTypeInfo.of(ROW_TYPE),
                new ArrowToRowDataOperator(ROW_TYPE))
            .uid("aligned-arrow-to-row")
            .setMaxParallelism(MAX_PARALLELISM);

    tech.streamfusion.compat.CheckpointFileSink.attach(
        restoredRows.map(new SlowCheckpointFailingMap()).uid("aligned-failing-map"),
        output,
        "aligned-file-sink");
    var graph = env.getStreamGraph();
    var hashes =
        new org.apache.flink.streaming.api.graph.StreamGraphHasherV2()
            .traverseStreamGraphAndGenerateHashes(graph);
    OPERATOR_IDS.clear();
    for (var node : graph.getStreamNodes()) {
      for (String name : new String[] {"key-group-split", "key-group-reassemble", "arrow-to-row"}) {
        if (node.getOperatorName().equals(name))
          OPERATOR_IDS.put(
              name, new org.apache.flink.runtime.jobgraph.OperatorID(hashes.get(node.getId())));
      }
    }
    assertTrue(OPERATOR_IDS.containsKey("key-group-split"));
    assertTrue(OPERATOR_IDS.containsKey(recoverable ? "key-group-reassemble" : "arrow-to-row"));
    env.execute(graph);
  }

  private static void assertExactlyOnceOutput(Path output, String message) throws Exception {
    List<String> lines;
    try (var paths = Files.walk(output)) {
      lines =
          paths
              .filter(Files::isRegularFile)
              .filter(path -> !path.getFileName().toString().startsWith("."))
              .flatMap(
                  path -> {
                    try {
                      return Files.readAllLines(path).stream();
                    } catch (java.io.IOException e) {
                      throw new java.io.UncheckedIOException(e);
                    }
                  })
              .collect(ListCollectors.toList());
    }
    assertEquals(ROWS, lines.size(), message + " lost or duplicated rows");
    Set<String> unique = new HashSet<>(lines);
    assertEquals(ROWS, unique.size(), "every source id must appear exactly once");
    for (int id = 0; id < ROWS; id++) {
      assertTrue(unique.contains(Integer.toString(id)), "missing id " + id);
    }
  }

  private static Path latestRetainedCheckpoint(Path checkpoints) throws Exception {
    try (var paths = Files.walk(checkpoints)) {
      return paths
          .filter(path -> path.getFileName().toString().equals("_metadata"))
          .map(Path::getParent)
          .max(
              java.util.Comparator.comparingLong(
                  checkpoint ->
                      Long.parseLong(
                          checkpoint.getFileName().toString().substring("chk-".length()))))
          .orElseThrow(() -> new AssertionError("no retained checkpoint found"));
    }
  }

  private static final class SlowCheckpointFailingMap extends RichMapFunction<RowData, String>
      implements CheckpointListener {
    private long seen;

    @Override
    public String map(RowData value) {
      seen++;
      LockSupport.parkNanos(500_000);
      return Long.toString(value.getLong(1));
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {
      if (seen >= 100 && FAILED_ONCE.compareAndSet(false, true)) {
        throw new RuntimeException("intentional failure after aligned checkpoint " + checkpointId);
      }
    }
  }
}
