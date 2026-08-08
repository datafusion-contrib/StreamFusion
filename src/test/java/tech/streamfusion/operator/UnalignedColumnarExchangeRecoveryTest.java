package tech.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import tech.streamfusion.planner.ColumnarKeyGroupPartitioner;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExternalizedCheckpointRetention;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.configuration.StateRecoveryOptions;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.runtime.checkpoint.metadata.CheckpointMetadata;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy;
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

class UnalignedColumnarExchangeRecoveryTest {

  private static final int ROWS = 12_000;
  private static final int MAX_PARALLELISM = 257;
  private static final AtomicBoolean FAILED_ONCE = new AtomicBoolean();
  private static final RowType ROW_TYPE =
      RowType.of(new LogicalType[] {new BigIntType(), new BigIntType()}, new String[] {"k", "id"});

  @Test
  void restoresInFlightKeyGroupBatchesAfterUnalignedRescale(
      @TempDir Path checkpoints, @TempDir Path output) throws Exception {
    FAILED_ONCE.set(false);
    try {
      runJob(2, checkpoints, output, null);
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
                    operator.getOperatorName().orElse("").contains("key-group-split")
                        || operator.getOperatorName().orElse("").contains("arrow-to-row"))
            .flatMap(operator -> operator.getStates().stream())
            .anyMatch(
                state ->
                    !state.getInputChannelState().isEmpty()
                        || !state.getResultSubpartitionState().isEmpty()),
        "checkpoint did not capture in-flight state on the Arrow exchange");

    runJob(3, checkpoints, output, retained);

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
              .toList();
    }
    assertEquals(ROWS, lines.size(), "unaligned restore lost or duplicated rows");
    Set<String> unique = new HashSet<>(lines);
    assertEquals(ROWS, unique.size(), "every source id must appear exactly once");
    for (int id = 0; id < ROWS; id++) {
      assertTrue(unique.contains(Integer.toString(id)), "missing id " + id);
    }
  }

  private static void runJob(int parallelism, Path checkpoints, Path output, Path restoreFrom)
      throws Exception {
    Configuration configuration = new Configuration();
    configuration.set(RestartStrategyOptions.RESTART_STRATEGY, "disable");
    configuration.set(
        CheckpointingOptions.CHECKPOINTS_DIRECTORY, checkpoints.toUri().toString());
    configuration.set(
        CheckpointingOptions.EXTERNALIZED_CHECKPOINT_RETENTION,
        ExternalizedCheckpointRetention.RETAIN_ON_CANCELLATION);
    if (restoreFrom != null) {
      configuration.set(StateRecoveryOptions.SAVEPOINT_PATH, restoreFrom.toUri().toString());
    }
    StreamExecutionEnvironment env =
        StreamExecutionEnvironment.getExecutionEnvironment(configuration);
    env.setParallelism(parallelism);
    env.setMaxParallelism(MAX_PARALLELISM);
    env.enableCheckpointing(50);
    env.getCheckpointConfig().enableUnalignedCheckpoints();

    DataStream<RowData> rows =
        env.fromSequence(0, ROWS - 1)
            .uid("unaligned-source")
            .map(value -> (RowData) GenericRowData.of(value % 97, value))
            .returns(InternalTypeInfo.of(ROW_TYPE))
            .uid("unaligned-rows");
    DataStream<ArrowBatch> columnar =
        rows.transform(
                "row-to-arrow",
                ArrowBatchTypeInformation.INSTANCE,
                new RowDataToArrowOperator(ROW_TYPE, 64, false, null))
            .uid("unaligned-row-to-arrow")
            .setMaxParallelism(MAX_PARALLELISM);
    DataStream<ArrowBatch> split =
        columnar
            .transform(
                "key-group-split",
                ArrowBatchTypeInformation.INSTANCE,
                new SplitByKeyGroupOperator(new int[] {0}, new int[] {-1}, MAX_PARALLELISM))
            .uid("unaligned-key-group-split")
            .setMaxParallelism(MAX_PARALLELISM);
    PartitionTransformation<ArrowBatch> partition =
        new PartitionTransformation<>(
            split.getTransformation(),
            new ColumnarKeyGroupPartitioner(MAX_PARALLELISM),
            StreamExchangeMode.PIPELINED);
    partition.setParallelism(parallelism);
    partition.setMaxParallelism(MAX_PARALLELISM);
    DataStream<RowData> restoredRows =
        new DataStream<>(env, partition)
            .transform(
                "arrow-to-row",
                InternalTypeInfo.of(ROW_TYPE),
                new ArrowToRowDataOperator(ROW_TYPE))
            .uid("unaligned-arrow-to-row")
            .setMaxParallelism(MAX_PARALLELISM);

    FileSink<String> sink =
        FileSink.forRowFormat(
                new org.apache.flink.core.fs.Path(output.toUri()),
                new SimpleStringEncoder<String>("UTF-8"))
            .withRollingPolicy(OnCheckpointRollingPolicy.build())
            .build();
    restoredRows
        .map(new SlowCheckpointFailingMap())
        .uid("unaligned-failing-map")
        .sinkTo(sink)
        .uid("unaligned-file-sink");
    env.execute("unaligned-columnar-exchange-recovery");
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
        throw new RuntimeException("intentional failure after unaligned checkpoint " + checkpointId);
      }
    }
  }
}
