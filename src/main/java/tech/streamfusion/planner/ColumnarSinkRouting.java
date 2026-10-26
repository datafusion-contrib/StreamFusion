package tech.streamfusion.planner;

import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Exchange;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.streaming.api.transformations.PartitionTransformation;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.config.ExecutionConfigOptions.SinkKeyedShuffle;
import tech.streamfusion.operator.ArrowBatch;
import tech.streamfusion.operator.ArrowBatchTypeInformation;
import tech.streamfusion.operator.OrderedKeyGroupReassembler;
import tech.streamfusion.operator.SplitByKeyGroupOperator;

/**
 * Routes Arrow batches to a connector writer by sink primary key, mirroring the key-by Flink's sink
 * translation inserts before a changelog writer.
 */
final class ColumnarSinkRouting {
  private ColumnarSinkRouting() {}

  /**
   * Whether Flink's sink input sits above a hash exchange. Flink leaves that exchange at the
   * default parallelism (-1), which every node above it inherits, while the native exchange
   * resolves its destination count; Flink's sink translation compares against the unresolved value.
   * Without an exchange the native input carries its source's parallelism, as Flink does.
   */
  static boolean inheritsDefaultParallelism(RelNode flinkInput) {
    for (RelNode node = flinkInput; !node.getInputs().isEmpty(); node = node.getInput(0))
      if (node instanceof Exchange)
        return ((Exchange) node).getDistribution().getType()
            == RelDistribution.Type.HASH_DISTRIBUTED;
    return false;
  }

  /** The input parallelism that Flink's sink translation compares the writer's against. */
  static int flinkInputParallelism(Transformation<?> input, boolean inheritsDefaultParallelism) {
    return inheritsDefaultParallelism
        ? ExecutionConfig.PARALLELISM_DEFAULT
        : input.getParallelism();
  }

  /**
   * Flink's rejection of a changelog that a writer without a primary key would receive at another
   * parallelism, where the updates of one row could reach different writer subtasks.
   */
  static void requirePrimaryKeyForRedistributedChangelog(
      String table, boolean insertOnly, int[] keys, int sinkParallelism, int inputParallelism) {
    if (!insertOnly && sinkParallelism != inputParallelism && keys.length == 0)
      throw new TableException(
          String.format(
              "The sink for table '%s' has a configured parallelism of %s, while the input"
                  + " parallelism is %s. Since the configured parallelism is different from the"
                  + " input's parallelism and the changelog mode is not insert-only, a primary"
                  + " key is required but could not be found.",
              table, sinkParallelism, inputParallelism));
  }

  /**
   * Flink's sink shuffle decision over its sink and input parallelism values: upsert
   * materialization always routes by sink key; otherwise a primary key is routed when {@code
   * table.exec.sink.keyed-shuffle} asks for it and the writer has more than one subtask.
   */
  static boolean shuffles(
      SinkKeyedShuffle policy, int[] keys, int target, int inputParallelism, boolean materialize) {
    return materialize
        || keys.length != 0
            && target != 1
            && (policy == SinkKeyedShuffle.FORCE
                || policy == SinkKeyedShuffle.AUTO && target != inputParallelism);
  }

  /**
   * Splits each batch by key group and partitions the pieces to {@code target} writer subtasks.
   * With unaligned checkpoints, a reassembler restores the per-key order of recovered pieces.
   */
  static Transformation<ArrowBatch> route(
      Transformation<ArrowBatch> input,
      StreamExecutionEnvironment environment,
      int[] keys,
      int[] timestampPrecisions,
      int target,
      String connector) {
    int groups = FlinkKeyGroupUtils.maxParallelism(environment, target);
    boolean recoverable = environment.getCheckpointConfig().isUnalignedCheckpointsEnabled();
    var split =
        new OneInputTransformation<>(
            input,
            connector + "-primary-key-split",
            SimpleOperatorFactory.of(
                new SplitByKeyGroupOperator(keys, timestampPrecisions, groups, recoverable)),
            ArrowBatchTypeInformation.INSTANCE,
            input.getParallelism(),
            false);
    var partition =
        new PartitionTransformation<>(split, new ColumnarKeyGroupPartitioner(groups, recoverable));
    partition.setParallelism(target);
    partition.setMaxParallelism(groups);
    if (!recoverable) return partition;
    var reassembled =
        new OneInputTransformation<>(
            partition,
            connector + "-primary-key-reassemble",
            SimpleOperatorFactory.of(new OrderedKeyGroupReassembler(groups)),
            ArrowBatchTypeInformation.INSTANCE,
            target,
            false);
    reassembled.setMaxParallelism(groups);
    return reassembled;
  }
}
