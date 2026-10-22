package tech.streamfusion.planner;

import org.apache.doris.flink.sink.DorisSink;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.streaming.api.transformations.PartitionTransformation;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.types.logical.RowType;
import tech.streamfusion.operator.ArrowBatch;
import tech.streamfusion.operator.ArrowBatchTypeInformation;
import tech.streamfusion.operator.NativeConstraintEnforcer;
import tech.streamfusion.operator.OrderedKeyGroupReassembler;
import tech.streamfusion.operator.SplitByKeyGroupOperator;

/**
 * Carries the stock SQL sink's constraints, parallelism and primary-key routing to its Arrow edge.
 */
final class DorisSinkRuntime {
  final DorisSink<ArrowBatch> sink;
  final RowType type;
  final Integer parallelism;
  final int[] keys;
  final int[] timestampPrecisions;
  final String[] notNull;

  DorisSinkRuntime(
      DorisSink<ArrowBatch> sink,
      RowType type,
      Integer parallelism,
      int[] keys,
      int[] timestampPrecisions,
      String[] notNull) {
    this.sink = sink;
    this.type = type;
    this.parallelism = parallelism;
    this.keys = keys;
    this.timestampPrecisions = timestampPrecisions;
    this.notNull = notNull;
  }

  Transformation<ArrowBatch> prepare(
      Transformation<ArrowBatch> input,
      StreamExecutionEnvironment environment,
      ExecNodeConfig config) {
    if (java.util.Arrays.stream(notNull).anyMatch(name -> name != null))
      input =
          new OneInputTransformation<>(
              input,
              "doris-not-null-enforcer",
              SimpleOperatorFactory.of(new NativeConstraintEnforcer(notNull)),
              ArrowBatchTypeInformation.INSTANCE,
              input.getParallelism(),
              false);
    int target = parallelism == null ? input.getParallelism() : parallelism;
    var policy = config.get(ExecutionConfigOptions.TABLE_EXEC_SINK_KEYED_SHUFFLE);
    boolean shuffle =
        keys.length != 0
            && target != 1
            && (policy == ExecutionConfigOptions.SinkKeyedShuffle.FORCE
                || policy == ExecutionConfigOptions.SinkKeyedShuffle.AUTO
                    && target != input.getParallelism());
    if (!shuffle) return input;
    int groups = FlinkKeyGroupUtils.maxParallelism(environment, target);
    boolean recoverable = environment.getCheckpointConfig().isUnalignedCheckpointsEnabled();
    var split =
        new OneInputTransformation<>(
            input,
            "doris-primary-key-split",
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
            "doris-primary-key-reassemble",
            SimpleOperatorFactory.of(new OrderedKeyGroupReassembler(groups)),
            ArrowBatchTypeInformation.INSTANCE,
            target,
            false);
    reassembled.setMaxParallelism(groups);
    return reassembled;
  }
}
