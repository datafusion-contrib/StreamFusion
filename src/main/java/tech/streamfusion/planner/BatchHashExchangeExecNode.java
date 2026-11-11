package tech.streamfusion.planner;

import java.util.List;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.transformations.PartitionTransformation;
import org.apache.flink.streaming.api.transformations.StreamExchangeMode;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.SingleTransformationTranslator;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecNode;
import org.apache.flink.table.types.logical.RowType;
import tech.streamfusion.operator.ArrowBatch;

/**
 * Flink owns the blocking shuffle and its replay; buffers use the ordinary Arrow IPC serializer.
 */
final class BatchHashExchangeExecNode extends ExecNodeBase<ArrowBatch>
    implements BatchExecNode<ArrowBatch>,
        SingleTransformationTranslator<ArrowBatch>,
        BatchColumnarExecNode {
  private final int[] keys;

  BatchHashExchangeExecNode(
      ReadableConfig config, RowType outputType, String description, int[] keys) {
    super(
        ExecNodeContext.newNodeId(),
        new ExecNodeContext("batch-exec-native-hash-exchange_1"),
        config,
        List.of(InputProperty.builder().damBehavior(InputProperty.DamBehavior.BLOCKING).build()),
        outputType,
        description);
    this.keys = keys.clone();
  }

  @Override
  @SuppressWarnings("unchecked")
  protected Transformation<ArrowBatch> translateToPlanInternal(
      PlannerBase planner, ExecNodeConfig config) {
    Transformation<ArrowBatch> input =
        (Transformation<ArrowBatch>) getInputEdges().get(0).translateToPlan(planner);
    int channels = Math.max(1, planner.getExecEnv().getParallelism());
    int maximum = FlinkKeyGroupUtils.defaultMaxParallelism(channels);
    var split =
        org.apache.flink.table.planner.plan.nodes.exec.utils.ExecNodeUtil
            .createOneInputTransformation(
                input,
                createTransformationMeta("native-batch-hash-split", config),
                new tech.streamfusion.operator.SplitByKeyGroupOperator(
                    keys,
                    FlinkKeyGroupUtils.timestampPrecisions((RowType) getOutputType(), keys),
                    maximum,
                    false,
                    new int[0],
                    true,
                    true),
                tech.streamfusion.operator.ArrowBatchTypeInformation.INSTANCE,
                input.getParallelism(),
                false);
    var exchange =
        new PartitionTransformation<>(
            split, new ColumnarKeyGroupPartitioner(maximum, false), StreamExchangeMode.BATCH);
    exchange.setParallelism(channels);
    exchange.setMaxParallelism(maximum);
    return exchange;
  }
}
