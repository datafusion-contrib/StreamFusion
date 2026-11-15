package tech.streamfusion.planner.batch;

import java.util.List;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.transformations.PartitionTransformation;
import org.apache.flink.streaming.api.transformations.StreamExchangeMode;
import org.apache.flink.streaming.runtime.partitioner.GlobalPartitioner;
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
final class BatchSingletonExchangeExecNode extends ExecNodeBase<ArrowBatch>
    implements BatchExecNode<ArrowBatch>,
        SingleTransformationTranslator<ArrowBatch>,
        BatchColumnarExecNode {
  BatchSingletonExchangeExecNode(ReadableConfig config, RowType outputType, String description) {
    super(
        ExecNodeContext.newNodeId(),
        new ExecNodeContext("batch-exec-native-singleton-exchange_1"),
        config,
        List.of(InputProperty.builder().damBehavior(InputProperty.DamBehavior.BLOCKING).build()),
        outputType,
        description);
  }

  @Override
  @SuppressWarnings("unchecked")
  protected Transformation<ArrowBatch> translateToPlanInternal(
      PlannerBase planner, ExecNodeConfig config) {
    Transformation<ArrowBatch> input =
        (Transformation<ArrowBatch>) getInputEdges().get(0).translateToPlan(planner);
    var exchange =
        new PartitionTransformation<>(
            input, new GlobalPartitioner<ArrowBatch>(), StreamExchangeMode.BATCH);
    exchange.setParallelism(1);
    exchange.setMaxParallelism(1);
    return exchange;
  }
}
