package tech.streamfusion.planner.batch;

import java.util.List;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.*;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.utils.ExecNodeUtil;
import org.apache.flink.table.types.logical.RowType;
import tech.streamfusion.operator.ArrowBatch;
import tech.streamfusion.operator.ArrowBatchTypeInformation;
import tech.streamfusion.operator.NativeBatchLimitOperator;

final class BatchLimitExecNode extends ExecNodeBase<ArrowBatch>
    implements BatchExecNode<ArrowBatch>,
        SingleTransformationTranslator<ArrowBatch>,
        BatchColumnarExecNode {
  private final long offset, end;

  BatchLimitExecNode(
      ReadableConfig config,
      RowType outputType,
      String description,
      InputProperty property,
      long offset,
      long end) {
    super(
        ExecNodeContext.newNodeId(),
        new ExecNodeContext("batch-exec-native-limit_1"),
        config,
        List.of(property),
        outputType,
        description);
    this.offset = offset;
    this.end = end;
  }

  @Override
  @SuppressWarnings("unchecked")
  protected Transformation<ArrowBatch> translateToPlanInternal(
      PlannerBase planner, ExecNodeConfig config) {
    var input = (Transformation<ArrowBatch>) getInputEdges().get(0).translateToPlan(planner);
    return ExecNodeUtil.createOneInputTransformation(
        input,
        createTransformationMeta("native-batch-limit", getDescription(), getDescription(), config),
        new NativeBatchLimitOperator(offset, end),
        ArrowBatchTypeInformation.INSTANCE,
        input.getParallelism(),
        false);
  }
}
