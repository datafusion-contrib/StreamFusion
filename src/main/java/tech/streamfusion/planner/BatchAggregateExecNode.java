package tech.streamfusion.planner;

import java.util.List;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.SingleTransformationTranslator;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.utils.ExecNodeUtil;
import org.apache.flink.table.types.logical.RowType;
import tech.streamfusion.operator.ArrowBatch;
import tech.streamfusion.operator.ArrowBatchTypeInformation;
import tech.streamfusion.operator.NativeBatchAggregateOperator;

final class BatchAggregateExecNode extends ExecNodeBase<ArrowBatch>
    implements BatchExecNode<ArrowBatch>, SingleTransformationTranslator<ArrowBatch> {
  private final RowType inputType;
  private final BatchAggregateSpec specification;

  BatchAggregateExecNode(
      ReadableConfig config,
      RowType inputType,
      RowType outputType,
      String description,
      BatchAggregateSpec specification,
      InputProperty property) {
    super(
        ExecNodeContext.newNodeId(),
        new ExecNodeContext("batch-exec-native-aggregate_1"),
        config,
        List.of(property),
        outputType,
        description);
    this.inputType = inputType;
    this.specification = specification;
  }

  @Override
  @SuppressWarnings("unchecked")
  protected Transformation<ArrowBatch> translateToPlanInternal(
      PlannerBase planner, ExecNodeConfig config) {
    var input = (Transformation<ArrowBatch>) getInputEdges().get(0).translateToPlan(planner);
    return ExecNodeUtil.createOneInputTransformation(
        input,
        createTransformationMeta(
            "native-batch-aggregate", getDescription(), getDescription(), config),
        new NativeBatchAggregateOperator(
            inputType,
            (RowType) getOutputType(),
            specification.kinds,
            specification.columns,
            specification.filters,
            specification.merge,
            specification.finalOutput,
            NativeConfig.transposeBatchRows(config)),
        ArrowBatchTypeInformation.INSTANCE,
        input.getParallelism(),
        false);
  }
}
