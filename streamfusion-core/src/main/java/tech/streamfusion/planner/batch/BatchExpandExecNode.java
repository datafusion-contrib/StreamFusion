package tech.streamfusion.planner.batch;

import java.util.List;
import java.util.stream.Collectors;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.*;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.utils.ExecNodeUtil;
import org.apache.flink.table.types.logical.RowType;
import tech.streamfusion.operator.ArrowBatch;
import tech.streamfusion.operator.ArrowBatchTypeInformation;
import tech.streamfusion.operator.NativeExpandOperator;
import tech.streamfusion.planner.common.EncodedExpression;

final class BatchExpandExecNode extends ExecNodeBase<ArrowBatch>
    implements BatchExecNode<ArrowBatch>,
        SingleTransformationTranslator<ArrowBatch>,
        BatchColumnarExecNode {
  private final List<EncodedExpression> projections;

  BatchExpandExecNode(
      ReadableConfig config,
      RowType outputType,
      String description,
      InputProperty property,
      List<EncodedExpression> projections) {
    super(
        ExecNodeContext.newNodeId(),
        new ExecNodeContext("batch-exec-native-expand_1"),
        config,
        List.of(property),
        outputType,
        description);
    this.projections = projections;
  }

  @Override
  @SuppressWarnings("unchecked")
  protected Transformation<ArrowBatch> translateToPlanInternal(
      PlannerBase planner, ExecNodeConfig config) {
    var input = (Transformation<ArrowBatch>) getInputEdges().get(0).translateToPlan(planner);
    var plans =
        projections.stream()
            .map(
                p ->
                    new NativeExpandOperator.Projection(
                        p.kinds(),
                        p.payload(),
                        p.childCounts(),
                        p.projectionRoots(),
                        p.longs(),
                        p.doubles(),
                        p.strings(),
                        p.outputNames()))
            .collect(Collectors.toList());
    return ExecNodeUtil.createOneInputTransformation(
        input,
        createTransformationMeta("native-batch-expand", getDescription(), getDescription(), config),
        new NativeExpandOperator(plans),
        ArrowBatchTypeInformation.INSTANCE,
        input.getParallelism(),
        false);
  }
}
