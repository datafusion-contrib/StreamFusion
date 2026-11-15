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
import tech.streamfusion.operator.NativeBatchWindowOperator;
import tech.streamfusion.planner.NativeConfig;

final class BatchWindowExecNode extends ExecNodeBase<ArrowBatch>
    implements BatchExecNode<ArrowBatch>,
        SingleTransformationTranslator<ArrowBatch>,
        BatchColumnarExecNode {
  private final RowType inputType;
  private final BatchWindowSpec spec;

  BatchWindowExecNode(
      ReadableConfig config,
      RowType inputType,
      RowType outputType,
      String description,
      InputProperty property,
      BatchWindowSpec spec) {
    super(
        ExecNodeContext.newNodeId(),
        new ExecNodeContext("batch-exec-native-window_1"),
        config,
        List.of(property),
        outputType,
        description);
    this.inputType = inputType;
    this.spec = spec;
  }

  @Override
  @SuppressWarnings("unchecked")
  protected Transformation<ArrowBatch> translateToPlanInternal(
      PlannerBase planner, ExecNodeConfig config) {
    var input = (Transformation<ArrowBatch>) getInputEdges().get(0).translateToPlan(planner);
    return ExecNodeUtil.createOneInputTransformation(
        input,
        createTransformationMeta("native-batch-window", getDescription(), getDescription(), config),
        new NativeBatchWindowOperator(
            inputType,
            (RowType) getOutputType(),
            spec.partition,
            spec.order,
            spec.flags,
            spec.kinds,
            spec.columns,
            spec.rows,
            spec.whole,
            spec.rankStart,
            spec.rankEnd,
            spec.outputRank,
            NativeConfig.boundedBatchRows(config)),
        ArrowBatchTypeInformation.INSTANCE,
        input.getParallelism(),
        false);
  }
}
