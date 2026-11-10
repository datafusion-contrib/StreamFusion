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
import tech.streamfusion.operator.NativeBatchSortOperator;

final class BatchSortExecNode extends ExecNodeBase<ArrowBatch>
    implements BatchExecNode<ArrowBatch>, SingleTransformationTranslator<ArrowBatch>, BatchColumnarExecNode {
  private final int[] columns;
  private final int[] flags;
  private final long offset;
  private final long end;

  BatchSortExecNode(
      ReadableConfig config,
      RowType outputType,
      String description,
      int[] columns,
      int[] flags,
      long offset,
      long end,
      InputProperty inputProperty) {
    super(
        ExecNodeContext.newNodeId(),
        new ExecNodeContext("batch-exec-native-sort_1"),
        config,
        List.of(inputProperty),
        outputType,
        description);
    this.columns = columns;
    this.flags = flags;
    this.offset = offset;
    this.end = end;
  }

  @Override
  @SuppressWarnings("unchecked")
  protected Transformation<ArrowBatch> translateToPlanInternal(
      PlannerBase planner, ExecNodeConfig config) {
    Transformation<ArrowBatch> input =
        (Transformation<ArrowBatch>) getInputEdges().get(0).translateToPlan(planner);
    return ExecNodeUtil.createOneInputTransformation(
        input,
        createTransformationMeta("native-batch-sort", getDescription(), getDescription(), config),
        new NativeBatchSortOperator(
            columns, flags, NativeConfig.boundedBatchRows(config), offset, end),
        ArrowBatchTypeInformation.INSTANCE,
        input.getParallelism(),
        false);
  }
}
