package tech.streamfusion.planner.batch;

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
import tech.streamfusion.planner.NativeConfig;

final class BatchAggregateExecNode extends ExecNodeBase<ArrowBatch>
    implements BatchExecNode<ArrowBatch>,
        SingleTransformationTranslator<ArrowBatch>,
        BatchColumnarExecNode {
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
    Transformation<ArrowBatch> aggregate =
        ExecNodeUtil.createOneInputTransformation(
            input,
            createTransformationMeta(
                "native-batch-aggregate", getDescription(), getDescription(), config),
            new NativeBatchAggregateOperator(
                inputType,
                (RowType) getOutputType(),
                specification.keys,
                specification.kinds,
                specification.columns,
                specification.filters,
                specification.merge,
                specification.finalOutput,
                NativeConfig.boundedBatchRows(config)),
            ArrowBatchTypeInformation.INSTANCE,
            input.getParallelism(),
            false);
    if (!specification.sortGroups) return aggregate;
    // Like Comet's SortAggregate adapter, preserve the host's ordered group output after hashing.
    int[] columns = java.util.stream.IntStream.range(0, specification.keys.length).toArray();
    int[] flags = new int[columns.length];
    java.util.Arrays.fill(flags, 2);
    return ExecNodeUtil.createOneInputTransformation(
        aggregate,
        createTransformationMeta(
            "native-batch-group-order", getDescription(), getDescription(), config),
        new tech.streamfusion.operator.NativeBatchSortOperator(
            columns, flags, NativeConfig.boundedBatchRows(config)),
        ArrowBatchTypeInformation.INSTANCE,
        aggregate.getParallelism(),
        false);
  }
}
