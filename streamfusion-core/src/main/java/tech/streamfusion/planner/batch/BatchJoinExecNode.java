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
import tech.streamfusion.operator.NativeBatchJoinOperator;
import tech.streamfusion.planner.NativeConfig;

final class BatchJoinExecNode extends ExecNodeBase<ArrowBatch>
    implements BatchExecNode<ArrowBatch>,
        SingleTransformationTranslator<ArrowBatch>,
        BatchColumnarExecNode {
  private final RowType leftType;
  private final RowType rightType;
  private final BatchJoinSpec spec;

  BatchJoinExecNode(
      ReadableConfig config,
      RowType left,
      RowType right,
      RowType result,
      String description,
      BatchJoinSpec spec) {
    super(
        ExecNodeContext.newNodeId(),
        new ExecNodeContext("batch-exec-native-hash-join_1"),
        config,
        List.of(property(spec.buildLeft ? 0 : 1), property(spec.buildLeft ? 1 : 0)),
        result,
        description);
    leftType = left;
    rightType = right;
    this.spec = spec;
  }

  private static InputProperty property(int priority) {
    return InputProperty.builder()
        .requiredDistribution(InputProperty.ANY_DISTRIBUTION)
        .damBehavior(InputProperty.DamBehavior.END_INPUT)
        .priority(priority)
        .build();
  }

  @Override
  @SuppressWarnings("unchecked")
  protected Transformation<ArrowBatch> translateToPlanInternal(
      PlannerBase planner, ExecNodeConfig config) {
    var left = (Transformation<ArrowBatch>) getInputEdges().get(0).translateToPlan(planner);
    var right = (Transformation<ArrowBatch>) getInputEdges().get(1).translateToPlan(planner);
    int parallelism =
        spec.broadcast
            ? (spec.buildLeft ? right : left).getParallelism()
            : Math.max(left.getParallelism(), right.getParallelism());
    return ExecNodeUtil.createTwoInputTransformation(
        left,
        right,
        createTransformationMeta(
            "native-batch-hash-join", getDescription(), getDescription(), config),
        new NativeBatchJoinOperator(
            leftType,
            rightType,
            (RowType) getOutputType(),
            spec.leftKeys,
            spec.rightKeys,
            spec.kind,
            spec.buildLeft,
            spec.nullEquals,
            NativeConfig.boundedBatchRows(config),
            spec.nestedLoop,
            spec.predicate,
            spec.mergeJoin),
        ArrowBatchTypeInformation.INSTANCE,
        parallelism,
        false);
  }
}
