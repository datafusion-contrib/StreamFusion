package tech.streamfusion.planner.batch;

import java.util.List;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory$;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.utils.ShortcutUtils;
import tech.streamfusion.planner.ColumnarInput;
import tech.streamfusion.planner.ColumnarOutput;
import tech.streamfusion.planner.common.EncodedExpression;
import tech.streamfusion.planner.common.NativePlanDetails;

final class BatchPhysicalNativeExpand extends BatchPhysicalNativeSingleRel
    implements ColumnarInput, ColumnarOutput {
  private final List<EncodedExpression> projections;

  BatchPhysicalNativeExpand(RelNode original, RelNode input, List<EncodedExpression> projections) {
    this(original, input, projections, NativePlanDetails.from(original));
  }

  private BatchPhysicalNativeExpand(
      RelNode original,
      RelNode input,
      List<EncodedExpression> projections,
      NativePlanDetails details) {
    super(original, input, details);
    this.projections = projections;
  }

  @Override
  public RelNode copy(RelTraitSet traits, List<RelNode> inputs) {
    var copy = new BatchPhysicalNativeExpand(this, inputs.get(0), projections, details);
    copy.traitSet = traits;
    return copy;
  }

  @Override
  public ExecNode<?> translateToExecNode() {
    return new BatchExpandExecNode(
        ShortcutUtils.unwrapTableConfig(this),
        FlinkTypeFactory$.MODULE$.toLogicalRowType(getRowType()),
        getRelDetailedDescription(),
        inputProperty(InputProperty.DamBehavior.PIPELINED),
        projections);
  }
}
