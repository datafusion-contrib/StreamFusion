package tech.streamfusion.planner.batch;

import java.util.List;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Calc;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.utils.ShortcutUtils;
import tech.streamfusion.planner.ColumnarInput;
import tech.streamfusion.planner.ColumnarOutput;
import tech.streamfusion.planner.common.CommonCalcExecNode;
import tech.streamfusion.planner.common.CalcSpec;
import tech.streamfusion.planner.common.NativePlanDetails;

final class BatchPhysicalNativeCalc extends BatchPhysicalNativeSingleRel
    implements ColumnarInput, ColumnarOutput {
  private final CalcSpec specification;

  BatchPhysicalNativeCalc(Calc original, RelNode input, CalcSpec specification) {
    this(original, input, specification,
        NativePlanDetails.from(original).withFunctions(specification.expression()));
  }

  private BatchPhysicalNativeCalc(
      RelNode original, RelNode input, CalcSpec specification, NativePlanDetails details) {
    super(original, input, details);
    this.specification = specification;
  }

  @Override
  public RelNode copy(RelTraitSet traits, List<RelNode> inputs) {
    var copy = new BatchPhysicalNativeCalc(this, inputs.get(0), specification, details);
    copy.traitSet = traits;
    return copy;
  }

  @Override
  public ExecNode<?> translateToExecNode() {
    return new BatchNativeExecNode<>(
        ShortcutUtils.unwrapTableConfig(this),
        new CommonCalcExecNode(
            ShortcutUtils.unwrapTableConfig(this),
            inputProperty(InputProperty.DamBehavior.PIPELINED),
            getRelDetailedDescription(), specification));
  }
}
