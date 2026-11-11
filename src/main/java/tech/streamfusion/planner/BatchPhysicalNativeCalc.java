package tech.streamfusion.planner;

import java.util.List;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Calc;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory$;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.utils.ShortcutUtils;
import tech.streamfusion.planner.common.CommonCalcExecNode;

final class BatchPhysicalNativeCalc extends BatchPhysicalNativeSingleRel
    implements ColumnarInput, ColumnarOutput {
  private final RexExpression expression;

  BatchPhysicalNativeCalc(Calc original, RelNode input, RexExpression expression) {
    this(original, input, expression, NativePlanDetails.from(original).withFunctions(expression));
  }

  private BatchPhysicalNativeCalc(
      RelNode original, RelNode input, RexExpression expression, NativePlanDetails details) {
    super(original, input, details);
    this.expression = expression;
  }

  @Override
  public RelNode copy(RelTraitSet traits, List<RelNode> inputs) {
    var copy = new BatchPhysicalNativeCalc(this, inputs.get(0), expression, details);
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
            FlinkTypeFactory$.MODULE$.toLogicalRowType(getRowType()),
            getRelDetailedDescription(),
            expression.kinds(),
            expression.payload(),
            expression.childCounts(),
            expression.longs(),
            expression.doubles(),
            expression.strings(),
            expression.projectionRoots(),
            expression.conditionRoot(),
            expression.outputNames(),
            expression.udfBinding()));
  }
}
