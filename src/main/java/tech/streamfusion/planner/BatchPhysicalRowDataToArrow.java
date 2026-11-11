package tech.streamfusion.planner;

import java.util.List;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory$;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.utils.ShortcutUtils;
import tech.streamfusion.planner.common.CommonRowDataToArrowExecNode;

final class BatchPhysicalRowDataToArrow extends BatchPhysicalNativeSingleRel
    implements ColumnarOutput {
  BatchPhysicalRowDataToArrow(RelNode input) {
    this(input, input);
  }

  private BatchPhysicalRowDataToArrow(RelNode original, RelNode input) {
    super(original, input, NativePlanDetails.EMPTY);
  }

  @Override
  public RelNode copy(RelTraitSet traits, List<RelNode> inputs) {
    var copy = new BatchPhysicalRowDataToArrow(this, inputs.get(0));
    copy.traitSet = traits;
    return copy;
  }

  @Override
  public ExecNode<?> translateToExecNode() {
    return new BatchNativeExecNode<>(
        ShortcutUtils.unwrapTableConfig(this),
        new CommonRowDataToArrowExecNode(
            ShortcutUtils.unwrapTableConfig(this),
            inputProperty(InputProperty.DamBehavior.PIPELINED),
            FlinkTypeFactory$.MODULE$.toLogicalRowType(getRowType()),
            getRelDetailedDescription(),
            false,
            null,
            false,
            8192));
  }
}
