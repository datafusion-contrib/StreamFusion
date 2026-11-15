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
import tech.streamfusion.planner.common.NativePlanDetails;

final class BatchPhysicalNativeWindow extends BatchPhysicalNativeSingleRel
    implements ColumnarInput, ColumnarOutput {
  private final BatchWindowSpec spec;

  BatchPhysicalNativeWindow(RelNode original, RelNode input, BatchWindowSpec spec) {
    this(original, input, spec, NativePlanDetails.from(original));
  }

  private BatchPhysicalNativeWindow(
      RelNode original, RelNode input, BatchWindowSpec spec, NativePlanDetails details) {
    super(original, input, details);
    this.spec = spec;
  }

  @Override
  public RelNode copy(RelTraitSet traits, List<RelNode> inputs) {
    var copy = new BatchPhysicalNativeWindow(this, inputs.get(0), spec, details);
    copy.traitSet = traits;
    return copy;
  }

  @Override
  public ExecNode<?> translateToExecNode() {
    return new BatchWindowExecNode(
        ShortcutUtils.unwrapTableConfig(this),
        FlinkTypeFactory$.MODULE$.toLogicalRowType(getInput().getRowType()),
        FlinkTypeFactory$.MODULE$.toLogicalRowType(getRowType()),
        getRelDetailedDescription(),
        inputProperty(InputProperty.DamBehavior.END_INPUT),
        spec);
  }
}
