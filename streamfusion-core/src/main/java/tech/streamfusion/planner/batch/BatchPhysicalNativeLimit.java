package tech.streamfusion.planner.batch;

import java.util.List;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Sort;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory$;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.utils.SortUtil;
import org.apache.flink.table.planner.utils.ShortcutUtils;
import tech.streamfusion.planner.ColumnarInput;
import tech.streamfusion.planner.ColumnarOutput;
import tech.streamfusion.planner.common.NativePlanDetails;

final class BatchPhysicalNativeLimit extends BatchPhysicalNativeSingleRel
    implements ColumnarInput, ColumnarOutput {
  private final long offset, end;

  BatchPhysicalNativeLimit(Sort original, RelNode input) {
    this(
        original,
        input,
        SortUtil.getLimitStart(original.offset),
        SortUtil.getLimitEnd(original.offset, original.fetch),
        NativePlanDetails.from(original));
  }

  private BatchPhysicalNativeLimit(
      RelNode original, RelNode input, long offset, long end, NativePlanDetails details) {
    super(original, input, details);
    this.offset = offset;
    this.end = end;
  }

  @Override
  public RelNode copy(RelTraitSet traits, List<RelNode> inputs) {
    var copy = new BatchPhysicalNativeLimit(this, inputs.get(0), offset, end, details);
    copy.traitSet = traits;
    return copy;
  }

  @Override
  public ExecNode<?> translateToExecNode() {
    return new BatchLimitExecNode(
        ShortcutUtils.unwrapTableConfig(this),
        FlinkTypeFactory$.MODULE$.toLogicalRowType(getRowType()),
        getRelDetailedDescription(),
        inputProperty(InputProperty.DamBehavior.PIPELINED),
        offset,
        end);
  }
}
