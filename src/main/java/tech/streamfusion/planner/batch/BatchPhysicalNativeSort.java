package tech.streamfusion.planner.batch;

import java.util.List;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Sort;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory$;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.utils.SortUtil;
import org.apache.flink.table.planner.utils.ShortcutUtils;
import tech.streamfusion.planner.ColumnarInput;
import tech.streamfusion.planner.ColumnarOutput;
import tech.streamfusion.planner.common.NativePlanDetails;

final class BatchPhysicalNativeSort extends BatchPhysicalNativeSingleRel
    implements ColumnarInput, ColumnarOutput {
  private final int[] columns;
  private final int[] flags;
  private final long offset;
  private final long end;

  BatchPhysicalNativeSort(Sort original, RelNode input, int[] columns, int[] flags) {
    this(
        original,
        input,
        columns,
        flags,
        SortUtil.getLimitStart(original.offset),
        original.fetch == null ? -1 : SortUtil.getLimitEnd(original.offset, original.fetch),
        NativePlanDetails.from(original));
  }

  private BatchPhysicalNativeSort(
      RelNode original,
      RelNode input,
      int[] columns,
      int[] flags,
      long offset,
      long end,
      NativePlanDetails details) {
    super(original, input, details);
    this.columns = columns;
    this.flags = flags;
    this.offset = offset;
    this.end = end;
  }

  @Override
  public RelNode copy(RelTraitSet traits, List<RelNode> inputs) {
    var copy =
        new BatchPhysicalNativeSort(this, inputs.get(0), columns, flags, offset, end, details);
    copy.traitSet = traits;
    return copy;
  }

  @Override
  public ExecNode<?> translateToExecNode() {
    return new BatchSortExecNode(
        ShortcutUtils.unwrapTableConfig(this),
        FlinkTypeFactory$.MODULE$.toLogicalRowType(getRowType()),
        getRelDetailedDescription(),
        columns,
        flags,
        offset,
        end,
        inputProperty(
            org.apache.flink.table.planner.plan.nodes.exec.InputProperty.DamBehavior.END_INPUT));
  }
}
