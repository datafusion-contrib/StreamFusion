package tech.streamfusion.planner;

import java.util.List;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory$;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.utils.ShortcutUtils;

/**
 * A verified SINGLETON distribution; hash, range, broadcast and hybrid routes are separate work.
 */
final class BatchPhysicalNativeExchange extends BatchPhysicalNativeSingleRel
    implements ColumnarInput, ColumnarOutput {
  BatchPhysicalNativeExchange(RelNode original, RelNode input) {
    this(original, input, NativePlanDetails.from(original));
  }

  private BatchPhysicalNativeExchange(RelNode original, RelNode input, NativePlanDetails details) {
    super(original, input, details);
  }

  @Override
  public RelNode copy(RelTraitSet traits, List<RelNode> inputs) {
    var copy = new BatchPhysicalNativeExchange(this, inputs.get(0), details);
    copy.traitSet = traits;
    return copy;
  }

  @Override
  public ExecNode<?> translateToExecNode() {
    return new BatchSingletonExchangeExecNode(
        ShortcutUtils.unwrapTableConfig(this),
        FlinkTypeFactory$.MODULE$.toLogicalRowType(getRowType()),
        getRelDetailedDescription());
  }
}
