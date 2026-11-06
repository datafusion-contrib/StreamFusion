package tech.streamfusion.planner;

import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.utils.ShortcutUtils;

final class StreamPhysicalNativeJvmOver extends StreamPhysicalNativeSingleRel
    implements ColumnarInput, ColumnarOutput {
  private final ExecNode<?> released;
  private final boolean rowtime;

  StreamPhysicalNativeJvmOver(
      RelOptCluster cluster,
      RelTraitSet traits,
      RelNode input,
      RelDataType output,
      ExecNode<?> released,
      boolean rowtime) {
    super(cluster, traits, input, output);
    this.released = released;
    this.rowtime = rowtime;
  }

  @Override
  public boolean requireWatermark() {
    return rowtime;
  }

  @Override
  public RelNode copy(RelTraitSet traits, List<RelNode> inputs) {
    return new StreamPhysicalNativeJvmOver(
        getCluster(), traits, inputs.get(0), outputRowType, released, rowtime);
  }

  @Override
  public ExecNode<?> translateToExecNode() {
    return new NativeJvmOverExecNode(
        ShortcutUtils.unwrapTableConfig(this),
        FlinkTypeFactory.toLogicalRowType(getInput().getRowType()),
        FlinkTypeFactory.toLogicalRowType(getRowType()),
        released,
        getRelDetailedDescription());
  }
}
