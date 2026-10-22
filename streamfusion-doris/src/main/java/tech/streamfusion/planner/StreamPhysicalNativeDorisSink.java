package tech.streamfusion.planner;

import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.utils.ShortcutUtils;

public final class StreamPhysicalNativeDorisSink extends StreamPhysicalNativeSingleRel
    implements ColumnarInput {
  private final DorisSinkRuntime runtime;

  StreamPhysicalNativeDorisSink(
      RelOptCluster cluster,
      RelTraitSet traits,
      RelNode input,
      RelDataType output,
      DorisSinkRuntime runtime) {
    super(cluster, traits, input, output);
    this.runtime = runtime;
  }

  @Override
  public boolean requireWatermark() {
    return false;
  }

  @Override
  public RelNode copy(RelTraitSet traits, List<RelNode> inputs) {
    return new StreamPhysicalNativeDorisSink(
        getCluster(), traits, inputs.get(0), outputRowType, runtime);
  }

  @Override
  public ExecNode<?> translateToExecNode() {
    return new NativeDorisSinkExecNode(
        ShortcutUtils.unwrapTableConfig(this), getRelDetailedDescription(), runtime);
  }
}
