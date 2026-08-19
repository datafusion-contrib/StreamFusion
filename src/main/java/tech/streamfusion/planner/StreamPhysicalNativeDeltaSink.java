package tech.streamfusion.planner;

import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.utils.ShortcutUtils;

/** Columnar physical Delta sink; Java still owns its changelog and commit protocol. */
public final class StreamPhysicalNativeDeltaSink extends StreamPhysicalNativeSingleRel
    implements ColumnarInput, RequiresRowKind {
  private final DeltaSinkMatcher.Planned planned;

  StreamPhysicalNativeDeltaSink(
      RelOptCluster cluster,
      RelTraitSet traits,
      RelNode input,
      RelDataType outputType,
      DeltaSinkMatcher.Planned planned) {
    super(cluster, traits, input, outputType);
    this.planned = planned;
  }

  @Override public boolean requireWatermark() { return false; }

  @Override
  public boolean requiresRowKind() {
    return "upsert".equalsIgnoreCase(planned.options.getOrDefault("write.mode", "append"));
  }

  @Override
  public RelNode copy(RelTraitSet traits, List<RelNode> inputs) {
    return new StreamPhysicalNativeDeltaSink(
        getCluster(), traits, inputs.get(0), outputRowType, planned);
  }

  @Override
  public ExecNode<?> translateToExecNode() {
    return new NativeDeltaSinkExecNode(
        ShortcutUtils.unwrapTableConfig(this),
        InputProperty.DEFAULT,
        planned.rowType,
        getRelDetailedDescription(),
        planned);
  }
}
