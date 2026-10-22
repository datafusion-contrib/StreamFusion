package tech.streamfusion.planner;

import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalRel;
import org.apache.flink.table.planner.utils.ShortcutUtils;
import tech.streamfusion.doris.DorisSourceRuntime;

public final class StreamPhysicalNativeDorisSource extends AbstractRelNode
    implements StreamPhysicalRel, ColumnarOutput {
  private final RelDataType type;
  private final DorisSourceRuntime source;
  private final long barrier = NativeRelDigests.nextId();

  StreamPhysicalNativeDorisSource(
      RelOptCluster cluster, RelTraitSet traits, RelDataType type, DorisSourceRuntime source) {
    super(cluster, traits);
    this.type = type;
    this.source = source;
  }

  @Override
  public boolean requireWatermark() {
    return false;
  }

  @Override
  protected RelDataType deriveRowType() {
    return type;
  }

  @Override
  public RelNode copy(RelTraitSet traits, List<RelNode> inputs) {
    return new StreamPhysicalNativeDorisSource(getCluster(), traits, type, source);
  }

  @Override
  public org.apache.calcite.rel.RelWriter explainTerms(org.apache.calcite.rel.RelWriter writer) {
    return NativeRelDigests.withBarrier(super.explainTerms(writer), barrier);
  }

  @Override
  public ExecNode<?> translateToExecNode() {
    return new NativeDorisSourceExecNode(
        ShortcutUtils.unwrapTableConfig(this), type, getRelDetailedDescription(), source);
  }
}
