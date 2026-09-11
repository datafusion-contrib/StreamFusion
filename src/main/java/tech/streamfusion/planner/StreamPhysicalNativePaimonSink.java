package tech.streamfusion.planner;

import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.utils.ShortcutUtils;

/** Columnar physical Paimon sink; Paimon retains its manifests, compaction, and commits. */
public final class StreamPhysicalNativePaimonSink extends StreamPhysicalNativeSingleRel
    implements ColumnarInput {
  private final PaimonSinkMatcher.Planned planned;

  StreamPhysicalNativePaimonSink(
      RelOptCluster cluster,
      RelTraitSet traits,
      RelNode input,
      RelDataType outputType,
      PaimonSinkMatcher.Planned planned) {
    super(cluster, traits, input, outputType);
    this.planned = planned;
  }

  @Override
  public boolean requireWatermark() {
    return false;
  }

  @Override
  public RelNode copy(RelTraitSet traits, List<RelNode> inputs) {
    return new StreamPhysicalNativePaimonSink(
        getCluster(), traits, inputs.get(0), outputRowType, planned);
  }

  @Override
  public ExecNode<?> translateToExecNode() {
    return new NativePaimonSinkExecNode(
        ShortcutUtils.unwrapTableConfig(this),
        InputProperty.DEFAULT,
        FlinkTypeFactory.toLogicalRowType(getRowType()),
        getRelDetailedDescription(),
        planned);
  }
}
