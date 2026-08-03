package tech.streamfusion.planner;

import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.utils.ShortcutUtils;

/** Columnar Kafka sink whose native boundary serializes batches before Flink publishes them. */
public final class StreamPhysicalNativeKafkaSink extends StreamPhysicalNativeSingleRel
    implements ColumnarInput {

  private final KafkaSinkMatcher.Planned planned;

  StreamPhysicalNativeKafkaSink(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelNode input,
      RelDataType outputRowType,
      KafkaSinkMatcher.Planned planned) {
    super(cluster, traitSet, input, outputRowType);
    this.planned = planned;
  }

  @Override
  public boolean requireWatermark() {
    return false;
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new StreamPhysicalNativeKafkaSink(
        getCluster(), traitSet, inputs.get(0), outputRowType, planned);
  }

  @Override
  public ExecNode<?> translateToExecNode() {
    return new NativeKafkaSinkExecNode(
        ShortcutUtils.unwrapTableConfig(this),
        InputProperty.DEFAULT,
        planned.rowType,
        getRelDetailedDescription(),
        planned);
  }
}
