package io.github.jordepic.streamfusion.planner;

import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalRel;
import org.apache.flink.table.planner.utils.ShortcutUtils;

/** Columnar Kafka sink whose native boundary serializes batches before Flink publishes them. */
public final class StreamPhysicalNativeKafkaSink extends SingleRel
    implements StreamPhysicalRel, ColumnarInput {

  private final RelDataType outputRowType;
  private final KafkaSinkMatcher.Planned planned;

  StreamPhysicalNativeKafkaSink(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelNode input,
      RelDataType outputRowType,
      KafkaSinkMatcher.Planned planned) {
    super(cluster, traitSet, input);
    this.outputRowType = outputRowType;
    this.planned = planned;
  }

  @Override
  public boolean requireWatermark() {
    return false;
  }

  @Override
  protected RelDataType deriveRowType() {
    return outputRowType;
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

  private final long reuseBarrier = NativeRelDigests.nextId();

  @Override
  public RelWriter explainTerms(RelWriter pw) {
    return NativeRelDigests.withBarrier(super.explainTerms(pw), reuseBarrier);
  }
}
