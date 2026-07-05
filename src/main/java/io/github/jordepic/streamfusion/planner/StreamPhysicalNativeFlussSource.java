package io.github.jordepic.streamfusion.planner;

import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory$;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalRel;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalTableSourceScan;
import org.apache.flink.table.planner.utils.ShortcutUtils;

/** Leaf physical node for a native Fluss log-table source that emits Arrow batches directly. */
public class StreamPhysicalNativeFlussSource extends AbstractRelNode
    implements StreamPhysicalRel, ColumnarOutput {

  private final StreamPhysicalTableSourceScan scan;
  private final RelDataType outputRowType;

  public StreamPhysicalNativeFlussSource(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelDataType outputRowType,
      StreamPhysicalTableSourceScan scan) {
    super(cluster, traitSet);
    this.outputRowType = outputRowType;
    this.scan = scan;
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
    return new StreamPhysicalNativeFlussSource(getCluster(), traitSet, outputRowType, scan);
  }

  @Override
  public RelWriter explainTerms(RelWriter writer) {
    return super.explainTerms(writer).item("connector", "fluss");
  }

  @Override
  public ExecNode<?> translateToExecNode() {
    return new NativeFlussSourceExecNode(
        ShortcutUtils.unwrapTableConfig(this),
        FlinkTypeFactory$.MODULE$.toLogicalRowType(outputRowType),
        getRelDetailedDescription(),
        FlussTables.build(scan));
  }
}
