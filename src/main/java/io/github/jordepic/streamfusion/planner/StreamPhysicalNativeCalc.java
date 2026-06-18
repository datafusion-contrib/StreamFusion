package io.github.jordepic.streamfusion.planner;

import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory$;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalRel;
import org.apache.flink.table.planner.utils.ShortcutUtils;

/**
 * Physical plan node that stands in for a projection the native operator can execute. It carries
 * the same output type and traits as the node it replaces, so the rest of the plan is unaffected,
 * and its only job is to translate to the native execution node.
 */
public class StreamPhysicalNativeCalc extends SingleRel implements StreamPhysicalRel {

  private final RelDataType outputRowType;

  public StreamPhysicalNativeCalc(
      RelOptCluster cluster, RelTraitSet traitSet, RelNode input, RelDataType outputRowType) {
    super(cluster, traitSet, input);
    this.outputRowType = outputRowType;
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
    return new StreamPhysicalNativeCalc(getCluster(), traitSet, inputs.get(0), outputRowType);
  }

  @Override
  public ExecNode<?> translateToExecNode() {
    return new NativeCalcExecNode(
        ShortcutUtils.unwrapTableConfig(this),
        InputProperty.DEFAULT,
        FlinkTypeFactory$.MODULE$.toLogicalRowType(getRowType()),
        getRelDetailedDescription());
  }
}
