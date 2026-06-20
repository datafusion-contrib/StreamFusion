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
 * Transpose entering a columnar region: a rowwise input becomes Arrow batches. Inserted by the
 * transition pass where a columnar operator consumes from a rowwise one. The logical row type is
 * unchanged — only the physical carrier becomes columnar.
 */
public class StreamPhysicalRowDataToArrow extends SingleRel
    implements StreamPhysicalRel, ColumnarOutput {

  public StreamPhysicalRowDataToArrow(RelOptCluster cluster, RelTraitSet traitSet, RelNode input) {
    super(cluster, traitSet, input);
  }

  @Override
  public boolean requireWatermark() {
    return false;
  }

  @Override
  protected RelDataType deriveRowType() {
    return getInput().getRowType();
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new StreamPhysicalRowDataToArrow(getCluster(), traitSet, inputs.get(0));
  }

  @Override
  public ExecNode<?> translateToExecNode() {
    return new RowDataToArrowExecNode(
        ShortcutUtils.unwrapTableConfig(this),
        InputProperty.DEFAULT,
        FlinkTypeFactory$.MODULE$.toLogicalRowType(getRowType()),
        getRelDetailedDescription());
  }
}
