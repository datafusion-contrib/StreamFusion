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
 * Physical node standing in for a {@link
 * org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalWatermarkAssigner} the
 * native columnar assigner runs. It forwards each Arrow batch unchanged and emits bounded
 * out-of-orderness watermarks from the rowtime column, so a columnar source can feed a columnar
 * window without transposing to rows just to assign watermarks. Keeps the assigner's output type
 * (which carries the rowtime time attribute); it is the watermark source, so it needs no upstream
 * watermark.
 */
public class StreamPhysicalNativeWatermarkAssigner extends SingleRel
    implements StreamPhysicalRel, ColumnarInput, ColumnarOutput {

  private final RelDataType outputRowType;
  private final int rowtimeColumn;
  private final long delayMillis;

  public StreamPhysicalNativeWatermarkAssigner(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelNode input,
      RelDataType outputRowType,
      int rowtimeColumn,
      long delayMillis) {
    super(cluster, traitSet, input);
    this.outputRowType = outputRowType;
    this.rowtimeColumn = rowtimeColumn;
    this.delayMillis = delayMillis;
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
    return new StreamPhysicalNativeWatermarkAssigner(
        getCluster(), traitSet, inputs.get(0), outputRowType, rowtimeColumn, delayMillis);
  }

  @Override
  public ExecNode<?> translateToExecNode() {
    return new NativeWatermarkAssignerExecNode(
        ShortcutUtils.unwrapTableConfig(this),
        InputProperty.DEFAULT,
        FlinkTypeFactory$.MODULE$.toLogicalRowType(getRowType()),
        getRelDetailedDescription(),
        rowtimeColumn,
        delayMillis);
  }
}
