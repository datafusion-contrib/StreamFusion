package tech.streamfusion.planner.stream;

import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory$;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.utils.ShortcutUtils;
import tech.streamfusion.operator.WatermarkExpression;
import tech.streamfusion.planner.ColumnarInput;
import tech.streamfusion.planner.ColumnarOutput;

/**
 * Physical node standing in for a {@link
 * org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalWatermarkAssigner} the
 * native columnar assigner runs. It forwards each Arrow batch unchanged and emits bounded
 * out-of-orderness watermarks from the rowtime column, so a columnar source can feed a columnar
 * window without transposing to rows just to assign watermarks. Keeps the assigner's output type
 * (which carries the rowtime time attribute); it is the watermark source, so it needs no upstream
 * watermark.
 */
public class StreamPhysicalNativeWatermarkAssigner extends StreamPhysicalNativeSingleRel
    implements ColumnarInput, ColumnarOutput {

  private final int rowtimeColumn;
  private final WatermarkExpression expression;
  private final boolean preserveBoundaries;

  public StreamPhysicalNativeWatermarkAssigner(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelNode input,
      RelDataType outputRowType,
      int rowtimeColumn,
      WatermarkExpression expression) {
    this(cluster, traitSet, input, outputRowType, rowtimeColumn, expression, true);
  }

  private StreamPhysicalNativeWatermarkAssigner(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelNode input,
      RelDataType outputRowType,
      int rowtimeColumn,
      WatermarkExpression expression,
      boolean preserveBoundaries) {
    super(cluster, traitSet, input, outputRowType);
    this.rowtimeColumn = rowtimeColumn;
    this.expression = expression;
    this.preserveBoundaries = preserveBoundaries;
  }

  StreamPhysicalNativeWatermarkAssigner wholeBatches() {
    return new StreamPhysicalNativeWatermarkAssigner(
        getCluster(), getTraitSet(), getInput(), outputRowType, rowtimeColumn, expression, false);
  }

  @Override
  public RelWriter explainTerms(RelWriter writer) {
    return super.explainTerms(writer).itemIf("watermarkGranularity", "batch", !preserveBoundaries);
  }

  @Override
  public boolean requireWatermark() {
    return false;
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new StreamPhysicalNativeWatermarkAssigner(
        getCluster(),
        traitSet,
        inputs.get(0),
        outputRowType,
        rowtimeColumn,
        expression,
        preserveBoundaries);
  }

  @Override
  public ExecNode<?> translateToExecNode() {
    return new NativeWatermarkAssignerExecNode(
        ShortcutUtils.unwrapTableConfig(this),
        InputProperty.DEFAULT,
        FlinkTypeFactory$.MODULE$.toLogicalRowType(getRowType()),
        getRelDetailedDescription(),
        rowtimeColumn,
        expression,
        preserveBoundaries);
  }
}
