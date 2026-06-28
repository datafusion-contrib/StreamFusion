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
 * Physical node standing in for the INNER {@code UNNEST} of an array (Flink's {@link
 * org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalCorrelate} over {@code
 * $UNNEST_ROWS$}). Columnar in and out ({@link ColumnarInput} and {@link ColumnarOutput}): it fans
 * each input row out to one row per element of its array column, appending the element, and carries
 * the {@code $row_kind$} tag through (changelog-transparent), so it sits in the columnar island
 * between its (native) input and whatever consumes the unnested rows.
 */
public class StreamPhysicalNativeUnnest extends SingleRel
    implements StreamPhysicalRel, ColumnarInput, ColumnarOutput {

  private final RelDataType outputRowType;
  private final int arrayColumn;
  private final boolean withOrdinality;
  private final boolean isLeft;

  public StreamPhysicalNativeUnnest(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelNode input,
      RelDataType outputRowType,
      int arrayColumn,
      boolean withOrdinality,
      boolean isLeft) {
    super(cluster, traitSet, input);
    this.outputRowType = outputRowType;
    this.arrayColumn = arrayColumn;
    this.withOrdinality = withOrdinality;
    this.isLeft = isLeft;
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
    return new StreamPhysicalNativeUnnest(
        getCluster(), traitSet, inputs.get(0), outputRowType, arrayColumn, withOrdinality, isLeft);
  }

  @Override
  public ExecNode<?> translateToExecNode() {
    return new NativeUnnestExecNode(
        ShortcutUtils.unwrapTableConfig(this),
        InputProperty.DEFAULT,
        FlinkTypeFactory$.MODULE$.toLogicalRowType(getRowType()),
        getRelDetailedDescription(),
        arrayColumn,
        withOrdinality,
        isLeft);
  }
}
