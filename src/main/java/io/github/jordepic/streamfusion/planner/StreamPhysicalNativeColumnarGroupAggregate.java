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
 * Columnar form of the non-windowed {@code GROUP BY} aggregate: Arrow batches in and out ({@link
 * ColumnarInput} and {@link ColumnarOutput}), so it flows Arrow between native operators with no
 * transpose — substituted when the aggregate's keyed input is kept columnar across the exchange. The
 * emitted changelog carries its kind on the batch's {@code $row_kind$} column.
 */
public class StreamPhysicalNativeColumnarGroupAggregate extends SingleRel
    implements StreamPhysicalRel, ColumnarInput, ColumnarOutput {

  private final RelDataType outputRowType;
  private final int[] aggregateKinds;
  private final int[] valueTypes;
  private final int[] valueColumns;
  private final int[] keyColumns;
  private final int[] filterColumns;
  private final boolean generateUpdateBefore;

  public StreamPhysicalNativeColumnarGroupAggregate(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelNode input,
      RelDataType outputRowType,
      int[] aggregateKinds,
      int[] valueTypes,
      int[] valueColumns,
      int[] keyColumns,
      int[] filterColumns,
      boolean generateUpdateBefore) {
    super(cluster, traitSet, input);
    this.outputRowType = outputRowType;
    this.aggregateKinds = aggregateKinds;
    this.valueTypes = valueTypes;
    this.valueColumns = valueColumns;
    this.keyColumns = keyColumns;
    this.filterColumns = filterColumns;
    this.generateUpdateBefore = generateUpdateBefore;
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
    return new StreamPhysicalNativeColumnarGroupAggregate(
        getCluster(),
        traitSet,
        inputs.get(0),
        outputRowType,
        aggregateKinds,
        valueTypes,
        valueColumns,
        keyColumns,
        filterColumns,
        generateUpdateBefore);
  }

  @Override
  public ExecNode<?> translateToExecNode() {
    return new NativeColumnarGroupAggExecNode(
        ShortcutUtils.unwrapTableConfig(this),
        InputProperty.DEFAULT,
        FlinkTypeFactory$.MODULE$.toLogicalRowType(getRowType()),
        getRelDetailedDescription(),
        aggregateKinds,
        valueTypes,
        valueColumns,
        keyColumns,
        filterColumns,
        generateUpdateBefore);
  }
}
