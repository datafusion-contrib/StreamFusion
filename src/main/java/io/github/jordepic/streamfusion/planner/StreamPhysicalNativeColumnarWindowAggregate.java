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
 * The columnar twin of {@link StreamPhysicalNativeWindowAggregate}: the same single-phase event-time
 * window aggregate, but consuming Arrow batches ({@link ColumnarInput}) from a columnar exchange so
 * no row transpose sits at the window's input. It still emits {@link
 * org.apache.flink.table.data.RowData} (the window results), so it is not {@link ColumnarOutput} — a
 * row consumer downstream needs no transpose either.
 */
public class StreamPhysicalNativeColumnarWindowAggregate extends SingleRel
    implements StreamPhysicalRel, ColumnarInput {

  private final RelDataType outputRowType;
  private final boolean cumulative;
  private final long windowMillis;
  private final long slideMillis;
  private final int timeColumn;
  private final int valueColumn;
  private final int[] keyColumns;
  private final int valueType;
  private final int[] aggregateKinds;

  public StreamPhysicalNativeColumnarWindowAggregate(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelNode input,
      RelDataType outputRowType,
      boolean cumulative,
      long windowMillis,
      long slideMillis,
      int timeColumn,
      int valueColumn,
      int[] keyColumns,
      int valueType,
      int[] aggregateKinds) {
    super(cluster, traitSet, input);
    this.outputRowType = outputRowType;
    this.cumulative = cumulative;
    this.windowMillis = windowMillis;
    this.slideMillis = slideMillis;
    this.timeColumn = timeColumn;
    this.valueColumn = valueColumn;
    this.keyColumns = keyColumns;
    this.valueType = valueType;
    this.aggregateKinds = aggregateKinds;
  }

  @Override
  public boolean requireWatermark() {
    return true;
  }

  @Override
  protected RelDataType deriveRowType() {
    return outputRowType;
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new StreamPhysicalNativeColumnarWindowAggregate(
        getCluster(),
        traitSet,
        inputs.get(0),
        outputRowType,
        cumulative,
        windowMillis,
        slideMillis,
        timeColumn,
        valueColumn,
        keyColumns,
        valueType,
        aggregateKinds);
  }

  @Override
  public ExecNode<?> translateToExecNode() {
    return new NativeColumnarWindowAggExecNode(
        ShortcutUtils.unwrapTableConfig(this),
        InputProperty.DEFAULT,
        FlinkTypeFactory$.MODULE$.toLogicalRowType(getRowType()),
        getRelDetailedDescription(),
        cumulative,
        windowMillis,
        slideMillis,
        timeColumn,
        valueColumn,
        keyColumns,
        valueType,
        aggregateKinds);
  }
}
