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
 * The columnar twin of {@link StreamPhysicalNativeGlobalWindowAggregate}: the global merge consuming
 * the partial-state Arrow batches the columnar local half emits ({@link ColumnarInput}) and emitting
 * the final per-window results as Arrow ({@link ColumnarOutput}). Arrow → Arrow like every native
 * operator but a source/sink (ticket 36); the planner inserts the {@code ArrowToRowData} transpose
 * before a rowwise sink at the island perimeter. The partial columns / slice-end position are not
 * needed — the batch already carries the native partial schema, fed straight to the aggregator.
 */
public class StreamPhysicalNativeColumnarGlobalWindowAggregate extends SingleRel
    implements StreamPhysicalRel, ColumnarInput, ColumnarOutput {

  private final RelDataType outputRowType;
  private final long windowMillis;
  private final long slideMillis;
  private final boolean cumulative;
  private final int[] keyColumns;
  private final int[] valueTypes;
  private final int[] aggregateKinds;

  public StreamPhysicalNativeColumnarGlobalWindowAggregate(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelNode input,
      RelDataType outputRowType,
      long windowMillis,
      long slideMillis,
      boolean cumulative,
      int[] keyColumns,
      int[] valueTypes,
      int[] aggregateKinds) {
    super(cluster, traitSet, input);
    this.outputRowType = outputRowType;
    this.windowMillis = windowMillis;
    this.slideMillis = slideMillis;
    this.cumulative = cumulative;
    this.keyColumns = keyColumns;
    this.valueTypes = valueTypes;
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
    return new StreamPhysicalNativeColumnarGlobalWindowAggregate(
        getCluster(),
        traitSet,
        inputs.get(0),
        outputRowType,
        windowMillis,
        slideMillis,
        cumulative,
        keyColumns,
        valueTypes,
        aggregateKinds);
  }

  @Override
  public ExecNode<?> translateToExecNode() {
    return new NativeColumnarGlobalWindowAggExecNode(
        ShortcutUtils.unwrapTableConfig(this),
        InputProperty.DEFAULT,
        FlinkTypeFactory$.MODULE$.toLogicalRowType(getRowType()),
        getRelDetailedDescription(),
        windowMillis,
        slideMillis,
        cumulative,
        keyColumns,
        valueTypes,
        aggregateKinds);
  }
}
