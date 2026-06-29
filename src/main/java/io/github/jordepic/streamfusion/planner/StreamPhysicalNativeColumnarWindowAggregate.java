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
 * window aggregate, consuming Arrow batches ({@link ColumnarInput}) from a columnar exchange and
 * emitting the window-result batches as Arrow ({@link ColumnarOutput}). Every native operator but a
 * source/sink is Arrow → Arrow; the planner inserts the {@code ArrowToRowData} transpose
 * before a rowwise sink at the island perimeter.
 */
public class StreamPhysicalNativeColumnarWindowAggregate extends SingleRel
    implements StreamPhysicalRel, ColumnarInput, ColumnarOutput {

  private final RelDataType outputRowType;
  private final boolean cumulative;
  private final long windowMillis;
  private final long slideMillis;
  private final int timeColumn;
  private final int[] valueColumns;
  private final int[] keyColumns;
  private final int[] valueTypes;
  private final int[] aggregateKinds;
  private final boolean proctime;
  private final boolean timestampLtz;

  public StreamPhysicalNativeColumnarWindowAggregate(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelNode input,
      RelDataType outputRowType,
      boolean cumulative,
      long windowMillis,
      long slideMillis,
      int timeColumn,
      int[] valueColumns,
      int[] keyColumns,
      int[] valueTypes,
      int[] aggregateKinds,
      boolean proctime,
      boolean timestampLtz) {
    super(cluster, traitSet, input);
    this.outputRowType = outputRowType;
    this.cumulative = cumulative;
    this.windowMillis = windowMillis;
    this.slideMillis = slideMillis;
    this.timeColumn = timeColumn;
    this.valueColumns = valueColumns;
    this.keyColumns = keyColumns;
    this.valueTypes = valueTypes;
    this.aggregateKinds = aggregateKinds;
    this.proctime = proctime;
    this.timestampLtz = timestampLtz;
  }

  @Override
  public boolean requireWatermark() {
    return !proctime; // proctime windows fire on a processing-time timer, not a watermark
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
        valueColumns,
        keyColumns,
        valueTypes,
        aggregateKinds,
        proctime,
        timestampLtz);
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
        valueColumns,
        keyColumns,
        valueTypes,
        aggregateKinds,
        proctime,
        timestampLtz);
  }
}
