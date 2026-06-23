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
 * Physical plan node standing in for an event-time tumbling-window sum the native operator handles.
 * It preserves the replaced node's output type and traits, and requires a watermark since the
 * window only fires on event time, so the surrounding plan and its watermark wiring are unaffected.
 */
public class StreamPhysicalNativeWindowAggregate extends SingleRel implements StreamPhysicalRel {

  private final RelDataType outputRowType;
  private final boolean cumulative;
  private final long windowMillis;
  private final long slideMillis;
  private final int timeColumn;
  private final int[] valueColumns;
  private final int[] keyColumns;
  private final int[] valueTypes;
  private final int[] aggregateKinds;

  public StreamPhysicalNativeWindowAggregate(
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
      int[] aggregateKinds) {
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
    return new StreamPhysicalNativeWindowAggregate(
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
        aggregateKinds);
  }

  @Override
  public ExecNode<?> translateToExecNode() {
    return new NativeWindowAggExecNode(
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
        aggregateKinds);
  }
}
