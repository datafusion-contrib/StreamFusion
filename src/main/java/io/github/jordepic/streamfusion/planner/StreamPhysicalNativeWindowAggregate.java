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
  private final long windowMillis;
  private final long slideMillis;
  private final int timeColumn;
  private final int valueColumn;
  private final int keyColumn;
  private final int[] aggregateKinds;

  public StreamPhysicalNativeWindowAggregate(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelNode input,
      RelDataType outputRowType,
      long windowMillis,
      long slideMillis,
      int timeColumn,
      int valueColumn,
      int keyColumn,
      int[] aggregateKinds) {
    super(cluster, traitSet, input);
    this.outputRowType = outputRowType;
    this.windowMillis = windowMillis;
    this.slideMillis = slideMillis;
    this.timeColumn = timeColumn;
    this.valueColumn = valueColumn;
    this.keyColumn = keyColumn;
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
        windowMillis,
        slideMillis,
        timeColumn,
        valueColumn,
        keyColumn,
        aggregateKinds);
  }

  @Override
  public ExecNode<?> translateToExecNode() {
    return new NativeWindowAggExecNode(
        ShortcutUtils.unwrapTableConfig(this),
        InputProperty.DEFAULT,
        FlinkTypeFactory$.MODULE$.toLogicalRowType(getRowType()),
        getRelDetailedDescription(),
        windowMillis,
        slideMillis,
        timeColumn,
        valueColumn,
        keyColumn,
        aggregateKinds);
  }
}
