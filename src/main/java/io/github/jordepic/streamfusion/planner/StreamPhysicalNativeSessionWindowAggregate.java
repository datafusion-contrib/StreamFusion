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
 * Physical plan node standing in for an event-time session-window aggregate the native operator
 * handles. Like the tumbling node it preserves the replaced node's output type and traits and
 * requires a watermark, but it carries the inactivity gap rather than a fixed size and slide.
 */
public class StreamPhysicalNativeSessionWindowAggregate extends SingleRel
    implements StreamPhysicalRel {

  private final RelDataType outputRowType;
  private final long gapMillis;
  private final int timeColumn;
  private final int valueColumn;
  private final int[] keyColumns;
  private final int valueType;
  private final int[] aggregateKinds;

  public StreamPhysicalNativeSessionWindowAggregate(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelNode input,
      RelDataType outputRowType,
      long gapMillis,
      int timeColumn,
      int valueColumn,
      int[] keyColumns,
      int valueType,
      int[] aggregateKinds) {
    super(cluster, traitSet, input);
    this.outputRowType = outputRowType;
    this.gapMillis = gapMillis;
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
    return new StreamPhysicalNativeSessionWindowAggregate(
        getCluster(),
        traitSet,
        inputs.get(0),
        outputRowType,
        gapMillis,
        timeColumn,
        valueColumn,
        keyColumns,
        valueType,
        aggregateKinds);
  }

  @Override
  public ExecNode<?> translateToExecNode() {
    return new NativeSessionWindowAggExecNode(
        ShortcutUtils.unwrapTableConfig(this),
        InputProperty.DEFAULT,
        FlinkTypeFactory$.MODULE$.toLogicalRowType(getRowType()),
        getRelDetailedDescription(),
        gapMillis,
        timeColumn,
        valueColumn,
        keyColumns,
        valueType,
        aggregateKinds);
  }
}
