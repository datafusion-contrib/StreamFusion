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

/** Physical node standing in for the global half of a two-phase tumbling window aggregate. */
public class StreamPhysicalNativeGlobalWindowAggregate extends SingleRel
    implements StreamPhysicalRel {

  private final RelDataType outputRowType;
  private final long windowMillis;
  private final long slideMillis;
  private final boolean cumulative;
  private final int[] keyColumns;
  private final int[] partialColumns;
  private final int sliceEndColumn;
  private final int[] valueTypes;
  private final int[] aggregateKinds;

  public StreamPhysicalNativeGlobalWindowAggregate(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelNode input,
      RelDataType outputRowType,
      long windowMillis,
      long slideMillis,
      boolean cumulative,
      int[] keyColumns,
      int[] partialColumns,
      int sliceEndColumn,
      int[] valueTypes,
      int[] aggregateKinds) {
    super(cluster, traitSet, input);
    this.outputRowType = outputRowType;
    this.windowMillis = windowMillis;
    this.slideMillis = slideMillis;
    this.cumulative = cumulative;
    this.keyColumns = keyColumns;
    this.partialColumns = partialColumns;
    this.sliceEndColumn = sliceEndColumn;
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
    return new StreamPhysicalNativeGlobalWindowAggregate(
        getCluster(),
        traitSet,
        inputs.get(0),
        outputRowType,
        windowMillis,
        slideMillis,
        cumulative,
        keyColumns,
        partialColumns,
        sliceEndColumn,
        valueTypes,
        aggregateKinds);
  }

  @Override
  public ExecNode<?> translateToExecNode() {
    return new NativeGlobalWindowAggExecNode(
        ShortcutUtils.unwrapTableConfig(this),
        InputProperty.DEFAULT,
        FlinkTypeFactory$.MODULE$.toLogicalRowType(getRowType()),
        getRelDetailedDescription(),
        windowMillis,
        slideMillis,
        cumulative,
        keyColumns,
        partialColumns,
        sliceEndColumn,
        valueTypes,
        aggregateKinds);
  }
}
