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
 * The columnar twin of {@link StreamPhysicalNativeLocalWindowAggregate}: the local pre-aggregate
 * consuming Arrow batches and emitting partial-state Arrow batches ({@link ColumnarInput} and {@link
 * ColumnarOutput}), so it feeds a columnar exchange and global half with no row transpose.
 */
public class StreamPhysicalNativeColumnarLocalWindowAggregate extends SingleRel
    implements StreamPhysicalRel, ColumnarInput, ColumnarOutput {

  private final RelDataType outputRowType;
  private final long sliceMillis;
  private final int timeColumn;
  private final int[] valueColumns;
  private final int[] keyColumns;
  private final int[] valueTypes;
  private final int[] aggregateKinds;

  public StreamPhysicalNativeColumnarLocalWindowAggregate(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelNode input,
      RelDataType outputRowType,
      long sliceMillis,
      int timeColumn,
      int[] valueColumns,
      int[] keyColumns,
      int[] valueTypes,
      int[] aggregateKinds) {
    super(cluster, traitSet, input);
    this.outputRowType = outputRowType;
    this.sliceMillis = sliceMillis;
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
    return new StreamPhysicalNativeColumnarLocalWindowAggregate(
        getCluster(),
        traitSet,
        inputs.get(0),
        outputRowType,
        sliceMillis,
        timeColumn,
        valueColumns,
        keyColumns,
        valueTypes,
        aggregateKinds);
  }

  @Override
  public ExecNode<?> translateToExecNode() {
    return new NativeColumnarLocalWindowAggExecNode(
        ShortcutUtils.unwrapTableConfig(this),
        InputProperty.DEFAULT,
        FlinkTypeFactory$.MODULE$.toLogicalRowType(getRowType()),
        getRelDetailedDescription(),
        sliceMillis,
        timeColumn,
        valueColumns,
        keyColumns,
        valueTypes,
        aggregateKinds);
  }
}
