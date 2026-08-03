package tech.streamfusion.planner;

import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory$;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.utils.ShortcutUtils;

/**
 * The columnar twin of {@link StreamPhysicalNativeLocalWindowAggregate}: the local pre-aggregate
 * consuming Arrow batches and emitting partial-state Arrow batches ({@link ColumnarInput} and {@link
 * ColumnarOutput}), so it feeds a columnar exchange and global half with no row transpose.
 */
public class StreamPhysicalNativeColumnarLocalWindowAggregate extends StreamPhysicalNativeSingleRel
    implements ColumnarInput, ColumnarOutput {

  private final long sliceMillis;
  private final int timeColumn;
  // Window-attached mode (q5): window boundaries read from these columns instead of slicing a rowtime;
  // both -1 in the ordinary rowtime mode.
  private final int windowStartColumn;
  private final int windowEndColumn;
  private final int[] valueColumns;
  private final int[] keyColumns;
  private final int[] valueTypes;
  private final int[] aggregateKinds;
  private final boolean timestampLtz;

  public StreamPhysicalNativeColumnarLocalWindowAggregate(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelNode input,
      RelDataType outputRowType,
      long sliceMillis,
      int timeColumn,
      int windowStartColumn,
      int windowEndColumn,
      int[] valueColumns,
      int[] keyColumns,
      int[] valueTypes,
      int[] aggregateKinds,
      boolean timestampLtz) {
    super(cluster, traitSet, input, outputRowType);
    this.sliceMillis = sliceMillis;
    this.timeColumn = timeColumn;
    this.windowStartColumn = windowStartColumn;
    this.windowEndColumn = windowEndColumn;
    this.valueColumns = valueColumns;
    this.keyColumns = keyColumns;
    this.valueTypes = valueTypes;
    this.aggregateKinds = aggregateKinds;
    this.timestampLtz = timestampLtz;
  }

  @Override
  public boolean requireWatermark() {
    return true;
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
        windowStartColumn,
        windowEndColumn,
        valueColumns,
        keyColumns,
        valueTypes,
        aggregateKinds,
        timestampLtz);
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
        windowStartColumn,
        windowEndColumn,
        valueColumns,
        keyColumns,
        valueTypes,
        aggregateKinds,
        timestampLtz,
        FlinkKeyGroupUtils.timestampPrecisions(getInput().getRowType(), keyColumns));
  }
}
