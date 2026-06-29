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
 * The columnar twin of {@link StreamPhysicalNativeSessionWindowAggregate}: the same session-window
 * aggregate, consuming Arrow batches ({@link ColumnarInput}) from a columnar exchange and emitting the
 * window-result batches as Arrow ({@link ColumnarOutput}). Arrow → Arrow like every native operator but
 * a source/sink; the planner inserts the {@code ArrowToRowData} transpose before a rowwise
 * sink at the island perimeter.
 */
public class StreamPhysicalNativeColumnarSessionWindowAggregate extends SingleRel
    implements StreamPhysicalRel, ColumnarInput, ColumnarOutput {

  private final RelDataType outputRowType;
  private final long gapMillis;
  private final int timeColumn;
  private final int[] valueColumns;
  private final int[] keyColumns;
  private final int[] valueTypes;
  private final int[] aggregateKinds;
  private final boolean proctime;
  private final boolean timestampLtz;

  public StreamPhysicalNativeColumnarSessionWindowAggregate(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelNode input,
      RelDataType outputRowType,
      long gapMillis,
      int timeColumn,
      int[] valueColumns,
      int[] keyColumns,
      int[] valueTypes,
      int[] aggregateKinds,
      boolean proctime,
      boolean timestampLtz) {
    super(cluster, traitSet, input);
    this.outputRowType = outputRowType;
    this.gapMillis = gapMillis;
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
    return !proctime;
  }

  @Override
  protected RelDataType deriveRowType() {
    return outputRowType;
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new StreamPhysicalNativeColumnarSessionWindowAggregate(
        getCluster(),
        traitSet,
        inputs.get(0),
        outputRowType,
        gapMillis,
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
    return new NativeColumnarSessionWindowAggExecNode(
        ShortcutUtils.unwrapTableConfig(this),
        InputProperty.DEFAULT,
        FlinkTypeFactory$.MODULE$.toLogicalRowType(getRowType()),
        getRelDetailedDescription(),
        gapMillis,
        timeColumn,
        valueColumns,
        keyColumns,
        valueTypes,
        aggregateKinds,
        proctime,
        timestampLtz);
  }
}
