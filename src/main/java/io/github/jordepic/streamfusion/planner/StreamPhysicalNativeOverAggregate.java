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
 * Physical node standing in for an event-time {@code OVER} aggregate (RANGE unbounded preceding,
 * optional PARTITION BY) the native operator runs. Columnar: it consumes and produces Arrow batches
 * ({@link ColumnarInput} and {@link ColumnarOutput}) — the input columns pass through with the
 * running aggregate(s) appended — so it rides the columnar shuffle with no transpose. Requires a
 * watermark, since each row is emitted once the watermark passes its rowtime.
 */
public class StreamPhysicalNativeOverAggregate extends SingleRel
    implements StreamPhysicalRel, ColumnarInput, ColumnarOutput {

  private final RelDataType outputRowType;
  private final int timeColumn;
  private final int[] valueColumns;
  private final int[] keyColumns;
  private final int[] valueTypes;
  private final int[] aggregateKinds;
  private final int frameKind;
  private final long frameOffset;

  public StreamPhysicalNativeOverAggregate(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelNode input,
      RelDataType outputRowType,
      int timeColumn,
      int[] valueColumns,
      int[] keyColumns,
      int[] valueTypes,
      int[] aggregateKinds,
      int frameKind,
      long frameOffset) {
    super(cluster, traitSet, input);
    this.outputRowType = outputRowType;
    this.timeColumn = timeColumn;
    this.valueColumns = valueColumns;
    this.keyColumns = keyColumns;
    this.valueTypes = valueTypes;
    this.aggregateKinds = aggregateKinds;
    this.frameKind = frameKind;
    this.frameOffset = frameOffset;
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
    return new StreamPhysicalNativeOverAggregate(
        getCluster(), traitSet, inputs.get(0), outputRowType, timeColumn, valueColumns, keyColumns,
        valueTypes, aggregateKinds, frameKind, frameOffset);
  }

  @Override
  public ExecNode<?> translateToExecNode() {
    return new NativeOverAggExecNode(
        ShortcutUtils.unwrapTableConfig(this),
        InputProperty.DEFAULT,
        FlinkTypeFactory$.MODULE$.toLogicalRowType(getRowType()),
        getRelDetailedDescription(),
        timeColumn,
        valueColumns,
        keyColumns,
        valueTypes,
        aggregateKinds,
        frameKind,
        frameOffset);
  }
}
