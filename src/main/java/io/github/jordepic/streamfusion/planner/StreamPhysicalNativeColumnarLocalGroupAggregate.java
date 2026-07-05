package io.github.jordepic.streamfusion.planner;

import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory$;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalRel;
import org.apache.flink.table.planner.utils.ShortcutUtils;

/**
 * Columnar form of the local half of a two-phase non-windowed {@code GROUP BY}: Arrow batches in and
 * out ({@link ColumnarInput} and {@link ColumnarOutput}), so the pre-aggregate, the keyed shuffle,
 * and the global merge flow Arrow with no transpose between them. Insert-only (it emits append-only
 * partials), so it requires no {@code $row_kind$} column on its output.
 */
public class StreamPhysicalNativeColumnarLocalGroupAggregate extends SingleRel
    implements StreamPhysicalRel, ColumnarInput, ColumnarOutput {

  private final RelDataType outputRowType;
  private final int[] aggregateKinds;
  private final int[] valueTypes;
  private final int[] valueColumns;
  private final int[] keyColumns;
  private final int[] distinctViewSources;

  public StreamPhysicalNativeColumnarLocalGroupAggregate(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelNode input,
      RelDataType outputRowType,
      int[] aggregateKinds,
      int[] valueTypes,
      int[] valueColumns,
      int[] keyColumns,
      int[] distinctViewSources) {
    super(cluster, traitSet, input);
    this.outputRowType = outputRowType;
    this.aggregateKinds = aggregateKinds;
    this.valueTypes = valueTypes;
    this.valueColumns = valueColumns;
    this.keyColumns = keyColumns;
    this.distinctViewSources = distinctViewSources;
  }

  @Override
  public boolean requireWatermark() {
    return false;
  }

  @Override
  protected RelDataType deriveRowType() {
    return outputRowType;
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new StreamPhysicalNativeColumnarLocalGroupAggregate(
        getCluster(),
        traitSet,
        inputs.get(0),
        outputRowType,
        aggregateKinds,
        valueTypes,
        valueColumns,
        keyColumns,
        distinctViewSources);
  }

  @Override
  public ExecNode<?> translateToExecNode() {
    return new NativeColumnarLocalGroupAggExecNode(
        ShortcutUtils.unwrapTableConfig(this),
        InputProperty.DEFAULT,
        FlinkTypeFactory$.MODULE$.toLogicalRowType(getRowType()),
        getRelDetailedDescription(),
        aggregateKinds,
        valueTypes,
        valueColumns,
        keyColumns,
        distinctViewSources);
  }

  /** Digest-only reuse barrier — see {@link NativeRelDigests}. */
  private final long reuseBarrier = NativeRelDigests.nextId();

  @Override
  public RelWriter explainTerms(RelWriter pw) {
    return NativeRelDigests.withBarrier(super.explainTerms(pw), reuseBarrier);
  }
}

