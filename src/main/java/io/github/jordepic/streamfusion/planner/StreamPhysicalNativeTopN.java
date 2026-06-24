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
 * Physical node standing in for an append-only streaming Top-N the native ranker runs. It preserves
 * the replaced node's output type and traits, including its retracting changelog mode. Row-fed and
 * needs no watermark — the rank fires per record and keeps a bounded per-partition buffer.
 */
public class StreamPhysicalNativeTopN extends SingleRel implements StreamPhysicalRel {

  private final RelDataType outputRowType;
  private final int[] partitionColumns;
  private final int[] sortIndices;
  private final int[] sortAscending;
  private final int[] sortNullsFirst;
  private final long limit;

  public StreamPhysicalNativeTopN(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelNode input,
      RelDataType outputRowType,
      int[] partitionColumns,
      int[] sortIndices,
      int[] sortAscending,
      int[] sortNullsFirst,
      long limit) {
    super(cluster, traitSet, input);
    this.outputRowType = outputRowType;
    this.partitionColumns = partitionColumns;
    this.sortIndices = sortIndices;
    this.sortAscending = sortAscending;
    this.sortNullsFirst = sortNullsFirst;
    this.limit = limit;
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
    return new StreamPhysicalNativeTopN(
        getCluster(),
        traitSet,
        inputs.get(0),
        outputRowType,
        partitionColumns,
        sortIndices,
        sortAscending,
        sortNullsFirst,
        limit);
  }

  @Override
  public ExecNode<?> translateToExecNode() {
    return new NativeTopNExecNode(
        ShortcutUtils.unwrapTableConfig(this),
        InputProperty.DEFAULT,
        FlinkTypeFactory$.MODULE$.toLogicalRowType(getRowType()),
        getRelDetailedDescription(),
        partitionColumns,
        sortIndices,
        sortAscending,
        sortNullsFirst,
        limit);
  }
}
