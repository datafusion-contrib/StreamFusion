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
 * Columnar form of the append-only streaming Top-N: Arrow batches in and out ({@link ColumnarInput}
 * and {@link ColumnarOutput}), substituted when the ranker's partitioned input is kept columnar
 * across the exchange. The emitted changelog carries its kind on the batch's {@code $row_kind$} column.
 */
public class StreamPhysicalNativeColumnarTopN extends SingleRel
    implements StreamPhysicalRel, ColumnarInput, ColumnarOutput {

  private final RelDataType outputRowType;
  private final int[] partitionColumns;
  private final int[] sortIndices;
  private final int[] sortAscending;
  private final int[] sortNullsFirst;
  private final long limit;
  private final boolean outputRankNumber;

  public StreamPhysicalNativeColumnarTopN(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelNode input,
      RelDataType outputRowType,
      int[] partitionColumns,
      int[] sortIndices,
      int[] sortAscending,
      int[] sortNullsFirst,
      long limit,
      boolean outputRankNumber) {
    super(cluster, traitSet, input);
    this.outputRowType = outputRowType;
    this.partitionColumns = partitionColumns;
    this.sortIndices = sortIndices;
    this.sortAscending = sortAscending;
    this.sortNullsFirst = sortNullsFirst;
    this.limit = limit;
    this.outputRankNumber = outputRankNumber;
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
    return new StreamPhysicalNativeColumnarTopN(
        getCluster(),
        traitSet,
        inputs.get(0),
        outputRowType,
        partitionColumns,
        sortIndices,
        sortAscending,
        sortNullsFirst,
        limit,
        outputRankNumber);
  }

  @Override
  public ExecNode<?> translateToExecNode() {
    return new NativeColumnarTopNExecNode(
        ShortcutUtils.unwrapTableConfig(this),
        InputProperty.DEFAULT,
        FlinkTypeFactory$.MODULE$.toLogicalRowType(getRowType()),
        getRelDetailedDescription(),
        partitionColumns,
        sortIndices,
        sortAscending,
        sortNullsFirst,
        limit,
        outputRankNumber);
  }
}
