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
 * Columnar form of the streaming Top-N: Arrow batches in and out ({@link ColumnarInput} and {@link
 * ColumnarOutput}), substituted when the ranker's partitioned input is kept columnar across the
 * exchange. The emitted changelog carries its kind on the batch's {@code $row_kind$} column.
 * {@code retracting} selects the changelog-input ranker (keeps the full buffer to promote on delete)
 * over the append-only one.
 */
public class StreamPhysicalNativeColumnarTopN extends StreamPhysicalNativeSingleRel
    implements ColumnarInput, ColumnarOutput {

  private final int[] partitionColumns;
  private final int[] sortIndices;
  private final int[] sortAscending;
  private final int[] sortNullsFirst;
  private final long offset;
  private final long limit;
  private final boolean outputRankNumber;
  private final boolean retracting;
  // Update-fast mode: the unique-key columns identifying the row a record replaces (null otherwise).
  private final int[] rowKeyColumns;
  private final boolean generateUpdateBefore;

  public StreamPhysicalNativeColumnarTopN(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelNode input,
      RelDataType outputRowType,
      int[] partitionColumns,
      int[] sortIndices,
      int[] sortAscending,
      int[] sortNullsFirst,
      long offset,
      long limit,
      boolean outputRankNumber,
      boolean retracting,
      int[] rowKeyColumns,
      boolean generateUpdateBefore) {
    super(cluster, traitSet, input, outputRowType);
    this.partitionColumns = partitionColumns;
    this.sortIndices = sortIndices;
    this.sortAscending = sortAscending;
    this.sortNullsFirst = sortNullsFirst;
    this.offset = offset;
    this.limit = limit;
    this.outputRankNumber = outputRankNumber;
    this.retracting = retracting;
    this.rowKeyColumns = rowKeyColumns;
    this.generateUpdateBefore = generateUpdateBefore;
  }

  @Override
  public boolean requireWatermark() {
    return false;
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
        offset,
        limit,
        outputRankNumber,
        retracting,
        rowKeyColumns,
        generateUpdateBefore);
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
        offset,
        limit,
        outputRankNumber,
        retracting,
        rowKeyColumns,
        generateUpdateBefore,
        rowKeyColumns == null
            ? null
            : FlinkKeyGroupUtils.timestampPrecisions(getInput().getRowType(), rowKeyColumns),
        FlinkKeyGroupUtils.timestampPrecisions(getInput().getRowType(), partitionColumns));
  }
}
