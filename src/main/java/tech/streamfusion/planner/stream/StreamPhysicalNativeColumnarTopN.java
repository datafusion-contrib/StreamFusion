package tech.streamfusion.planner.stream;

import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory$;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.utils.ShortcutUtils;
import tech.streamfusion.planner.ColumnarInput;
import tech.streamfusion.planner.ColumnarOutput;
import tech.streamfusion.planner.FlinkKeyGroupUtils;

/**
 * Columnar form of the streaming Top-N: Arrow batches in and out ({@link ColumnarInput} and {@link
 * ColumnarOutput}), substituted when the ranker's partitioned input is kept columnar across the
 * exchange. The emitted changelog carries its kind on the batch's {@code $row_kind$} column.
 * {@code retracting} selects the changelog-input ranker (keeps the full buffer to promote on delete)
 * over the append-only one. Non-time ROW_NUMBER uses the append-only state without a bound and
 * selects row-identity updates instead of rank-slot replacements.
 */
public class StreamPhysicalNativeColumnarTopN extends StreamPhysicalNativeSingleRel
    implements ColumnarInput, ColumnarOutput {

  private final int[] partitionColumns;
  private final int[] sortIndices;
  private final int[] sortAscending;
  private final int[] sortNullsFirst;
  private final long offset;
  private final long limit;
  private final int rankEndColumn;
  private final boolean firstBound;
  private final boolean nonTimeRowNumber;
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
      int rankEndColumn,
      boolean outputRankNumber,
      boolean retracting,
      int[] rowKeyColumns,
      boolean generateUpdateBefore) {
    this(
        cluster,
        traitSet,
        input,
        outputRowType,
        partitionColumns,
        sortIndices,
        sortAscending,
        sortNullsFirst,
        offset,
        limit,
        rankEndColumn,
        outputRankNumber,
        retracting,
        rowKeyColumns,
        generateUpdateBefore,
        false);
  }

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
      int rankEndColumn,
      boolean outputRankNumber,
      boolean retracting,
      int[] rowKeyColumns,
      boolean generateUpdateBefore,
      boolean firstBound) {
    this(
        cluster,
        traitSet,
        input,
        outputRowType,
        partitionColumns,
        sortIndices,
        sortAscending,
        sortNullsFirst,
        offset,
        limit,
        rankEndColumn,
        outputRankNumber,
        retracting,
        rowKeyColumns,
        generateUpdateBefore,
        firstBound,
        false);
  }

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
      int rankEndColumn,
      boolean outputRankNumber,
      boolean retracting,
      int[] rowKeyColumns,
      boolean generateUpdateBefore,
      boolean firstBound,
      boolean nonTimeRowNumber) {
    super(cluster, traitSet, input, outputRowType);
    this.partitionColumns = partitionColumns;
    this.sortIndices = sortIndices;
    this.sortAscending = sortAscending;
    this.sortNullsFirst = sortNullsFirst;
    this.offset = offset;
    this.limit = limit;
    this.rankEndColumn = rankEndColumn;
    this.firstBound = firstBound;
    this.nonTimeRowNumber = nonTimeRowNumber;
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
        rankEndColumn,
        outputRankNumber,
        retracting,
        rowKeyColumns,
        generateUpdateBefore,
        firstBound,
        nonTimeRowNumber);
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
        rankEndColumn,
        outputRankNumber,
        retracting,
        rowKeyColumns,
        generateUpdateBefore,
        rowKeyColumns == null
            ? null
            : FlinkKeyGroupUtils.timestampPrecisions(getInput().getRowType(), rowKeyColumns),
        FlinkKeyGroupUtils.timestampPrecisions(getInput().getRowType(), partitionColumns),
        firstBound,
        nonTimeRowNumber);
  }
}
