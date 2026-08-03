package tech.streamfusion.planner;

import tech.streamfusion.operator.RowDataArrowConverter;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory$;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalRank;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalRel;
import org.apache.flink.table.planner.plan.utils.ChangelogPlanUtils;
import org.apache.flink.table.planner.plan.utils.RankProcessStrategy;
import org.apache.flink.table.runtime.operators.rank.ConstantRankRange;
import org.apache.flink.table.runtime.operators.rank.RankType;

/**
 * Recognizes the streaming Top-N the native ranker implements:
 * {@code ROW_NUMBER() OVER (PARTITION BY … ORDER BY …) BETWEEN rankStart AND rankEnd}, with or
 * without the rank number projected. Requires {@code ROW_NUMBER} (Flink rejects streaming
 * RANK/DENSE_RANK), a constant rank range, and input/output column types the row/Arrow conversion
 * supports. The caller picks the ranker: the append-only one for an insert-only, no-offset query, or
 * the retracting one (full buffer, rank window {@code [offset+1, rankEnd]}) for a changelog input or
 * an {@code OFFSET} (rank start > 1).
 */
final class TopNMatcher {

  private TopNMatcher() {}

  static boolean matches(StreamPhysicalRank rank) {
    return unsupportedReason(rank) == null;
  }

  /** The specific reason this rank is not accelerable, or null if it is. */
  static String unsupportedReason(StreamPhysicalRank rank) {
    if (rank.rankType() != RankType.ROW_NUMBER) {
      return "Top-N: only ROW_NUMBER ranks (RANK/DENSE_RANK fall back)";
    }
    if (!(rank.rankRange() instanceof ConstantRankRange)) {
      return "Top-N: only a constant rank range";
    }
    if (DeduplicateMatcher.isTimeOrder(rank)) {
      // A time-ordered rank is deduplication (DeduplicateMatcher), not a value Top-N.
      return "Top-N: a time-ordered rank is deduplication, not a value Top-N";
    }
    // The whole row crosses the boundary unchanged, so every column (incl. partition/order keys)
    // must be a type the conversion handles.
    if (!RowDataArrowConverter.supports(
        FlinkTypeFactory$.MODULE$.toLogicalRowType(rank.getRowType()))) {
      return "Top-N: a column type the boundary cannot carry";
    }
    return null;
  }

  static int[] partitionColumns(StreamPhysicalRank rank) {
    return rank.partitionKey().toArray();
  }

  /** The rank window upper bound (rankEnd): the operator emits ranks {@code [offset+1, limit]}. */
  static long limit(StreamPhysicalRank rank) {
    return ((ConstantRankRange) rank.rankRange()).getRankEnd();
  }

  /** The 0-based offset (rankStart - 1); > 0 for an {@code OFFSET} (range not starting at rank 1). */
  static long offset(StreamPhysicalRank rank) {
    return ((ConstantRankRange) rank.rankRange()).getRankStart() - 1;
  }

  static boolean outputRankNumber(StreamPhysicalRank rank) {
    return rank.outputRankNumber();
  }

  static int[] sortIndices(StreamPhysicalRank rank) {
    return rank.orderKey().getFieldCollations().stream()
        .mapToInt(RelFieldCollation::getFieldIndex)
        .toArray();
  }

  static int[] sortAscending(StreamPhysicalRank rank) {
    return rank.orderKey().getFieldCollations().stream()
        .mapToInt(fc -> fc.getDirection().isDescending() ? 0 : 1)
        .toArray();
  }

  static int[] sortNullsFirst(StreamPhysicalRank rank) {
    return rank.orderKey().getFieldCollations().stream()
        .mapToInt(fc -> nullsFirst(fc) ? 1 : 0)
        .toArray();
  }

  /** Whether nulls sort first for this column, resolving the unspecified case from the direction. */
  private static boolean nullsFirst(RelFieldCollation fc) {
    RelFieldCollation.NullDirection effective =
        fc.nullDirection == RelFieldCollation.NullDirection.UNSPECIFIED
            ? fc.getDirection().defaultNullDirection()
            : fc.nullDirection;
    return effective == RelFieldCollation.NullDirection.FIRST;
  }

  static RelNode substitute(StreamPhysicalRank rank, PlanContext ctx) {
    // An update-fast rank (unique-keyed input with a monotonic sort key) receives a changelog
    // WITHOUT retractions — the upstream is planned to emit only +I/+U, and rank rows are
    // replaced by their unique key (the retracting ranker's full-row retraction model would
    // accumulate every version). It routes to the update-fast ranker, which mirrors Flink's
    // UpdatableTopNFunction/FastTop1Function state shape.
    if (rank.rankStrategy() instanceof RankProcessStrategy.UpdateFastStrategy) {
      if (TopNMatcher.offset(rank) > 0) {
        ctx.decline("Top-N: update-fast rank with OFFSET runs on the host");
        return null;
      }
      int[] updateFastPartitions = TopNMatcher.partitionColumns(rank);
      return new StreamPhysicalNativeColumnarTopN(
          rank.getCluster(),
          rank.getTraitSet(),
          ctx.columnarInput(rank.getInput(), updateFastPartitions),
          rank.getRowType(),
          updateFastPartitions,
          TopNMatcher.sortIndices(rank),
          TopNMatcher.sortAscending(rank),
          TopNMatcher.sortNullsFirst(rank),
          0,
          TopNMatcher.limit(rank),
          TopNMatcher.outputRankNumber(rank),
          false,
          ((RankProcessStrategy.UpdateFastStrategy) rank.rankStrategy()).getPrimaryKeys());
    }
    int[] partitionColumns = TopNMatcher.partitionColumns(rank);
    long offset = TopNMatcher.offset(rank);
    // A changelog input or an OFFSET routes to the retracting ranker (full buffer + rank window);
    // the append-only bounded ranker handles only the insert-only, no-offset case.
    boolean retracting =
        offset > 0 || !ChangelogPlanUtils.isInsertOnly((StreamPhysicalRel) rank.getInput());
    // Columnar (Arrow in/out); keep the partitioned shuffle columnar where the input sits on a
    // columnar producer, else the transition pass transposes at the boundary.
    return new StreamPhysicalNativeColumnarTopN(
        rank.getCluster(),
        rank.getTraitSet(),
        ctx.columnarInput(rank.getInput(), partitionColumns),
        rank.getRowType(),
        partitionColumns,
        TopNMatcher.sortIndices(rank),
        TopNMatcher.sortAscending(rank),
        TopNMatcher.sortNullsFirst(rank),
        offset,
        TopNMatcher.limit(rank),
        TopNMatcher.outputRankNumber(rank),
        retracting,
        null);
  }
}
