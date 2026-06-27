package io.github.jordepic.streamfusion.planner;

import io.github.jordepic.streamfusion.operator.RowDataArrowConverter;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory$;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalRank;
import org.apache.flink.table.runtime.operators.rank.ConstantRankRange;
import org.apache.flink.table.runtime.operators.rank.RankType;

/**
 * Recognizes the append-only streaming Top-N the native ranker implements:
 * {@code ROW_NUMBER() OVER (PARTITION BY … ORDER BY …) <= N} with the rank number not projected.
 * Requires {@code ROW_NUMBER} (not RANK/DENSE_RANK), a constant rank range starting at 1 (no offset),
 * no rank-number output column, and input/output column types the row/Arrow conversion supports. The
 * caller additionally requires an insert-only input — only the append-only Top-N is implemented.
 * Anything else (rank-number output, an offset, RANK/DENSE_RANK, a retracting input) falls back.
 */
final class TopNMatcher {

  private TopNMatcher() {}

  static boolean matches(StreamPhysicalRank rank) {
    if (rank.rankType() != RankType.ROW_NUMBER) {
      return false;
    }
    if (rank.outputRankNumber()) {
      return false; // the rank column is not emitted by the no-row-number path
    }
    if (!(rank.rankRange() instanceof ConstantRankRange)) {
      return false;
    }
    ConstantRankRange range = (ConstantRankRange) rank.rankRange();
    if (range.getRankStart() != 1) {
      return false; // an offset (rank start > 1) is not supported
    }
    if (DeduplicateMatcher.isRowtimeOrder(rank)) {
      return false; // a rowtime-ordered rank is deduplication (DeduplicateMatcher), not a value Top-N
    }
    // The whole row crosses the boundary unchanged, so every column (incl. partition/order keys)
    // must be a type the conversion handles.
    return RowDataArrowConverter.supports(
        FlinkTypeFactory$.MODULE$.toLogicalRowType(rank.getRowType()));
  }

  static int[] partitionColumns(StreamPhysicalRank rank) {
    return rank.partitionKey().toArray();
  }

  static long limit(StreamPhysicalRank rank) {
    return ((ConstantRankRange) rank.rankRange()).getRankEnd();
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
}
