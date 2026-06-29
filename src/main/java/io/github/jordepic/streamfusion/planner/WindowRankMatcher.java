package io.github.jordepic.streamfusion.planner;

import io.github.jordepic.streamfusion.operator.RowDataArrowConverter;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory$;
import org.apache.flink.table.planner.plan.logical.WindowAttachedWindowingStrategy;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalWindowRank;
import org.apache.flink.table.runtime.operators.rank.ConstantRankRange;
import org.apache.flink.table.runtime.operators.rank.RankType;

/**
 * Recognizes window Top-N — {@code ROW_NUMBER() OVER (PARTITION BY window[, key] ORDER BY …) <= N}
 * over a windowing-TVF input, planned as a {@link StreamPhysicalWindowRank}. The window is attached
 * as {@code window_start}/{@code window_end} columns (a {@link WindowAttachedWindowingStrategy}); the
 * native ranker keeps, per window and partition key, the top N rows by the order key and emits them
 * when a watermark closes the window. Requires {@code ROW_NUMBER}, a constant range starting at 1,
 * and input columns the conversion supports; anything else (RANK/DENSE_RANK, an offset) falls back.
 */
final class WindowRankMatcher {

  private WindowRankMatcher() {}

  static boolean matches(StreamPhysicalWindowRank rank) {
    if (rank.rankType() != RankType.ROW_NUMBER) {
      return false;
    }
    if (!(rank.rankRange() instanceof ConstantRankRange)) {
      return false;
    }
    if (((ConstantRankRange) rank.rankRange()).getRankStart() != 1) {
      return false; // an offset (rank start > 1) is not supported
    }
    if (!(rank.windowing() instanceof WindowAttachedWindowingStrategy)) {
      return false; // the window must be attached as columns (the windowing-TVF output)
    }
    return RowDataArrowConverter.supports(
        FlinkTypeFactory$.MODULE$.toLogicalRowType(rank.getRowType()));
  }

  private static WindowAttachedWindowingStrategy windowing(StreamPhysicalWindowRank rank) {
    return (WindowAttachedWindowingStrategy) rank.windowing();
  }

  static int windowStartColumn(StreamPhysicalWindowRank rank) {
    return windowing(rank).getWindowStart();
  }

  static int windowEndColumn(StreamPhysicalWindowRank rank) {
    return windowing(rank).getWindowEnd();
  }

  static int[] partitionColumns(StreamPhysicalWindowRank rank) {
    return rank.partitionKey().toArray();
  }

  static long limit(StreamPhysicalWindowRank rank) {
    return ((ConstantRankRange) rank.rankRange()).getRankEnd();
  }

  static boolean outputRankNumber(StreamPhysicalWindowRank rank) {
    return rank.outputRankNumber();
  }

  static boolean isProctime(StreamPhysicalWindowRank rank) {
    return windowing(rank).isProctime();
  }

  static long windowMillis(StreamPhysicalWindowRank rank) {
    return WindowAggregateMatcher.windowSize(windowing(rank));
  }

  static long slideMillis(StreamPhysicalWindowRank rank) {
    return WindowAggregateMatcher.windowSlide(windowing(rank));
  }

  static boolean cumulative(StreamPhysicalWindowRank rank) {
    return WindowAggregateMatcher.isCumulative(windowing(rank));
  }

  static int[] sortIndices(StreamPhysicalWindowRank rank) {
    return rank.orderKey().getFieldCollations().stream()
        .mapToInt(RelFieldCollation::getFieldIndex)
        .toArray();
  }

  static int[] sortAscending(StreamPhysicalWindowRank rank) {
    return rank.orderKey().getFieldCollations().stream()
        .mapToInt(fc -> fc.getDirection().isDescending() ? 0 : 1)
        .toArray();
  }

  static int[] sortNullsFirst(StreamPhysicalWindowRank rank) {
    return rank.orderKey().getFieldCollations().stream()
        .mapToInt(fc -> nullsFirst(fc) ? 1 : 0)
        .toArray();
  }

  private static boolean nullsFirst(RelFieldCollation fc) {
    RelFieldCollation.NullDirection effective =
        fc.nullDirection == RelFieldCollation.NullDirection.UNSPECIFIED
            ? fc.getDirection().defaultNullDirection()
            : fc.nullDirection;
    return effective == RelFieldCollation.NullDirection.FIRST;
  }

  static String unsupportedReason(StreamPhysicalWindowRank rank) {
    return "window Top-N: needs ROW_NUMBER() OVER (PARTITION BY window[, key] ORDER BY …) <= N over a"
        + " windowing-TVF input, with input columns the Arrow conversion supports";
  }
}
