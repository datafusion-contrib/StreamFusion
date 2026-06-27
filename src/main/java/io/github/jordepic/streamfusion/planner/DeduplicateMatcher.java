package io.github.jordepic.streamfusion.planner;

import io.github.jordepic.streamfusion.operator.RowDataArrowConverter;
import java.util.List;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory$;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalRank;
import org.apache.flink.table.runtime.operators.rank.ConstantRankRange;
import org.apache.flink.table.runtime.operators.rank.RankType;

/**
 * Recognizes append-only keep-first deduplication: {@code ROW_NUMBER() OVER (PARTITION BY … ORDER BY
 * rowtime ASC) = 1}. The host plans this as a {@link StreamPhysicalRank} whose sole order key is an
 * event-time attribute ascending and whose range is exactly rank 1 — distinct from a value-ordered
 * Top-N (handled by {@link TopNMatcher}). Such a rank is Flink's insert-only {@code
 * RowTimeDeduplicateKeepFirstRowFunction}: per key, emit the minimum-rowtime row once, when the
 * watermark reaches it. Keep-last (descending) is retracting and falls back; a non-time order key is
 * a Top-N, not deduplication.
 */
final class DeduplicateMatcher {

  private DeduplicateMatcher() {}

  static boolean matches(StreamPhysicalRank rank) {
    if (rank.rankType() != RankType.ROW_NUMBER || rank.outputRankNumber()) {
      return false;
    }
    if (!(rank.rankRange() instanceof ConstantRankRange)) {
      return false;
    }
    ConstantRankRange range = (ConstantRankRange) rank.rankRange();
    if (range.getRankStart() != 1 || range.getRankEnd() != 1) {
      return false; // exactly the top row per key
    }
    List<RelFieldCollation> collations = rank.orderKey().getFieldCollations();
    if (collations.size() != 1 || collations.get(0).getDirection().isDescending()) {
      return false; // keep-first is a single ascending order key (keep-last is retracting → host)
    }
    if (!isRowtimeOrder(rank)) {
      return false; // a non-time order key is a value Top-N, not deduplication
    }
    return RowDataArrowConverter.supports(
        FlinkTypeFactory$.MODULE$.toLogicalRowType(rank.getRowType()));
  }

  /** Whether the rank's sole order key is an event-time (rowtime) attribute — the dedup signal. */
  static boolean isRowtimeOrder(StreamPhysicalRank rank) {
    List<RelFieldCollation> collations = rank.orderKey().getFieldCollations();
    if (collations.size() != 1) {
      return false;
    }
    RelDataType orderType =
        rank.getInput().getRowType().getFieldList().get(collations.get(0).getFieldIndex()).getType();
    return FlinkTypeFactory$.MODULE$.isRowtimeIndicatorType(orderType);
  }

  static int[] partitionColumns(StreamPhysicalRank rank) {
    return rank.partitionKey().toArray();
  }

  static int rowtimeColumn(StreamPhysicalRank rank) {
    return rank.orderKey().getFieldCollations().get(0).getFieldIndex();
  }

  static String unsupportedReason(StreamPhysicalRank rank) {
    return "deduplication: needs ROW_NUMBER() OVER (PARTITION BY … ORDER BY rowtime ASC) = 1"
        + " (keep-first, insert-only); keep-last/descending is retracting and stays on the host";
  }
}
