package tech.streamfusion.planner;

import tech.streamfusion.operator.RowDataArrowConverter;
import java.util.List;
import java.util.Optional;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory$;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalRank;
import org.apache.flink.table.planner.plan.utils.ChangelogPlanUtils;
import org.apache.flink.table.runtime.operators.rank.ConstantRankRange;
import org.apache.flink.table.runtime.operators.rank.RankType;

/**
 * Recognizes deduplication: {@code ROW_NUMBER() OVER (PARTITION BY … ORDER BY <time> …) = 1}, where
 * {@code <time>} is a rowtime or proctime attribute. The host plans this as a {@link
 * StreamPhysicalRank} whose sole order key is a time attribute and whose range is exactly rank 1 —
 * distinct from a value-ordered Top-N (handled by {@link TopNMatcher}). Ascending is keep-first,
 * descending is keep-last. Rowtime keep-first ({@code RowTimeDeduplicateKeepFirstRowFunction}) emits
 * each key's minimum-rowtime row once on the watermark; the other three — rowtime keep-last ({@code
 * RowTimeDeduplicateFunction}) and proctime keep-first/keep-last ({@code ProcTimeDeduplicateKeep…}) —
 * emit eagerly in arrival order. A non-time order key is a Top-N, not deduplication.
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
    if (rank.orderKey().getFieldCollations().size() != 1) {
      return false; // a single order key (the time attribute); ascending = keep-first, descending = keep-last
    }
    if (!isTimeOrder(rank)) {
      return false; // a non-time order key is a value Top-N, not deduplication
    }
    return RowDataArrowConverter.supports(
        FlinkTypeFactory$.MODULE$.toLogicalRowType(rank.getRowType()));
  }

  /** Keep-last (descending rowtime) vs keep-first (ascending). Keep-last emits a retract changelog. */
  static boolean keepLast(StreamPhysicalRank rank) {
    return rank.orderKey().getFieldCollations().get(0).getDirection().isDescending();
  }

  /** Whether the host wants UPDATE_BEFORE rows on this node's output edge (keep-last only). */
  static boolean generateUpdateBefore(StreamPhysicalRank rank) {
    return ChangelogPlanUtils.generateUpdateBefore(rank);
  }

  /** Whether the rank's sole order key is a time attribute (rowtime or proctime) — the dedup signal. */
  static boolean isTimeOrder(StreamPhysicalRank rank) {
    return orderType(rank)
        .map(
            type ->
                FlinkTypeFactory$.MODULE$.isRowtimeIndicatorType(type)
                    || FlinkTypeFactory$.MODULE$.isProctimeIndicatorType(type))
        .orElse(false);
  }

  /** Whether the dedup orders by processing time (arrival order) rather than a rowtime. */
  static boolean isProctime(StreamPhysicalRank rank) {
    return orderType(rank)
        .map(FlinkTypeFactory$.MODULE$::isProctimeIndicatorType)
        .orElse(false);
  }

  private static Optional<RelDataType> orderType(StreamPhysicalRank rank) {
    List<RelFieldCollation> collations = rank.orderKey().getFieldCollations();
    if (collations.size() != 1) {
      return Optional.empty();
    }
    return Optional.of(
        rank.getInput().getRowType().getFieldList().get(collations.get(0).getFieldIndex()).getType());
  }

  static int[] partitionColumns(StreamPhysicalRank rank) {
    return rank.partitionKey().toArray();
  }

  static int rowtimeColumn(StreamPhysicalRank rank) {
    return rank.orderKey().getFieldCollations().get(0).getFieldIndex();
  }

  static String unsupportedReason(StreamPhysicalRank rank) {
    return "deduplication: needs ROW_NUMBER() OVER (PARTITION BY … ORDER BY rowtime|proctime ASC|DESC)"
        + " = 1 (keep-first or keep-last) over an insert-only input";
  }

  static RelNode substitute(StreamPhysicalRank rank, PlanContext ctx) {
    int[] partitionColumns = DeduplicateMatcher.partitionColumns(rank);
    // Columnar (Arrow in/out); the partitioned shuffle stays columnar where the input sits on a
    // columnar producer, else the transition pass transposes at the boundary. Watermark-released
    // keep-first is insert-only; keep-last — and rowtime keep-first under mini-batch — emits a
    // retract changelog (the native rel inherits the host rank's changelog trait, so the
    // boundary transpose carries $row_kind$).
    return new StreamPhysicalNativeDeduplicate(
        rank.getCluster(),
        rank.getTraitSet(),
        ctx.columnarInput(rank.getInput(), partitionColumns),
        rank.getRowType(),
        partitionColumns,
        DeduplicateMatcher.rowtimeColumn(rank),
        DeduplicateMatcher.keepLast(rank),
        DeduplicateMatcher.generateUpdateBefore(rank),
        DeduplicateMatcher.isProctime(rank));
  }
}
