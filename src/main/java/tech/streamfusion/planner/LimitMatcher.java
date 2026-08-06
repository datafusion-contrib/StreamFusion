package tech.streamfusion.planner;

import tech.streamfusion.operator.RowDataArrowConverter;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rex.RexLiteral;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory$;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalRel;
import org.apache.flink.table.planner.plan.utils.ChangelogPlanUtils;

/**
 * Recognizes a global {@code FETCH}/{@code LIMIT} — Flink's {@code StreamPhysicalLimit} (plain
 * {@code LIMIT n}) and {@code StreamPhysicalSortLimit} ({@code ORDER BY … LIMIT n}). Both extend
 * Calcite {@link Sort} and both lower to a global ({@code ALL_IN_ONE}, no partition) {@code ROW_NUMBER}
 * rank with range {@code [offset+1, offset+fetch]} — exactly the append-only streaming Top-N the
 * native ranker already runs, with an empty partition key. {@code LIMIT} has an empty sort collation
 * (the ranker then keeps the first {@code N} rows by arrival, evicting the newest beyond {@code N},
 * which never enters — so it is insert-only), while {@code SORT LIMIT} carries the order keys and
 * emits a changelog as the top set changes.
 *
 * <p>Matched when a {@code FETCH} is present (streaming requires it) and the row type the row/Arrow
 * conversion carries. The rank window is {@code [offset+1, offset+fetch]}; an {@code OFFSET} routes
 * through the retracting ranker (which keeps the full buffer), a no-offset limit through the
 * append-only one. The caller additionally requires an insert-only input — a retracting input makes
 * the host pick its retract strategy, which we do not reproduce.
 */
final class LimitMatcher {

  private LimitMatcher() {}

  static boolean matches(Sort sort) {
    if (sort.fetch == null) {
      return false; // a streaming FETCH/LIMIT must bound the output (Flink rejects an unbounded one)
    }
    return RowDataArrowConverter.supports(
        FlinkTypeFactory$.MODULE$.toLogicalRowType(sort.getRowType()));
  }

  /** The 0-based offset (the {@code OFFSET} count, 0 if absent). */
  static long offset(Sort sort) {
    return sort.offset == null ? 0 : RexLiteral.intValue(sort.offset);
  }

  /** The rank window upper bound (rankEnd) = offset + fetch; the operator emits {@code [offset+1, end]}. */
  static long limit(Sort sort) {
    return offset(sort) + RexLiteral.intValue(sort.fetch);
  }

  static int[] sortIndices(Sort sort) {
    return sort.getCollation().getFieldCollations().stream()
        .mapToInt(RelFieldCollation::getFieldIndex)
        .toArray();
  }

  static int[] sortAscending(Sort sort) {
    return sort.getCollation().getFieldCollations().stream()
        .mapToInt(fc -> fc.getDirection().isDescending() ? 0 : 1)
        .toArray();
  }

  static int[] sortNullsFirst(Sort sort) {
    return sort.getCollation().getFieldCollations().stream()
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

  static String unsupportedReason(Sort sort) {
    if (sort.fetch == null) {
      return "limit: a FETCH/LIMIT count is required on a streaming query";
    }
    return "limit: needs a row type the Arrow conversion supports and an insert-only input";
  }

  static RelNode substitute(Sort sort, PlanContext ctx) {
    boolean insertOnlyInput = ChangelogPlanUtils.isInsertOnly((StreamPhysicalRel) sort.getInput());
    if (LimitMatcher.matches(sort) && insertOnlyInput) {
      if (!NativeConfig.operatorEnabled("limit")) {
        ctx.decline(Substitution.disabledReason("limit"));
        return null;
      }
      int[] partitionColumns = new int[0]; // global limit — a single gather, no partition
      long offset = LimitMatcher.offset(sort);
      return new StreamPhysicalNativeColumnarTopN(
          sort.getCluster(),
          sort.getTraitSet(),
          ctx.columnarInput(sort.getInput(), partitionColumns),
          sort.getRowType(),
          partitionColumns,
          LimitMatcher.sortIndices(sort),
          LimitMatcher.sortAscending(sort),
          LimitMatcher.sortNullsFirst(sort),
          offset,
          LimitMatcher.limit(sort),
          false, // a global LIMIT never projects a rank column
          offset > 0, // an OFFSET uses the retracting ranker; no-offset the append-only one
          null,
          false);
    }
    // A retracting input is the one reason not in unsupportedReason.
    ctx.decline(
        insertOnlyInput
            ? LimitMatcher.unsupportedReason(sort)
            : "limit: needs an insert-only input (the append-only ranker is implemented)");
    return null;
  }
}
