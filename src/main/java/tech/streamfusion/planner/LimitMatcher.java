package tech.streamfusion.planner;

import java.lang.reflect.Field;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rex.RexLiteral;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory$;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalRel;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalSortLimit;
import org.apache.flink.table.planner.plan.utils.ChangelogPlanUtils;
import org.apache.flink.table.planner.plan.utils.RankProcessStrategy;
import tech.streamfusion.operator.RowDataArrowConverter;

/**
 * Recognizes a global {@code FETCH}/{@code LIMIT} — Flink's {@code StreamPhysicalLimit} (plain
 * {@code LIMIT n}) and {@code StreamPhysicalSortLimit} ({@code ORDER BY … LIMIT n}). Both extend
 * Calcite {@link Sort} and both lower to a global ({@code ALL_IN_ONE}, no partition) {@code ROW_NUMBER}
 * rank with range {@code [offset+1, offset+fetch]} — exactly the streaming Top-N the
 * native ranker already runs, with an empty partition key. {@code LIMIT} has an empty sort collation
 * (the ranker then keeps the first {@code N} rows by arrival, evicting the newest beyond {@code N},
 * which never enters — so it is insert-only), while {@code SORT LIMIT} carries the order keys and
 * emits a changelog as the top set changes.
 *
 * <p>Matched when a {@code FETCH} is present (streaming requires it) and the row type the row/Arrow
 * conversion carries. The rank window is {@code [offset+1, offset+fetch]}; an {@code OFFSET} routes
 * through the retracting ranker (which keeps the full buffer), a no-offset limit through the
 * append-only one. Updating inputs reuse the host's selected retract or update-fast strategy;
 * the latter replaces rows by their unique key while retaining the skipped prefix.
 */
final class LimitMatcher {
  private LimitMatcher() {}

  static boolean matches(Sort sort) {
    if (sort.fetch == null) {
      return false; // a streaming FETCH/LIMIT must bound the output (Flink rejects an unbounded one)
    }
    return ArrowRowTypeSupport.supports(FlinkTypeFactory$.MODULE$.toLogicalRowType(sort.getInput().getRowType()))
        && RowDataArrowConverter.supports(
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
    return "limit: a row type is unsupported by the Arrow conversion or retained-row codec";
  }

  static RelNode substitute(Sort sort, PlanContext ctx) {
    if (LimitMatcher.matches(sort)) {
      if (!NativeConfig.operatorEnabled("limit")) {
        ctx.decline(Substitution.disabledReason("limit"));
        return null;
      }
      int[] partitionColumns = new int[0]; // global limit — a single gather, no partition
      long offset = LimitMatcher.offset(sort);
      RankProcessStrategy strategy = rankStrategy(sort);
      if (strategy == null || strategy instanceof RankProcessStrategy.UndefinedStrategy) {
        ctx.decline("limit: the selected Flink rank strategy is unavailable");
        return null;
      }
      int[] rowKeyColumns = null;
      if (strategy instanceof RankProcessStrategy.UpdateFastStrategy) {
        RankProcessStrategy.UpdateFastStrategy updateFast =
            ((RankProcessStrategy.UpdateFastStrategy) strategy);

        rowKeyColumns = updateFast.getPrimaryKeys();
      }
      if (offset > 0 && rowKeyColumns == null) {
        String reason = TopNMatcher.retractingOffsetInputReason(sort.getInput());
        if (reason != null) {
          ctx.decline(reason);
          return null;
        }
      }
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
          -1,
          false, // a global LIMIT never projects a rank column
          offset > 0 || strategy instanceof RankProcessStrategy.RetractStrategy,
          rowKeyColumns,
          ChangelogPlanUtils.generateUpdateBefore((StreamPhysicalRel) sort));
    }
    ctx.decline(LimitMatcher.unsupportedReason(sort));
    return null;
  }

  private static RankProcessStrategy rankStrategy(Sort sort) {
    if (sort instanceof StreamPhysicalSortLimit) {
      // Scala's constructor parameter has no public accessor. Read the strategy already selected
      // during changelog inference: recomputing it could require retractions the input no longer emits.
      try {
        Field field = StreamPhysicalSortLimit.class.getDeclaredField("rankStrategy");
        field.setAccessible(true);
        return (RankProcessStrategy) field.get(sort);
      } catch (ReflectiveOperationException e) {
        return null;
      }
    }
    return ChangelogPlanUtils.isInsertOnly((StreamPhysicalRel) sort.getInput())
        ? RankProcessStrategy.APPEND_FAST_STRATEGY : RankProcessStrategy.RETRACT_STRATEGY;
  }
}
