package tech.streamfusion.planner;

import java.util.Map;
import java.util.Optional;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Exchange;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory$;
import org.apache.flink.table.planner.plan.nodes.exec.spec.JoinSpec;
import org.apache.flink.table.planner.plan.nodes.physical.common.CommonPhysicalJoin;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalGroupAggregateBase;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalJoin;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalRel;
import org.apache.flink.table.planner.plan.utils.ChangelogPlanUtils;
import org.apache.flink.table.runtime.operators.join.FlinkJoinType;
import tech.streamfusion.compat.FlinkCompat;
import tech.streamfusion.operator.RowDataArrowConverter;

/**
 * Recognizes the regular (non-windowed) equi-joins the native updating join implements:
 * {@code a JOIN b ON a.k = b.k}, where both inputs may be changelogs. Supports INNER, LEFT/RIGHT/FULL
 * outer, and SEMI/ANTI (the native joiner tracks a per-row match-degree for the latter families).
 * Also admits keyless INNER joins over two insert-only inputs. Requires an expressible predicate
 * and input/output types the Arrow converter and retained-row codec support. Each equi-key retains
 * its own ordinary or null-safe equality policy.
 */
final class RegularJoinMatcher {

  private RegularJoinMatcher() {}

  static boolean matches(StreamPhysicalJoin join) {
    return unsupportedReason(join) == null;
  }

  static String unsupportedReason(StreamPhysicalJoin join) {
    JoinSpec joinSpec = ((CommonPhysicalJoin) join).joinSpec();
    if (joinTypeCode(joinSpec.getJoinType()) < 0) {
      return "regular join: unsupported join type " + joinSpec.getJoinType();
    }
    int[] leftKeys = joinSpec.getLeftKeys();
    if (leftKeys.length != joinSpec.getRightKeys().length) {
      return "regular join: mismatched join-key arity";
    }
    if (leftKeys.length == 0) {
      if (joinSpec.getJoinType() != FlinkJoinType.INNER
          || !ChangelogPlanUtils.isInsertOnly((StreamPhysicalRel) join.getLeft())
          || !ChangelogPlanUtils.isInsertOnly((StreamPhysicalRel) join.getRight())) {
        return "regular join: keyless joins require INNER and two insert-only inputs";
      }
      if (join.getLeft().getRowType().getFieldCount() == 0
          || join.getRight().getRowType().getFieldCount() == 0) {
        return "regular join: keyless joins require a payload column on each input";
      }
    }
    if (joinSpec.getNonEquiCondition().isPresent() && nonEquiPredicate(join) == null) {
      return "regular join: the residual non-equi condition is not natively expressible";
    }
    if (!RowDataArrowConverter.supports(
            FlinkTypeFactory$.MODULE$.toLogicalRowType(join.getLeft().getRowType()))
        || !RowDataArrowConverter.supports(
            FlinkTypeFactory$.MODULE$.toLogicalRowType(join.getRight().getRowType()))) {
      return "regular join: an input column type is not supported";
    }
    if (!ArrowRowTypeSupport.supports(
            FlinkTypeFactory$.MODULE$.toLogicalRowType(join.getLeft().getRowType()))
        || !ArrowRowTypeSupport.supports(
            FlinkTypeFactory$.MODULE$.toLogicalRowType(join.getRight().getRowType()))) {
      return "regular join: Arrow row codec cannot carry MAP or MULTISET payloads";
    }
    return null;
  }

  /** The native join-type code (0=INNER,1=LEFT,2=RIGHT,3=FULL,4=SEMI,5=ANTI), or -1 if unsupported. */
  static int joinTypeCode(FlinkJoinType joinType) {
    switch (joinType) {
      case INNER:
        return 0;
      case LEFT:
        return 1;
      case RIGHT:
        return 2;
      case FULL:
        return 3;
      case SEMI:
        return 4;
      case ANTI:
        return 5;
      default:
        return -1;
    }
  }

  static int joinTypeCode(StreamPhysicalJoin join) {
    return joinTypeCode(((CommonPhysicalJoin) join).joinSpec().getJoinType());
  }

  /**
   * The encoded residual non-equi join condition (its input refs index into the joined
   * {@code [left.., right..]} row), or null when the join has none or it is not natively expressible.
   * Search expressions are expanded first, as the Calc path does.
   */
  static RexExpression nonEquiPredicate(StreamPhysicalJoin join) {
    Optional<RexNode> condition = ((CommonPhysicalJoin) join).joinSpec().getNonEquiCondition();
    if (condition.isEmpty()) {
      return null;
    }
    RexNode expanded =
        RexUtil.expandSearch(join.getCluster().getRexBuilder(), null, condition.get());
    return RexExpression.encode(expanded, join);
  }

  static int[] leftKeys(StreamPhysicalJoin join) {
    return ((CommonPhysicalJoin) join).joinSpec().getLeftKeys();
  }

  static int[] rightKeys(StreamPhysicalJoin join) {
    return ((CommonPhysicalJoin) join).joinSpec().getRightKeys();
  }

  static int[] filterNulls(StreamPhysicalJoin join) {
    boolean[] flags = ((CommonPhysicalJoin) join).joinSpec().getFilterNulls();
    return java.util.stream.IntStream.range(0, flags.length)
        .map(i -> flags[i] ? 1 : 0).toArray();
  }

  /** Whether the join keys contain a planner-proven upsert key for this input side. */
  static boolean joinKeyIsUnique(StreamPhysicalJoin join, int inputOrdinal) {
    int[] joinKeys = inputOrdinal == 0 ? leftKeys(join) : rightKeys(join);
    org.apache.calcite.rel.RelNode input = join.getInput(inputOrdinal);
    scala.collection.Iterator<int[]> keys = join.getUpsertKeys(input, joinKeys).iterator();
    while (keys.hasNext()) {
      int[] uniqueKey = keys.next();
      boolean contained = true;
      for (int column : uniqueKey) {
        boolean found = false;
        for (int joinKey : joinKeys) {
          if (column == joinKey) {
            found = true;
            break;
          }
        }
        contained &= found;
      }
      if (contained) {
        return true;
      }
    }
    // Changelog inference can consume this metadata before the physical keyed Exchange is fixed,
    // while a later query through getUpsertKeysInKeyGroupRange may be empty. Preserve the exact
    // structural invariant for GROUP BY: its output grouping prefix is an upsert key, and an
    // Exchange does not change it. This is the same key Flink's GroupAggFunction uses for its
    // update-after-only changelog.
    RelNode producer = input instanceof Exchange ? ((Exchange) input).getInput() : input;
    int groupCount =
        producer instanceof StreamPhysicalGroupAggregateBase
            ? ((StreamPhysicalGroupAggregateBase) producer).grouping().length
            : producer instanceof StreamPhysicalNativeColumnarGroupAggregate
                ? ((StreamPhysicalNativeColumnarGroupAggregate) producer).groupingCount()
                : 0;
    if (groupCount > 0) {
      for (int groupColumn = 0; groupColumn < groupCount; groupColumn++) {
        boolean found = false;
        for (int joinKey : joinKeys) {
          found |= joinKey == groupColumn;
        }
        if (!found) {
          return false;
        }
      }
      return true;
    }
    return false;
  }

  static RelNode substitute(StreamPhysicalJoin join, PlanContext ctx) {
    int[] leftKeys = RegularJoinMatcher.leftKeys(join);
    int[] rightKeys = RegularJoinMatcher.rightKeys(join);
    // Child substitution has already run. Query the original host tree: native children do not
    // implement Flink's upsert-key metadata, and querying them turns replacement state into a
    // multiset that can emit obsolete rows again on a later match.
    StreamPhysicalJoin original = (StreamPhysicalJoin) ctx.originalNode(join);
    boolean leftJoinKeyUnique = RegularJoinMatcher.joinKeyIsUnique(original, 0);
    boolean rightJoinKeyUnique = RegularJoinMatcher.joinKeyIsUnique(original, 1);
    boolean leftInsertOnly =
        ChangelogPlanUtils.isInsertOnly((StreamPhysicalRel) join.getLeft());
    boolean rightInsertOnly =
        ChangelogPlanUtils.isInsertOnly((StreamPhysicalRel) join.getRight());
    // A STATE_TTL hint sets each side's retention independently (0 = left, 1 = right —
    // Flink's FlinkHints.LEFT_INPUT convention), overriding the job-wide retention for that
    // side alone; -1 means no hint, resolved at translate time.
    Map<Integer, Long> hintTtls = FlinkCompat.joinStateTtl(join.getHints());
    // Columnar (Arrow in/out); keep each side's keyed shuffle columnar where it sits on a
    // columnar producer, else the transition pass transposes at the boundary.
    return new StreamPhysicalNativeColumnarUpdatingJoin(
        join.getCluster(),
        join.getTraitSet(),
        ctx.columnarInput(join.getLeft(), leftKeys),
        ctx.columnarInput(join.getRight(), rightKeys),
        join.getRowType(),
        leftKeys,
        rightKeys,
        filterNulls(join),
        RegularJoinMatcher.joinTypeCode(join),
        RegularJoinMatcher.nonEquiPredicate(join),
        leftJoinKeyUnique,
        rightJoinKeyUnique,
        leftInsertOnly,
        rightInsertOnly,
        hintTtls.getOrDefault(0, -1L),
        hintTtls.getOrDefault(1, -1L));
  }
}
