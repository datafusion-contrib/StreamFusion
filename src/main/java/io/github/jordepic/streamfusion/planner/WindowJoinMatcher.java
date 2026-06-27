package io.github.jordepic.streamfusion.planner;

import java.util.Optional;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.flink.table.planner.plan.logical.WindowAttachedWindowingStrategy;
import org.apache.flink.table.planner.plan.logical.WindowingStrategy;
import org.apache.flink.table.planner.plan.nodes.exec.spec.JoinSpec;
import org.apache.flink.table.planner.plan.nodes.physical.common.CommonPhysicalJoin;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalWindowJoin;

/**
 * Recognizes the event-time INNER window joins the native operator implements:
 * {@code a JOIN b ON a.k = b.k} where both sides were windowed by identical event-time windowing
 * TVFs, so each row carries matching {@code window_start}/{@code window_end} columns. Requires an
 * INNER join, one or more equi-join keys of supported types (bigint/int/string/boolean/date/timestamp/decimal), null-filtering
 * keys, no residual non-equi predicate, and a window-attached event-time windowing on both sides.
 * Anything else (outer joins, proctime, an extra filter, an unsupported key type) falls back.
 */
final class WindowJoinMatcher {

  private WindowJoinMatcher() {}

  static boolean matches(StreamPhysicalWindowJoin join) {
    return unsupportedReason(join) == null;
  }

  /** The specific reason this window join is not accelerable, or null if it is. */
  static String unsupportedReason(StreamPhysicalWindowJoin join) {
    JoinSpec joinSpec = ((CommonPhysicalJoin) join).joinSpec();
    if (IntervalJoinMatcher.joinTypeCode(joinSpec.getJoinType()) < 0) {
      return "window join: only INNER/LEFT/RIGHT/FULL joins (semi/anti are regular joins)";
    }
    int[] leftKeys = joinSpec.getLeftKeys();
    int[] rightKeys = joinSpec.getRightKeys();
    if (leftKeys.length == 0 || leftKeys.length != rightKeys.length) {
      return "window join: needs at least one equi-join key";
    }
    if (joinSpec.getNonEquiCondition().isPresent() && nonEquiPredicate(join) == null) {
      return "window join: the residual non-equi condition is not natively expressible";
    }
    for (boolean filterNull : joinSpec.getFilterNulls()) {
      if (!filterNull) {
        return "window join: requires null-dropping (INNER) equi keys";
      }
    }
    if (!(join.leftWindowing() instanceof WindowAttachedWindowingStrategy)
        || !(join.rightWindowing() instanceof WindowAttachedWindowingStrategy)
        || !join.leftWindowing().isRowtime()
        || !join.rightWindowing().isRowtime()) {
      return "window join: both sides must carry an event-time, window-attached window";
    }
    RelDataType leftType = join.getLeft().getRowType();
    RelDataType rightType = join.getRight().getRowType();
    for (int i = 0; i < leftKeys.length; i++) {
      if (!WindowAggregateMatcher.supportedGroupingKeyType(
              leftType.getFieldList().get(leftKeys[i]).getType().getSqlTypeName())
          || !WindowAggregateMatcher.supportedGroupingKeyType(
              rightType.getFieldList().get(rightKeys[i]).getType().getSqlTypeName())) {
        return "window join: equi-join keys must be bigint/int/string/boolean/date/timestamp/decimal";
      }
    }
    return null;
  }

  /**
   * The encoded residual non-equi condition (its input refs index into the joined
   * {@code [left.., right..]} row), or null when there is none or it is not natively expressible.
   */
  static RexExpression nonEquiPredicate(StreamPhysicalWindowJoin join) {
    Optional<RexNode> condition = ((CommonPhysicalJoin) join).joinSpec().getNonEquiCondition();
    if (condition.isEmpty()) {
      return null;
    }
    RexNode expanded =
        RexUtil.expandSearch(join.getCluster().getRexBuilder(), null, condition.get());
    return RexExpression.encode(expanded);
  }

  /** The native join-type code (0=INNER,1=LEFT,2=RIGHT,3=FULL); never -1 once {@link #matches}. */
  static int joinTypeCode(StreamPhysicalWindowJoin join) {
    return IntervalJoinMatcher.joinTypeCode(((CommonPhysicalJoin) join).joinSpec().getJoinType());
  }

  static int[] leftKeys(StreamPhysicalWindowJoin join) {
    return ((CommonPhysicalJoin) join).joinSpec().getLeftKeys();
  }

  static int[] rightKeys(StreamPhysicalWindowJoin join) {
    return ((CommonPhysicalJoin) join).joinSpec().getRightKeys();
  }

  static int leftWindowStart(StreamPhysicalWindowJoin join) {
    return attached(join.leftWindowing()).getWindowStart();
  }

  static int leftWindowEnd(StreamPhysicalWindowJoin join) {
    return attached(join.leftWindowing()).getWindowEnd();
  }

  static int rightWindowStart(StreamPhysicalWindowJoin join) {
    return attached(join.rightWindowing()).getWindowStart();
  }

  static int rightWindowEnd(StreamPhysicalWindowJoin join) {
    return attached(join.rightWindowing()).getWindowEnd();
  }

  private static WindowAttachedWindowingStrategy attached(WindowingStrategy windowing) {
    return (WindowAttachedWindowingStrategy) windowing;
  }
}
