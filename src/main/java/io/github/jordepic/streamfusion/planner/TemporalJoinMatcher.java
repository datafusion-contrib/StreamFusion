package io.github.jordepic.streamfusion.planner;

import java.util.List;
import java.util.Optional;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.flink.table.planner.plan.nodes.exec.spec.JoinSpec;
import org.apache.flink.table.planner.plan.nodes.physical.common.CommonPhysicalJoin;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalTemporalJoin;
import org.apache.flink.table.runtime.operators.join.FlinkJoinType;

/**
 * Recognizes the event-time temporal table joins the native operator implements:
 * {@code probe JOIN versioned FOR SYSTEM_TIME AS OF probe.rowtime ON probe.k = versioned.k}. Requires
 * an INNER or LEFT join (Flink rejects RIGHT/FULL for temporal join), an event-time temporal condition
 * (the build side versioned by its rowtime), one or more equi-join keys of supported types, and no
 * residual non-equi predicate beyond the temporal condition. Processing-time temporal joins (which
 * Flink itself rejects for a versioned table) and anything else fall back to the host.
 *
 * <p>The probe/build rowtime columns and the build primary key are carried inside a synthetic
 * {@code __TEMPORAL_JOIN_CONDITION} RexCall that Flink embeds in the join's non-equi condition (and
 * only unpacks when translating to its exec node). We re-extract the two time-attribute column indices
 * from that call; the equi keys come from the join spec, as for the interval/window join.
 */
final class TemporalJoinMatcher {

  /** Flink's marker function for the temporal-join condition (carries the time attributes + PK). */
  private static final String TEMPORAL_JOIN_CONDITION = "__TEMPORAL_JOIN_CONDITION";

  private TemporalJoinMatcher() {}

  static boolean matches(StreamPhysicalTemporalJoin join) {
    return unsupportedReason(join) == null;
  }

  /** The specific reason this temporal join is not accelerable, or null if it is. */
  static String unsupportedReason(StreamPhysicalTemporalJoin join) {
    JoinSpec joinSpec = ((CommonPhysicalJoin) join).joinSpec();
    if (joinTypeCode(joinSpec.getJoinType()) < 0) {
      return "temporal join: only INNER/LEFT joins (Flink rejects RIGHT/FULL for temporal join)";
    }
    int[] leftKeys = joinSpec.getLeftKeys();
    int[] rightKeys = joinSpec.getRightKeys();
    if (leftKeys.length == 0 || leftKeys.length != rightKeys.length) {
      return "temporal join: needs at least one equi-join key";
    }
    for (boolean filterNull : joinSpec.getFilterNulls()) {
      if (!filterNull) {
        return "temporal join: requires null-dropping (INNER) equi keys";
      }
    }
    RelDataType leftType = join.getLeft().getRowType();
    RelDataType rightType = join.getRight().getRowType();
    for (int i = 0; i < leftKeys.length; i++) {
      if (!WindowAggregateMatcher.supportedGroupingKeyType(
              leftType.getFieldList().get(leftKeys[i]).getType().getSqlTypeName())
          || !WindowAggregateMatcher.supportedGroupingKeyType(
              rightType.getFieldList().get(rightKeys[i]).getType().getSqlTypeName())) {
        return "temporal join: equi-join keys must be bigint/int/string/boolean/date/timestamp/decimal";
      }
    }
    Optional<RexNode> condition = joinSpec.getNonEquiCondition();
    if (condition.isEmpty()) {
      return "temporal join: missing temporal join condition";
    }
    // Only the synthetic temporal condition may remain as a non-equi conjunct; any further residual
    // predicate is not supported yet (it would need to ride the joined row into the native filter).
    List<RexNode> conjuncts = RelOptUtil.conjunctions(condition.get());
    RexCall temporalCall = null;
    for (RexNode conjunct : conjuncts) {
      if (isTemporalCall(conjunct)) {
        temporalCall = (RexCall) conjunct;
      } else {
        return "temporal join: a residual non-equi predicate beyond FOR SYSTEM_TIME is not supported";
      }
    }
    if (temporalCall == null) {
      return "temporal join: missing temporal join condition";
    }
    if (!isRowTime(temporalCall, leftType.getFieldCount())) {
      return "temporal join: only event-time (FOR SYSTEM_TIME AS OF rowtime); proctime is unsupported";
    }
    return null;
  }

  /** The native join-type code (0=INNER, 1=LEFT), or -1 for an unsupported type. */
  static int joinTypeCode(FlinkJoinType joinType) {
    switch (joinType) {
      case INNER:
        return 0;
      case LEFT:
        return 1;
      default:
        return -1;
    }
  }

  static int joinTypeCode(StreamPhysicalTemporalJoin join) {
    return joinTypeCode(((CommonPhysicalJoin) join).joinSpec().getJoinType());
  }

  static int[] leftKeys(StreamPhysicalTemporalJoin join) {
    return ((CommonPhysicalJoin) join).joinSpec().getLeftKeys();
  }

  static int[] rightKeys(StreamPhysicalTemporalJoin join) {
    return ((CommonPhysicalJoin) join).joinSpec().getRightKeys();
  }

  /** Probe-side rowtime column index (into the left input row). */
  static int leftTime(StreamPhysicalTemporalJoin join) {
    return inputRefIndex(temporalCall(join).getOperands().get(0));
  }

  /** Build-side rowtime column index (into the right input row; the operand is over the joined row). */
  static int rightTime(StreamPhysicalTemporalJoin join) {
    int leftFieldCount = join.getLeft().getRowType().getFieldCount();
    return inputRefIndex(temporalCall(join).getOperands().get(1)) - leftFieldCount;
  }

  /** The single {@code __TEMPORAL_JOIN_CONDITION} call embedded in the join's non-equi condition. */
  private static RexCall temporalCall(StreamPhysicalTemporalJoin join) {
    RexNode condition = ((CommonPhysicalJoin) join).joinSpec().getNonEquiCondition().orElseThrow();
    for (RexNode conjunct : RelOptUtil.conjunctions(condition)) {
      if (isTemporalCall(conjunct)) {
        return (RexCall) conjunct;
      }
    }
    throw new IllegalStateException("temporal join condition not found");
  }

  private static boolean isTemporalCall(RexNode node) {
    return node instanceof RexCall
        && ((RexCall) node).getOperator().getName().equals(TEMPORAL_JOIN_CONDITION);
  }

  /**
   * A rowtime temporal condition carries the build-side rowtime as its second operand (an input ref
   * into the joined row); a proctime one carries the primary-key marker call there instead.
   */
  private static boolean isRowTime(RexCall temporalCall, int leftFieldCount) {
    if (temporalCall.getOperands().size() < 3) {
      return false;
    }
    int rightTimeRef = inputRefIndex(temporalCall.getOperands().get(1));
    return rightTimeRef >= leftFieldCount;
  }

  /** The column index of an input ref, unwrapping a single enclosing cast; -1 if not a ref. */
  private static int inputRefIndex(RexNode node) {
    if (node instanceof RexInputRef) {
      return ((RexInputRef) node).getIndex();
    }
    if (node instanceof RexCall && ((RexCall) node).getOperands().size() == 1) {
      return inputRefIndex(((RexCall) node).getOperands().get(0));
    }
    return -1;
  }
}
