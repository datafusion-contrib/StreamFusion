package io.github.jordepic.streamfusion.planner;

import java.lang.reflect.Field;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.flink.table.planner.plan.nodes.exec.spec.IntervalJoinSpec;
import org.apache.flink.table.planner.plan.nodes.exec.spec.JoinSpec;
import org.apache.flink.table.planner.plan.nodes.physical.common.CommonPhysicalJoin;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalIntervalJoin;
import org.apache.flink.table.runtime.operators.join.FlinkJoinType;

/**
 * Recognizes the event-time INNER interval joins the native operator implements:
 * {@code a JOIN b ON a.k = b.k AND a.rt BETWEEN b.rt + lower AND b.rt + upper}. Requires an INNER
 * join, an event-time interval, one or more equi-join keys all of supported types
 * (bigint/int/string), null-filtering keys (INNER semantics), and no residual non-equi predicate —
 * the whole interval condition must live in the window bounds. Anything else (outer joins, proctime,
 * an extra filter, an unsupported key type) falls back to the host.
 */
final class IntervalJoinMatcher {

  private IntervalJoinMatcher() {}

  static boolean matches(StreamPhysicalIntervalJoin join) {
    return unsupportedReason(join) == null;
  }

  /** The specific reason this interval join is not accelerable, or null if it is. */
  static String unsupportedReason(StreamPhysicalIntervalJoin join) {
    JoinSpec joinSpec = ((CommonPhysicalJoin) join).joinSpec();
    if (joinSpec.getJoinType() != FlinkJoinType.INNER) {
      return "interval join: only INNER joins (outer/semi/anti emit nulls or differ)";
    }
    int[] leftKeys = joinSpec.getLeftKeys();
    int[] rightKeys = joinSpec.getRightKeys();
    if (leftKeys.length == 0 || leftKeys.length != rightKeys.length) {
      return "interval join: needs at least one equi-join key";
    }
    if (joinSpec.getNonEquiCondition().isPresent()) {
      return "interval join: a residual non-equi condition is not applied";
    }
    for (boolean filterNull : joinSpec.getFilterNulls()) {
      if (!filterNull) {
        return "interval join: requires null-dropping (INNER) equi keys";
      }
    }
    RelDataType leftType = join.getLeft().getRowType();
    RelDataType rightType = join.getRight().getRowType();
    for (int i = 0; i < leftKeys.length; i++) {
      if (!WindowAggregateMatcher.supportedKeyType(
              leftType.getFieldList().get(leftKeys[i]).getType().getSqlTypeName())
          || !WindowAggregateMatcher.supportedKeyType(
              rightType.getFieldList().get(rightKeys[i]).getType().getSqlTypeName())) {
        return "interval join: equi-join keys must be bigint/int/string";
      }
    }
    IntervalJoinSpec.WindowBounds bounds = windowBounds(join);
    if (bounds == null || !bounds.isEventTime()) {
      return "interval join: requires event-time interval bounds (proctime not supported)";
    }
    return null;
  }

  static int[] leftKeys(StreamPhysicalIntervalJoin join) {
    return ((CommonPhysicalJoin) join).joinSpec().getLeftKeys();
  }

  static int[] rightKeys(StreamPhysicalIntervalJoin join) {
    return ((CommonPhysicalJoin) join).joinSpec().getRightKeys();
  }

  static int leftTime(StreamPhysicalIntervalJoin join) {
    return windowBounds(join).getLeftTimeIdx();
  }

  static int rightTime(StreamPhysicalIntervalJoin join) {
    return windowBounds(join).getRightTimeIdx();
  }

  /** Inclusive lower bound (millis) on {@code left.rt - right.rt}. */
  static long lowerMillis(StreamPhysicalIntervalJoin join) {
    return windowBounds(join).getLeftLowerBound();
  }

  /** Inclusive upper bound (millis) on {@code left.rt - right.rt}. */
  static long upperMillis(StreamPhysicalIntervalJoin join) {
    return windowBounds(join).getLeftUpperBound();
  }

  /** Reads the interval bounds, which the rel keeps in a private field (no public accessor). */
  private static IntervalJoinSpec.WindowBounds windowBounds(StreamPhysicalIntervalJoin join) {
    try {
      Field field = StreamPhysicalIntervalJoin.class.getDeclaredField("windowBounds");
      field.setAccessible(true);
      return (IntervalJoinSpec.WindowBounds) field.get(join);
    } catch (ReflectiveOperationException e) {
      throw new IllegalStateException("failed to read interval-join window bounds", e);
    }
  }
}
