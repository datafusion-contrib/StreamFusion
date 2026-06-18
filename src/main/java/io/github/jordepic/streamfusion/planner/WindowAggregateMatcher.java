package io.github.jordepic.streamfusion.planner;

import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.flink.table.planner.plan.logical.TimeAttributeWindowingStrategy;
import org.apache.flink.table.planner.plan.logical.TumblingWindowSpec;
import org.apache.flink.table.planner.plan.logical.WindowingStrategy;
import org.apache.flink.table.types.logical.LogicalTypeRoot;

/**
 * Recognizes the window aggregations the native operator implements: an event-time tumbling window
 * over a local-time-zone attribute, with no extra grouping keys or a single integer key, and one or
 * more aggregates that all read the same bigint value column reducing it to an int
 * (SUM/MIN/MAX/COUNT, plus AVG only as a lone aggregate). Operates on the
 * windowing/grouping/aggregate components so the single-phase and local-phase nodes share it.
 */
final class WindowAggregateMatcher {

  static final int KIND_AVG = 4;

  private WindowAggregateMatcher() {}

  static boolean matches(
      WindowingStrategy windowing,
      int[] grouping,
      scala.collection.Seq<AggregateCall> aggCalls,
      RelDataType inputType) {
    if (!(windowing instanceof TimeAttributeWindowingStrategy) || !windowing.isRowtime()) {
      return false;
    }
    // Window bounds are emitted via the session zone, which matches the host only for a
    // local-time-zone event-time attribute.
    if (windowing.getTimeAttributeType().getTypeRoot()
        != LogicalTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE) {
      return false;
    }
    if (!(windowing.getWindow() instanceof TumblingWindowSpec)) {
      return false;
    }
    if (grouping.length > 1 || aggCalls.isEmpty()) {
      return false;
    }
    if (grouping.length == 1
        && inputType.getFieldList().get(grouping[0]).getType().getSqlTypeName()
            != SqlTypeName.BIGINT) {
      return false;
    }

    // Every aggregate must read the same single bigint value column and use a supported kind.
    int valueColumn = aggCalls.apply(0).getArgList().isEmpty() ? -1 : aggCalls.apply(0).getArgList().get(0);
    if (valueColumn < 0
        || inputType.getFieldList().get(valueColumn).getType().getSqlTypeName()
            != SqlTypeName.BIGINT) {
      return false;
    }
    boolean multiple = aggCalls.size() > 1;
    for (int i = 0; i < aggCalls.size(); i++) {
      AggregateCall call = aggCalls.apply(i);
      int kind = aggregateKind(call.getAggregation().getKind());
      if (call.getArgList().size() != 1 || call.getArgList().get(0) != valueColumn || kind < 0) {
        return false;
      }
      // AVG has multi-field partial state, so it is only supported as a lone aggregate.
      if (kind == KIND_AVG && multiple) {
        return false;
      }
    }
    return true;
  }

  static boolean containsAvg(scala.collection.Seq<AggregateCall> aggCalls) {
    for (int i = 0; i < aggCalls.size(); i++) {
      if (aggregateKind(aggCalls.apply(i).getAggregation().getKind()) == KIND_AVG) {
        return true;
      }
    }
    return false;
  }

  static long windowMillis(WindowingStrategy windowing) {
    return ((TumblingWindowSpec) windowing.getWindow()).getSize().toMillis();
  }

  static int timeColumn(WindowingStrategy windowing) {
    return ((TimeAttributeWindowingStrategy) windowing).getTimeAttributeIndex();
  }

  static int valueColumn(scala.collection.Seq<AggregateCall> aggCalls) {
    return aggCalls.apply(0).getArgList().get(0);
  }

  static int keyColumn(int[] grouping) {
    return grouping.length == 1 ? grouping[0] : -1;
  }

  static int[] kinds(scala.collection.Seq<AggregateCall> aggCalls) {
    int[] kinds = new int[aggCalls.size()];
    for (int i = 0; i < aggCalls.size(); i++) {
      kinds[i] = aggregateKind(aggCalls.apply(i).getAggregation().getKind());
    }
    return kinds;
  }

  /** Native code for the aggregate, or -1 if unsupported. Mirrors the kinds in {@code Native}. */
  static int aggregateKind(SqlKind kind) {
    switch (kind) {
      case SUM:
        return 0;
      case MIN:
        return 1;
      case MAX:
        return 2;
      case COUNT:
        return 3;
      case AVG:
        return KIND_AVG;
      default:
        return -1;
    }
  }
}
