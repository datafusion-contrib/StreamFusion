package io.github.jordepic.streamfusion.planner;

import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.flink.table.planner.plan.logical.TimeAttributeWindowingStrategy;
import org.apache.flink.table.planner.plan.logical.TumblingWindowSpec;
import org.apache.flink.table.planner.plan.logical.WindowingStrategy;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalWindowAggregate;
import org.apache.flink.table.types.logical.LogicalTypeRoot;

/**
 * Recognizes the window aggregations the native operator implements: an event-time tumbling window
 * with no extra grouping keys and a single aggregate over one column that reduces an int column to
 * an int ({@code SUM}, {@code MIN}, {@code MAX}, {@code COUNT}, {@code AVG}). Matching only these keeps results
 * identical and lets every other windowing fall back to the host engine.
 */
final class WindowAggregateMatcher {

  private WindowAggregateMatcher() {}

  static boolean matches(StreamPhysicalWindowAggregate aggregate) {
    WindowingStrategy windowing = aggregate.windowing();
    if (!(windowing instanceof TimeAttributeWindowingStrategy) || !windowing.isRowtime()) {
      return false;
    }
    // The operator emits window bounds by converting epoch millis through the session zone, which
    // matches the host only for a local-time-zone event-time attribute.
    if (windowing.getTimeAttributeType().getTypeRoot()
        != LogicalTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE) {
      return false;
    }
    if (!(windowing.getWindow() instanceof TumblingWindowSpec)) {
      return false;
    }
    // Group by the window only, or by the window plus a single integer key.
    int[] grouping = aggregate.grouping();
    if (grouping.length > 1 || aggregate.aggCalls().size() != 1) {
      return false;
    }
    if (grouping.length == 1
        && aggregate.getInput().getRowType().getFieldList().get(grouping[0]).getType().getSqlTypeName()
            != SqlTypeName.BIGINT) {
      return false;
    }
    AggregateCall call = aggregate.aggCalls().apply(0);
    return call.getArgList().size() == 1 && aggregateKind(call.getAggregation().getKind()) >= 0;
  }

  /** Index of the single grouping key column, or -1 when grouping by the window only. */
  static int keyColumn(StreamPhysicalWindowAggregate aggregate) {
    int[] grouping = aggregate.grouping();
    return grouping.length == 1 ? grouping[0] : -1;
  }

  /** Native code for the aggregate, or -1 if unsupported. Mirrors the kinds in {@code Native}. */
  private static int aggregateKind(SqlKind kind) {
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
        return 4;
      default:
        return -1;
    }
  }

  static int aggregateKind(StreamPhysicalWindowAggregate aggregate) {
    return aggregateKind(aggregate.aggCalls().apply(0).getAggregation().getKind());
  }

  static long windowMillis(StreamPhysicalWindowAggregate aggregate) {
    return ((TumblingWindowSpec) aggregate.windowing().getWindow()).getSize().toMillis();
  }

  static int timeColumn(StreamPhysicalWindowAggregate aggregate) {
    return ((TimeAttributeWindowingStrategy) aggregate.windowing()).getTimeAttributeIndex();
  }

  static int valueColumn(StreamPhysicalWindowAggregate aggregate) {
    return aggregate.aggCalls().apply(0).getArgList().get(0);
  }
}
