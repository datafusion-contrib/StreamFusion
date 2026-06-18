package io.github.jordepic.streamfusion.planner;

import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.flink.table.planner.plan.logical.TimeAttributeWindowingStrategy;
import org.apache.flink.table.planner.plan.logical.TumblingWindowSpec;
import org.apache.flink.table.planner.plan.logical.WindowingStrategy;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalWindowAggregate;

/**
 * Recognizes the one window aggregation the native operator implements: an event-time tumbling
 * window with a single {@code SUM} and no extra grouping keys. Matching only this shape keeps
 * results identical and lets every other windowing fall back to the host engine.
 */
final class WindowAggregateMatcher {

  private WindowAggregateMatcher() {}

  static boolean matches(StreamPhysicalWindowAggregate aggregate) {
    WindowingStrategy windowing = aggregate.windowing();
    if (!(windowing instanceof TimeAttributeWindowingStrategy) || !windowing.isRowtime()) {
      return false;
    }
    if (!(windowing.getWindow() instanceof TumblingWindowSpec)) {
      return false;
    }
    if (aggregate.grouping().length != 0 || aggregate.aggCalls().size() != 1) {
      return false;
    }
    AggregateCall call = aggregate.aggCalls().apply(0);
    return call.getAggregation().getKind() == SqlKind.SUM && call.getArgList().size() == 1;
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
