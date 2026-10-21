package tech.streamfusion.planner;

import org.apache.calcite.rel.RelNode;
import org.apache.flink.table.planner.plan.logical.CumulativeWindowSpec;
import org.apache.flink.table.planner.plan.logical.HoppingWindowSpec;
import org.apache.flink.table.planner.plan.logical.TimeAttributeWindowingStrategy;
import org.apache.flink.table.planner.plan.logical.TumblingWindowSpec;
import org.apache.flink.table.planner.plan.logical.WindowSpec;
import org.apache.flink.table.planner.plan.logical.WindowingStrategy;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalWindowTableFunction;

/**
 * Recognizes the windowing table functions the native operator implements: an event-time
 * TUMBLE/HOP/CUMULATE over a timestamp rowtime, whose input columns the row/Arrow conversion can
 * all carry. The window assignment math is shared with the window aggregate ({@link
 * WindowAggregateMatcher#windowSize}/{@link WindowAggregateMatcher#windowSlide}/ {@link
 * WindowAggregateMatcher#isCumulative}/{@link WindowAggregateMatcher#timeColumn}), so a TVF feeding
 * a window join or aggregate assigns windows identically to a fused window aggregate.
 */
final class WindowTableFunctionMatcher {

  private WindowTableFunctionMatcher() {}

  static boolean matches(StreamPhysicalWindowTableFunction tvf) {
    WindowingStrategy windowing = tvf.windowing();
    if (!(windowing instanceof TimeAttributeWindowingStrategy)) {
      return false;
    }
    if (!WindowAggregateMatcher.supportedTimeAttribute(windowing)) {
      return false;
    }
    if (!WindowZoneGate.admits(tvf, windowing)) {
      return false;
    }
    return aligned(windowing.getWindow())
        && FilterCalcMatcher.convertibleRow(tvf.getInput().getRowType());
  }

  static boolean isProctime(StreamPhysicalWindowTableFunction tvf) {
    return tvf.windowing().isProctime();
  }

  private static boolean aligned(WindowSpec spec) {
    return spec instanceof TumblingWindowSpec
        || spec instanceof HoppingWindowSpec
        || spec instanceof CumulativeWindowSpec;
  }

  static int timeColumn(StreamPhysicalWindowTableFunction tvf) {
    return WindowAggregateMatcher.timeColumn(tvf.windowing());
  }

  static long windowMillis(StreamPhysicalWindowTableFunction tvf) {
    return WindowAggregateMatcher.windowSize(tvf.windowing());
  }

  static long slideMillis(StreamPhysicalWindowTableFunction tvf) {
    return WindowAggregateMatcher.windowSlide(tvf.windowing());
  }

  static boolean cumulative(StreamPhysicalWindowTableFunction tvf) {
    return WindowAggregateMatcher.isCumulative(tvf.windowing());
  }

  static String unsupportedReason(StreamPhysicalWindowTableFunction tvf) {
    String zoneReason = WindowZoneGate.unsupportedReason(tvf, tvf.windowing());
    if (zoneReason != null) {
      return "windowing table function: " + zoneReason;
    }
    return "windowing table function: needs an event-time TUMBLE/HOP/CUMULATE over a"
        + " TIMESTAMP or TIMESTAMP_LTZ rowtime, with input columns the Arrow conversion supports";
  }

  static RelNode substitute(StreamPhysicalWindowTableFunction tvf, PlanContext ctx) {
    return new StreamPhysicalNativeWindowTableFunction(
            tvf.getCluster(),
            tvf.getTraitSet(),
            tvf.getInputs().get(0),
            tvf.getRowType(),
            WindowTableFunctionMatcher.timeColumn(tvf),
            WindowTableFunctionMatcher.windowMillis(tvf),
            WindowTableFunctionMatcher.slideMillis(tvf),
            WindowTableFunctionMatcher.cumulative(tvf),
            WindowTableFunctionMatcher.isProctime(tvf),
            WindowZoneGate.boundaryOffsetMillis(tvf, tvf.windowing()))
        .withCalendarZone(WindowZoneGate.calendarZoneId(tvf, tvf.windowing()))
        .withAssignmentOffset(WindowAggregateMatcher.assignmentOffset(tvf.windowing()));
  }
}
