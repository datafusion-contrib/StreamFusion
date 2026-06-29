package io.github.jordepic.streamfusion.planner;

import java.time.Duration;
import org.apache.flink.table.planner.plan.logical.CumulativeWindowSpec;
import org.apache.flink.table.planner.plan.logical.HoppingWindowSpec;
import org.apache.flink.table.planner.plan.logical.TimeAttributeWindowingStrategy;
import org.apache.flink.table.planner.plan.logical.TumblingWindowSpec;
import org.apache.flink.table.planner.plan.logical.WindowSpec;
import org.apache.flink.table.planner.plan.logical.WindowingStrategy;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalWindowTableFunction;
import org.apache.flink.table.types.logical.LogicalTypeRoot;

/**
 * Recognizes the windowing table functions the native operator implements: an event-time
 * TUMBLE/HOP/CUMULATE (cumulative only with a zero offset) over a local-time-zone rowtime, whose
 * input columns the row/Arrow conversion can all carry. The window assignment math is shared with the
 * window aggregate ({@link WindowAggregateMatcher#windowSize}/{@link WindowAggregateMatcher#windowSlide}/
 * {@link WindowAggregateMatcher#isCumulative}/{@link WindowAggregateMatcher#timeColumn}), so a TVF
 * feeding a window join or aggregate assigns windows identically to a fused window aggregate.
 */
final class WindowTableFunctionMatcher {

  private WindowTableFunctionMatcher() {}

  static boolean matches(StreamPhysicalWindowTableFunction tvf) {
    return matches(tvf.windowing(), tvf.getInput().getRowType());
  }

  private static boolean matches(
      WindowingStrategy windowing, org.apache.calcite.rel.type.RelDataType inputType) {
    if (!(windowing instanceof TimeAttributeWindowingStrategy)) {
      return false;
    }
    // Window bounds are emitted via the session zone, which matches the host only for a
    // local-time-zone time attribute (the same constraint as the window aggregate). Both event-time
    // (assign by rowtime) and proctime (assign by the clock) windowings are accepted; the downstream
    // window join/rank closes the windows on a watermark or a processing-time timer respectively.
    if (windowing.getTimeAttributeType().getTypeRoot()
        != LogicalTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE) {
      return false;
    }
    return aligned(windowing.getWindow()) && FilterCalcMatcher.convertibleRow(inputType);
  }

  static boolean isProctime(StreamPhysicalWindowTableFunction tvf) {
    return tvf.windowing().isProctime();
  }

  /** Tumbling/hopping are always aligned to the epoch; cumulative only with a zero offset. */
  private static boolean aligned(WindowSpec spec) {
    if (spec instanceof TumblingWindowSpec || spec instanceof HoppingWindowSpec) {
      return true;
    }
    if (spec instanceof CumulativeWindowSpec) {
      Duration offset = ((CumulativeWindowSpec) spec).getOffset();
      return offset == null || offset.isZero();
    }
    return false;
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
    return "windowing table function: needs an event-time TUMBLE/HOP/CUMULATE (zero offset) over a"
        + " local-time-zone rowtime, with input columns the Arrow conversion supports";
  }
}
