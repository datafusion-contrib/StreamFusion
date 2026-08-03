package tech.streamfusion.planner;

import java.time.Duration;
import org.apache.calcite.rel.RelNode;
import org.apache.flink.table.planner.plan.logical.LogicalWindow;
import org.apache.flink.table.planner.plan.logical.SessionGroupWindow;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalGroupWindowAggregate;
import org.apache.flink.table.runtime.groupwindow.NamedWindowProperty;
import org.apache.flink.table.runtime.groupwindow.ProctimeAttribute;
import org.apache.flink.table.runtime.groupwindow.RowtimeAttribute;
import org.apache.flink.table.runtime.groupwindow.WindowEnd;
import org.apache.flink.table.runtime.groupwindow.WindowStart;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks;
import scala.collection.Seq;

/**
 * Recognizes the legacy SESSION group-window aggregate — {@code GROUP BY k, SESSION(rowtime, INTERVAL
 * g)} (Flink's {@code StreamPhysicalGroupWindowAggregate}, distinct from the windowing-TVF window
 * aggregate) — and routes it to the native session window operator. The legacy operator's output
 * layout {@code [grouping keys, aggregates, window_start, window_end]} is exactly what the native
 * session operator emits, so it reuses that operator directly. Event-time only; the named window
 * properties must be exactly {@code (WindowStart, WindowEnd)} so the layout matches.
 */
final class GroupWindowSessionMatcher {

  private GroupWindowSessionMatcher() {}

  static boolean matches(StreamPhysicalGroupWindowAggregate agg) {
    LogicalWindow window = agg.window();
    if (!(window instanceof SessionGroupWindow)) {
      return false;
    }
    LogicalType timeType = window.timeAttribute().getOutputDataType().getLogicalType();
    // Event-time only (a proctime session takes a different timer path); the attribute must be a
    // rowtime timestamp the operator renders — local-zoned or a plain TIMESTAMP.
    if (LogicalTypeChecks.isProctimeAttribute(timeType) || !supportedTimeRoot(timeType)) {
      return false;
    }
    // The output must be [grouping keys, aggregates, window_start, window_end] — the native layout.
    if (!startEndProperties(agg.namedWindowProperties())) {
      return false;
    }
    return WindowAggregateMatcher.supportedAggregates(
        agg.grouping(), agg.aggCalls(), agg.getInput().getRowType());
  }

  static long gapMillis(StreamPhysicalGroupWindowAggregate agg) {
    return ((SessionGroupWindow) agg.window())
        .gap()
        .getValueAs(Duration.class)
        .orElseThrow(() -> new IllegalStateException("session gap is not a duration"))
        .toMillis();
  }

  static int timeColumn(StreamPhysicalGroupWindowAggregate agg) {
    return agg.window().timeAttribute().getFieldIndex();
  }

  static boolean isLtz(StreamPhysicalGroupWindowAggregate agg) {
    return agg.window().timeAttribute().getOutputDataType().getLogicalType().getTypeRoot()
        == LogicalTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE;
  }

  private static boolean supportedTimeRoot(LogicalType type) {
    LogicalTypeRoot root = type.getTypeRoot();
    return root == LogicalTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE
        || root == LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE;
  }

  /**
   * The output's window properties must be {@code window_start, window_end} optionally followed by a
   * rowtime then a proctime attribute — the order the native operator emits them (window_start,
   * window_end, then window_end-1 for rowtime and a proctime marker). Flink appends all four to a
   * group-window's output even when only some are selected (a Calc above projects the rest away).
   */
  private static boolean startEndProperties(Seq<NamedWindowProperty> properties) {
    int n = properties.size();
    if (n < 2 || n > 4) {
      return false;
    }
    if (!(properties.apply(0).getProperty() instanceof WindowStart)
        || !(properties.apply(1).getProperty() instanceof WindowEnd)) {
      return false;
    }
    if (n >= 3 && !(properties.apply(2).getProperty() instanceof RowtimeAttribute)) {
      return false;
    }
    return n < 4 || properties.apply(3).getProperty() instanceof ProctimeAttribute;
  }

  static String unsupportedReason(StreamPhysicalGroupWindowAggregate agg) {
    return "legacy session group-window: needs an event-time SESSION(...) over a local-time-zone or"
        + " plain TIMESTAMP rowtime, SUM/MIN/MAX/COUNT/AVG aggregates, and exactly (window_start,"
        + " window_end) as the window properties";
  }

  static RelNode substitute(StreamPhysicalGroupWindowAggregate agg, PlanContext ctx) {
    int[] keyColumns = WindowAggregateMatcher.keyColumns(agg.grouping());
    return new StreamPhysicalNativeColumnarSessionWindowAggregate(
        agg.getCluster(),
        agg.getTraitSet(),
        ctx.columnarInput(agg.getInputs().get(0), keyColumns),
        agg.getRowType(),
        GroupWindowSessionMatcher.gapMillis(agg),
        GroupWindowSessionMatcher.timeColumn(agg),
        WindowAggregateMatcher.valueColumns(agg.aggCalls()),
        keyColumns,
        WindowAggregateMatcher.valueTypeCodes(agg.aggCalls(), agg.getInput().getRowType()),
        WindowAggregateMatcher.kinds(agg.aggCalls()),
        false, // event-time (proctime sessions are not on this path)
        GroupWindowSessionMatcher.isLtz(agg));
  }
}
