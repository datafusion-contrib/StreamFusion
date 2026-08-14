package tech.streamfusion.planner;

import java.time.Instant;
import java.time.zone.ZoneOffsetTransition;
import java.time.zone.ZoneRules;
import org.apache.calcite.rel.RelNode;
import org.apache.flink.table.planner.plan.logical.CumulativeWindowSpec;
import org.apache.flink.table.planner.plan.logical.SessionWindowSpec;
import org.apache.flink.table.planner.plan.logical.WindowSpec;
import org.apache.flink.table.planner.plan.logical.WindowingStrategy;
import org.apache.flink.table.planner.utils.ShortcutUtils;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;

/**
 * Admission gate for windows over a TIMESTAMP_LTZ time attribute (event-time or proctime). Flink
 * shifts such timestamps onto the session-zone timeline before assigning, merging, and firing
 * windows, while the native operators work on the raw epoch timeline and only render boundaries
 * through the zone at the island edge. The two agree exactly when the zone keeps one fixed offset
 * after 1970 and, for fixed-grid windows, that offset is a whole number of grids — then the shifted
 * grid and the epoch grid are the same instants. A zone with later transitions (DST) moves records
 * relative to one another across a transition, changing fixed-grid membership and session
 * connectivity, so those decline to Flink.
 */
final class WindowZoneGate {

  private WindowZoneGate() {}

  /** Null when the windowing is admissible in this node's session zone, else the fallback reason. */
  static String unsupportedReason(RelNode node, WindowingStrategy windowing) {
    if (!isLtz(windowing.getTimeAttributeType())) {
      return null;
    }
    ZoneRules rules = ShortcutUtils.unwrapTableConfig(node).getLocalTimeZone().getRules();
    WindowSpec spec = windowing.getWindow();
    if (spec instanceof SessionWindowSpec) {
      return fixedAfterEpoch(rules)
          ? null
          : "TIMESTAMP_LTZ session windows require the session zone to remain fixed after 1970";
    }
    if (fixedAfterEpoch(rules) && offsetAligns(rules, gridMillis(spec, windowing))) {
      return null;
    }
    return "TIMESTAMP_LTZ windows require the session-zone offset to align with the window slide"
        + " (the max size for CUMULATE) and the zone to remain fixed after 1970";
  }

  static boolean admits(RelNode node, WindowingStrategy windowing) {
    return unsupportedReason(node, windowing) == null;
  }

  static boolean isLtz(LogicalType timeAttributeType) {
    return timeAttributeType.getTypeRoot() == LogicalTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE;
  }

  /**
   * The grid the shifted assignment must land on: cumulative windows start at multiples of the max
   * size (which the step divides), every other fixed shape at multiples of the slide.
   */
  private static long gridMillis(WindowSpec spec, WindowingStrategy windowing) {
    return spec instanceof CumulativeWindowSpec
        ? WindowAggregateMatcher.windowSize(windowing)
        : WindowAggregateMatcher.windowSlide(windowing);
  }

  static boolean fixedAfterEpoch(ZoneRules rules) {
    for (ZoneOffsetTransition transition : rules.getTransitions()) {
      if (!transition.getInstant().isBefore(Instant.EPOCH)) {
        return false;
      }
    }
    return rules.getTransitionRules().isEmpty();
  }

  static boolean offsetAligns(ZoneRules rules, long gridMillis) {
    return gridMillis > 0
        && Math.floorMod(rules.getOffset(Instant.EPOCH).getTotalSeconds() * 1000L, gridMillis) == 0;
  }
}
