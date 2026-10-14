package tech.streamfusion.planner;

import java.time.Instant;
import org.apache.calcite.rel.RelNode;
import org.apache.flink.table.planner.plan.logical.WindowingStrategy;
import org.apache.flink.table.planner.utils.ShortcutUtils;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;

/** Shared admission and time-domain configuration for LTZ window assignment and consumers. */
final class WindowZoneGate {

  private WindowZoneGate() {}

  /**
   * Null when the windowing is admissible in this node's session zone, else the fallback reason.
   */
  static String unsupportedReason(RelNode node, WindowingStrategy windowing) {
    return null;
  }

  static String calendarZoneId(RelNode node, WindowingStrategy windowing) {
    var zone = ShortcutUtils.unwrapTableConfig(node).getLocalTimeZone();
    return isLtz(windowing.getTimeAttributeType()) && !zone.getRules().isFixedOffset()
        ? zone.getId()
        : null;
  }

  static boolean admits(RelNode node, WindowingStrategy windowing) {
    return unsupportedReason(node, windowing) == null;
  }

  static boolean isLtz(LogicalType timeAttributeType) {
    return timeAttributeType.getTypeRoot() == LogicalTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE;
  }

  static long boundaryOffsetMillis(RelNode node, WindowingStrategy windowing) {
    return isLtz(windowing.getTimeAttributeType())
        ? ShortcutUtils.unwrapTableConfig(node)
                .getLocalTimeZone()
                .getRules()
                .getOffset(Instant.EPOCH)
                .getTotalSeconds()
            * 1000L
        : 0;
  }
}
