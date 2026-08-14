package tech.streamfusion.planner;

import tech.streamfusion.operator.RowDataArrowConverter;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.zone.ZoneOffsetTransition;
import java.time.zone.ZoneRules;
import org.apache.calcite.rel.RelNode;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory$;
import org.apache.flink.table.planner.plan.logical.LogicalWindow;
import org.apache.flink.table.planner.plan.logical.SessionGroupWindow;
import org.apache.flink.table.planner.plan.logical.SlidingGroupWindow;
import org.apache.flink.table.planner.plan.logical.TumblingGroupWindow;
import org.apache.flink.table.expressions.ValueLiteralExpression;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalGroupWindowAggregate;
import org.apache.flink.table.planner.plan.utils.AggregateUtil;
import org.apache.flink.table.planner.plan.utils.ChangelogPlanUtils;
import org.apache.flink.table.planner.utils.ShortcutUtils;
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
 * Routes supported legacy group-window aggregates onto the native window operators.
 *
 * <p>Flink deprecates every property POJO used by this physical node because the whole legacy Group
 * Window feature is deprecated. {@code StreamPhysicalGroupWindowAggregate} exposes no replacement
 * property API, so matching these types is the compatibility boundary this class exists to support.
 */
@SuppressWarnings("deprecation")
final class GroupWindowAggregateMatcher {

  private GroupWindowAggregateMatcher() {}

  static boolean matches(StreamPhysicalGroupWindowAggregate agg) {
    return unsupportedReason(agg) == null;
  }

  static String unsupportedReason(StreamPhysicalGroupWindowAggregate agg) {
    LogicalWindow window = agg.window();
    boolean session = window instanceof SessionGroupWindow;
    boolean fixed = window instanceof TumblingGroupWindow || window instanceof SlidingGroupWindow;
    if (!session && !fixed) {
      return "legacy group-window: only TUMBLE, HOP, and event-time SESSION are native";
    }

    LogicalType timeType = window.timeAttribute().getOutputDataType().getLogicalType();
    if (!supportedTimeRoot(timeType)) {
      return "legacy group-window: the time attribute must be TIMESTAMP or TIMESTAMP_LTZ";
    }
    boolean proctime = LogicalTypeChecks.isProctimeAttribute(timeType);
    if (session && proctime) {
      return "legacy group-window: processing-time SESSION is not native";
    }
    if (agg.emitStrategy().produceUpdates() || agg.emitStrategy().getAllowLateness() > 0) {
      return "legacy group-window: early/late firing and allowed lateness are not native";
    }
    if (!ChangelogPlanUtils.inputInsertOnly(agg)) {
      return "legacy group-window: retracting or updating input is not native";
    }
    if (!supportedProperties(agg.namedWindowProperties(), proctime)) {
      return "legacy group-window: unsupported window-property order";
    }
    if (!RowDataArrowConverter.supports(
            FlinkTypeFactory$.MODULE$.toLogicalRowType(agg.getInput().getRowType()))
        || !RowDataArrowConverter.supports(
            FlinkTypeFactory$.MODULE$.toLogicalRowType(agg.getRowType()))) {
      return "legacy group-window: an input or output column type the boundary cannot carry";
    }
    if (!WindowAggregateMatcher.supportedAggregates(
        agg.grouping(), agg.aggCalls(), agg.getInput().getRowType())) {
      return "legacy group-window: unsupported grouping key, aggregate, or aggregate value type";
    }

    if (session) {
      if (gap(agg) == null) {
        return "legacy group-window: session gap must be a time interval";
      }
      if (timeType.getTypeRoot() == LogicalTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE
          && !sessionZoneFixedAfterEpoch(agg)) {
        return "legacy SESSION over TIMESTAMP_LTZ requires the session zone to remain fixed after"
            + " 1970";
      }
      return null;
    }
    Duration size = size(agg);
    Duration slide = slide(agg);
    if (size == null || slide == null) {
      return "legacy group-window: window size and slide must be day-time intervals";
    }
    long sizeMillis = size.toMillis();
    long slideMillis = slide.toMillis();
    if (sizeMillis <= 0 || slideMillis <= 0) {
      return "legacy group-window: window size and slide must be positive";
    }
    if (proctime && sizeMillis % slideMillis != 0) {
      return "legacy group-window: processing-time HOP requires slide to divide size";
    }
    if (timeType.getTypeRoot() == LogicalTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE
        && !sessionZoneAlignsWithSlide(agg, slideMillis)) {
      return "legacy group-window: TIMESTAMP_LTZ requires every session-zone offset to align with"
          + " the window slide and the zone to remain fixed after 1970";
    }
    return null;
  }

  private static boolean sessionZoneAlignsWithSlide(
      StreamPhysicalGroupWindowAggregate agg, long slideMillis) {
    ZoneRules rules = ShortcutUtils.unwrapTableConfig(agg).getLocalTimeZone().getRules();
    return sessionZoneFixedAfterEpoch(rules)
        && offsetAligns(rules.getOffset(Instant.EPOCH), slideMillis);
  }

  private static boolean sessionZoneFixedAfterEpoch(StreamPhysicalGroupWindowAggregate agg) {
    return sessionZoneFixedAfterEpoch(
        ShortcutUtils.unwrapTableConfig(agg).getLocalTimeZone().getRules());
  }

  private static boolean sessionZoneFixedAfterEpoch(ZoneRules rules) {
    for (ZoneOffsetTransition transition : rules.getTransitions()) {
      if (transition.getInstant().isBefore(Instant.EPOCH)) {
        continue;
      }
      return false;
    }
    return rules.getTransitionRules().isEmpty();
  }

  private static boolean offsetAligns(ZoneOffset offset, long slideMillis) {
    return Math.floorMod(offset.getTotalSeconds() * 1000L, slideMillis) == 0;
  }

  private static boolean supportedTimeRoot(LogicalType type) {
    LogicalTypeRoot root = type.getTypeRoot();
    return root == LogicalTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE
        || root == LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE;
  }

  /** Flink emits no properties, or start/end followed by the time attributes in this order. */
  private static boolean supportedProperties(
      Seq<NamedWindowProperty> properties, boolean proctime) {
    int count = properties.size();
    if (count == 0) {
      return true;
    }
    int maximum = proctime ? 3 : 4;
    if (count < 2
        || count > maximum
        || !(properties.apply(0).getProperty() instanceof WindowStart)
        || !(properties.apply(1).getProperty() instanceof WindowEnd)) {
      return false;
    }
    if (count == 2) {
      return true;
    }
    if (proctime) {
      return properties.apply(2).getProperty() instanceof ProctimeAttribute;
    }
    if (!(properties.apply(2).getProperty() instanceof RowtimeAttribute)) {
      return false;
    }
    return count < 4 || properties.apply(3).getProperty() instanceof ProctimeAttribute;
  }

  private static Duration size(StreamPhysicalGroupWindowAggregate agg) {
    if (agg.window() instanceof TumblingGroupWindow tumblingGroupWindow) {
      return duration(tumblingGroupWindow.size());
    }
    return duration(((SlidingGroupWindow) agg.window()).size());
  }

  private static Duration slide(StreamPhysicalGroupWindowAggregate agg) {
    if (agg.window() instanceof TumblingGroupWindow) {
      return size(agg);
    }
    return duration(((SlidingGroupWindow) agg.window()).slide());
  }

  private static Duration duration(ValueLiteralExpression value) {
    return AggregateUtil.hasTimeIntervalType(value)
        ? value.getValueAs(Duration.class).orElse(null)
        : null;
  }

  private static Duration gap(StreamPhysicalGroupWindowAggregate agg) {
    return ((SessionGroupWindow) agg.window()).gap().getValueAs(Duration.class).orElse(null);
  }

  private static int timeColumn(StreamPhysicalGroupWindowAggregate agg) {
    return agg.window().timeAttribute().getFieldIndex();
  }

  private static boolean isProctime(StreamPhysicalGroupWindowAggregate agg) {
    return LogicalTypeChecks.isProctimeAttribute(
        agg.window().timeAttribute().getOutputDataType().getLogicalType());
  }

  private static boolean isLtz(StreamPhysicalGroupWindowAggregate agg) {
    return agg.window().timeAttribute().getOutputDataType().getLogicalType().getTypeRoot()
        == LogicalTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE;
  }

  static RelNode substitute(StreamPhysicalGroupWindowAggregate agg, PlanContext ctx) {
    int[] keyColumns = WindowAggregateMatcher.keyColumns(agg.grouping());
    if (agg.window() instanceof SessionGroupWindow) {
      return new StreamPhysicalNativeColumnarSessionWindowAggregate(
          agg.getCluster(),
          agg.getTraitSet(),
          ctx.columnarInput(agg.getInputs().get(0), keyColumns),
          agg.getRowType(),
          gap(agg).toMillis(),
          timeColumn(agg),
          WindowAggregateMatcher.valueColumns(agg.aggCalls()),
          keyColumns,
          WindowAggregateMatcher.valueTypeCodes(agg.aggCalls(), agg.getInput().getRowType()),
          WindowAggregateMatcher.kinds(agg.aggCalls()),
          false,
          isLtz(agg));
    }
    long windowMillis = size(agg).toMillis();
    return new StreamPhysicalNativeColumnarWindowAggregate(
        agg.getCluster(),
        agg.getTraitSet(),
        ctx.columnarInput(agg.getInputs().get(0), keyColumns),
        agg.getRowType(),
        false,
        windowMillis,
        slide(agg).toMillis(),
        timeColumn(agg),
        WindowAggregateMatcher.valueColumns(agg.aggCalls()),
        keyColumns,
        WindowAggregateMatcher.valueTypeCodes(agg.aggCalls(), agg.getInput().getRowType()),
        WindowAggregateMatcher.kinds(agg.aggCalls()),
        isProctime(agg),
        isLtz(agg));
  }
}
