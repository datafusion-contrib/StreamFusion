package tech.streamfusion.planner;

import java.util.Arrays;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.flink.table.planner.plan.logical.CumulativeWindowSpec;
import org.apache.flink.table.planner.plan.logical.HoppingWindowSpec;
import org.apache.flink.table.planner.plan.logical.SliceAttachedWindowingStrategy;
import org.apache.flink.table.planner.plan.logical.TumblingWindowSpec;
import org.apache.flink.table.planner.plan.logical.WindowAttachedWindowingStrategy;
import org.apache.flink.table.planner.plan.logical.WindowSpec;
import org.apache.flink.table.planner.plan.logical.WindowingStrategy;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalGlobalWindowAggregate;

/**
 * Matches a global window merge over flattened local accumulator fields. SUM/MIN/MAX/COUNT use
 * one partial column; AVG uses a widened sum and count pair. Slice partials fan out into their
 * containing windows; attached-window partials merge into the one window they name.
 */
final class GlobalWindowAggregateMatcher {

  private GlobalWindowAggregateMatcher() {}

  static boolean matches(StreamPhysicalGlobalWindowAggregate aggregate) {
    return unsupportedReason(aggregate) == null;
  }

  /** The specific reason this global window aggregate is not accelerable, or null if it is. */
  static String unsupportedReason(StreamPhysicalGlobalWindowAggregate aggregate) {
    WindowingStrategy windowing = aggregate.windowing();
    // Slice-attached: the local emits per-slice partials the global fans into windows. Window-attached
    // (q5): the partials already correspond to full windows, so the global merges each into its one
    // window (no fan-out; see slideMillis) and the fan-out divide checks below do not apply.
    boolean attached = windowing instanceof WindowAttachedWindowingStrategy;
    if (!(windowing instanceof SliceAttachedWindowingStrategy) && !attached) {
      return "global window aggregate: requires a slice-attached or window-attached windowing";
    }
    // The global re-buckets slices (or merges attached windows) on the raw epoch grid, so it holds
    // the same session-zone requirement as the local that produced them — gating them together
    // keeps the pair on the same side of the fallback.
    String zoneReason = WindowZoneGate.unsupportedReason(aggregate, windowing);
    if (zoneReason != null) {
      return "global window aggregate: " + zoneReason;
    }
    WindowSpec spec = windowing.getWindow();
    if (attached) {
      // No fan-out to bound; only the aggregate/key shape (checked below) matters.
    } else if (spec instanceof HoppingWindowSpec) {
      // The global fans each slice into size/slide windows, which requires slide to divide size.
      HoppingWindowSpec hop = (HoppingWindowSpec) spec;
      if (hop.getSize().toMillis() % hop.getSlide().toMillis() != 0) {
        return "global window aggregate: HOP slide must divide size";
      }
    } else if (spec instanceof CumulativeWindowSpec) {
      // The global fans each slice into the nested windows whose end is a step multiple up to the
      // max size, so the step must divide the max size (Flink guarantees this for CUMULATE).
      CumulativeWindowSpec cum = (CumulativeWindowSpec) spec;
      if (cum.getMaxSize().toMillis() % cum.getStep().toMillis() != 0) {
        return "global window aggregate: CUMULATE step must divide max size";
      }
    } else if (!(spec instanceof TumblingWindowSpec)) {
      return "global window aggregate: only TUMBLE/HOP/CUMULATE windows";
    }
    int[] grouping = aggregate.grouping();
    RelDataType inputType = aggregate.getInput().getRowType();
    // An empty aggregate list is allowed: the global half of a grouping-only window (windowed
    // distinct) merges only slice ends and emits one row per (key, window).
    for (int column : grouping) {
      if (!WindowAggregateMatcher.supportedGroupingKeyType(
          inputType.getFieldList().get(column).getType().getSqlTypeName())) {
        return "global window aggregate: grouping keys must be bigint/int/string/boolean/date";
      }
    }
    boolean retracting = !WindowAggregateMatcher.insertOnlyInput(aggregate);
    if (retracting) {
      if (attached
          || !windowing.isRowtime()
          || !WindowAggregateMatcher.supportedRetractingAggregates(
              aggregate.aggCalls(), aggregate.inputRowTypeOfLocalAgg())) {
        return "global window aggregate: retracting input requires grouping-only,"
            + " numeric SUM/AVG or numeric COUNT";
      }
      int fields = WindowAggregateMatcher.partialFieldCount(aggregate.aggCalls(), true);
      int[] retractKinds = WindowAggregateMatcher.retractingKinds(aggregate.aggCalls());
      fields += retractKinds.length - aggregate.aggCalls().size();
      if (inputType.getFieldCount() != grouping.length + fields + 1) {
        return "global window aggregate: retracting accumulator partial layout differs from Flink";
      }
      int[] columns = partialColumns(aggregate);
      for (int i = 0; i < aggregate.aggCalls().size(); i++) {
        AggregateCall call = aggregate.aggCalls().apply(i);
        SqlTypeName partialType =
            call.getAggregation().getKind() == SqlKind.AVG
                ? switch (call.getType().getSqlTypeName()) {
                  case FLOAT, REAL, DOUBLE -> SqlTypeName.DOUBLE;
                  case DECIMAL -> SqlTypeName.DECIMAL;
                  default -> SqlTypeName.BIGINT;
                }
                : call.getType().getSqlTypeName();
        if (inputType.getFieldList().get(columns[i]).getType().getSqlTypeName() != partialType) {
          return "global window aggregate: retracting result partial has an unexpected type";
        }
        if (WindowAggregateMatcher.partialWidth(call, true) == 2
            && inputType.getFieldList().get(columns[i] + 1).getType().getSqlTypeName()
                != SqlTypeName.BIGINT) {
          return "global window aggregate: retracting SUM/AVG requires a BIGINT count partial";
        }
      }
      if (retractKinds.length > aggregate.aggCalls().size()
          && inputType.getFieldList().get(inputType.getFieldCount() - 2).getType().getSqlTypeName()
              != SqlTypeName.BIGINT) {
        return "global window aggregate: live-row count partial must be BIGINT";
      }
      return null;
    }
    // Partials are positional; AVG consumes two adjacent fields. The merge agg's own argList
    // does not locate those fields. A COUNT merge
    // carries an empty argList for COUNT(*) and a single arg for COUNT(col); both sum the partial
    // counts via the count accumulator, so an empty argList is allowed only for COUNT.
    int requiredFields = grouping.length + WindowAggregateMatcher.partialFieldCount(aggregate.aggCalls()) + 1;
    if (inputType.getFieldCount() < requiredFields) {
      return "global window aggregate: missing accumulator partial fields";
    }
    int[] partialColumns = partialColumns(aggregate);
    for (int i = 0; i < aggregate.aggCalls().size(); i++) {
      AggregateCall call = aggregate.aggCalls().apply(i);
      int kind = WindowAggregateMatcher.aggregateKind(call.getAggregation().getKind());
      if (call.isDistinct()) {
        RelDataType partial = inputType.getFieldList().get(partialColumns[i]).getType();
        if (kind != WindowAggregateMatcher.KIND_COUNT
            || partial.getSqlTypeName() != SqlTypeName.ARRAY
            || !WindowAggregateMatcher.supportedDistinctValueType(
                partial.getComponentType().getSqlTypeName())) {
          return "global window aggregate: COUNT(DISTINCT) requires native value-set partials";
        }
        continue;
      }
      // Every admitted aggregate reads one original value (or none for COUNT(*)).
      if (kind < 0 || call.getArgList().size() > 1) {
        return "global window aggregate: requires SUM/MIN/MAX/COUNT or paired AVG partials";
      }
      if (call.getArgList().isEmpty() && kind != WindowAggregateMatcher.KIND_COUNT) {
        return "global window aggregate: requires SUM/MIN/MAX/COUNT or paired AVG partials";
      }
      // The partial column carries the aggregate's buffer type: the value type for SUM/MIN/MAX
      // (SUM's partial is Flink's nullable-sum buffer, DECIMAL(38, s) for a decimal SUM), bigint
      // for COUNT. Any single-phase-supported value type merges natively.
      SqlTypeName partialType = inputType.getFieldList().get(partialColumns[i]).getType().getSqlTypeName();
      if (!WindowAggregateMatcher.supportedValueType(partialType)) {
        return "global window aggregate: unsupported partial column type " + partialType;
      }
      if (kind == WindowAggregateMatcher.KIND_AVG
          && inputType.getFieldList().get(partialColumns[i] + 1).getType().getSqlTypeName()
              != SqlTypeName.BIGINT) {
        return "global window aggregate: AVG count partial must be BIGINT";
      }
    }
    return null;
  }

  /** Value types select the accumulator, preserving AVG's result width and decimal sum scale. */
  static int[] valueTypes(StreamPhysicalGlobalWindowAggregate aggregate) {
    RelDataType inputType = aggregate.getInput().getRowType();
    int[] columns = partialColumns(aggregate);
    int[] types = new int[aggregate.aggCalls().size()];
    for (int i = 0; i < types.length; i++) {
      AggregateCall call = aggregate.aggCalls().apply(i);
      RelDataType partialType = inputType.getFieldList().get(columns[i]).getType();
      if (call.isDistinct()) {
        types[i] = WindowAggregateMatcher.windowValueTypeCode(partialType.getComponentType());
        continue;
      }
      // Integral and FLOAT AVG sums widen, but their result retains the original value type.
      // Decimal AVG instead needs the partial sum's original scale for its exact division.
      RelDataType valueType = WindowAggregateMatcher.partialWidth(call) == 2
              && partialType.getSqlTypeName() != SqlTypeName.DECIMAL
          ? call.getType() : partialType;
      types[i] = WindowAggregateMatcher.typeCode(valueType);
    }
    return Arrays.copyOf(types, kinds(aggregate).length);
  }

  /** Whether the global merges a cumulative window (nested windows sharing a bucket start). */
  static boolean cumulative(StreamPhysicalGlobalWindowAggregate aggregate) {
    return aggregate.windowing().getWindow() instanceof CumulativeWindowSpec;
  }

  /** The full window size in millis: hopping size, cumulative max size, or the tumbling size. */
  static long windowMillis(StreamPhysicalGlobalWindowAggregate aggregate) {
    WindowSpec spec = aggregate.windowing().getWindow();
    if (spec instanceof HoppingWindowSpec) {
      return ((HoppingWindowSpec) spec).getSize().toMillis();
    }
    if (spec instanceof CumulativeWindowSpec) {
      return ((CumulativeWindowSpec) spec).getMaxSize().toMillis();
    }
    return ((TumblingWindowSpec) spec).getSize().toMillis();
  }

  /** The slide in millis: the hopping slide, the cumulative step, or the size for tumbling. */
  static long slideMillis(StreamPhysicalGlobalWindowAggregate aggregate) {
    // Window-attached: each partial is already a full window, so the global must not fan out — a slide
    // equal to the size makes the merge place each partial into exactly one window.
    if (aggregate.windowing() instanceof WindowAttachedWindowingStrategy) {
      return windowMillis(aggregate);
    }
    WindowSpec spec = aggregate.windowing().getWindow();
    if (spec instanceof HoppingWindowSpec) {
      return ((HoppingWindowSpec) spec).getSlide().toMillis();
    }
    if (spec instanceof CumulativeWindowSpec) {
      return ((CumulativeWindowSpec) spec).getStep().toMillis();
    }
    return ((TumblingWindowSpec) spec).getSize().toMillis();
  }

  static int[] keyColumns(StreamPhysicalGlobalWindowAggregate aggregate) {
    return aggregate.grouping();
  }

  static int[] partialColumns(StreamPhysicalGlobalWindowAggregate aggregate) {
    // Each aggregate starts after all preceding state fields, including both fields of each AVG.
    int base = aggregate.grouping().length;
    int[] columns = new int[aggregate.aggCalls().size()];
    for (int i = 0; i < columns.length; i++) {
      columns[i] = base;
      base +=
          WindowAggregateMatcher.partialWidth(
              aggregate.aggCalls().apply(i), !WindowAggregateMatcher.insertOnlyInput(aggregate));
    }
    return columns;
  }

  static int sliceEndColumn(StreamPhysicalGlobalWindowAggregate aggregate) {
    return ((SliceAttachedWindowingStrategy) aggregate.windowing()).getSliceEnd();
  }

  static int[] kinds(StreamPhysicalGlobalWindowAggregate aggregate) {
    if (!WindowAggregateMatcher.insertOnlyInput(aggregate)) {
      return WindowAggregateMatcher.retractingKinds(aggregate.aggCalls());
    }
    int[] kinds = new int[aggregate.aggCalls().size()];
    for (int i = 0; i < kinds.length; i++) {
      AggregateCall call = aggregate.aggCalls().apply(i);
      kinds[i] =
          call.isDistinct()
              ? WindowAggregateMatcher.KIND_COUNT_DISTINCT
              : WindowAggregateMatcher.aggregateKind(call.getAggregation().getKind());
    }
    return kinds;
  }

  static RelNode substitute(StreamPhysicalGlobalWindowAggregate agg, PlanContext ctx) {
    int[] keyColumns = GlobalWindowAggregateMatcher.keyColumns(agg);
    // Always columnar: the columnar local emits Arrow partials, a native exchange
    // splits them by key, and the columnar global merges — the whole two-phase pipeline flows
    // Arrow. (columnarInput keeps the partial shuffle Arrow; the local is always a columnar
    // producer now, so no transpose arises here.)
    return new StreamPhysicalNativeColumnarGlobalWindowAggregate(
        agg.getCluster(),
        agg.getTraitSet(),
        ctx.columnarInput(agg.getInputs().get(0), keyColumns),
        agg.getRowType(),
        GlobalWindowAggregateMatcher.windowMillis(agg),
        GlobalWindowAggregateMatcher.slideMillis(agg),
        GlobalWindowAggregateMatcher.cumulative(agg),
        keyColumns,
        GlobalWindowAggregateMatcher.valueTypes(agg),
        GlobalWindowAggregateMatcher.kinds(agg),
        WindowAggregateMatcher.isLtz(agg.windowing()));
  }
}
