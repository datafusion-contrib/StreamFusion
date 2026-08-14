package tech.streamfusion.planner;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataType;
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
 * Recognizes the global half of a two-phase aggregation: a slice-attached tumbling or hopping
 * window, no extra key or a single integer key, and one or more mergeable single-field aggregates
 * (SUM/MIN/MAX/COUNT over any single-phase-supported value type; no AVG, whose (sum, count) buffer
 * spans two partial columns). Its input is the local half's
 * {@code [key?, partial0..partialN-1, slice_end]}. For hopping, the planner appends a synthetic
 * {@code COUNT(*)} partial; it is merged like any count but excluded from the output (see
 * {@link #outputAggregateCount}).
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
    // Partials are positional ([grouping…, partial0..partialN-1, slice_end]); the i-th partial is
    // the i-th aggregate's, so the merge agg's own argList is not used to locate it. A COUNT merge
    // carries an empty argList for COUNT(*) and a single arg for COUNT(col); both sum the partial
    // counts via the count accumulator, so an empty argList is allowed only for COUNT.
    int base = aggregate.grouping().length;
    for (int i = 0; i < aggregate.aggCalls().size(); i++) {
      AggregateCall call = aggregate.aggCalls().apply(i);
      int kind = WindowAggregateMatcher.aggregateKind(call.getAggregation().getKind());
      // A DISTINCT merge dedups values the plain partial merge would double-count — fall back.
      if (call.isDistinct()) {
        return "global window aggregate: DISTINCT aggregates are not supported";
      }
      // Single-field mergeable partial only: sum (also count's merge), min, max.
      if (kind < 0 || kind == WindowAggregateMatcher.KIND_AVG || call.getArgList().size() > 1) {
        return "global window aggregate: only single-field SUM/MIN/MAX/COUNT partials (no AVG)";
      }
      if (call.getArgList().isEmpty() && kind != WindowAggregateMatcher.KIND_COUNT) {
        return "global window aggregate: only single-field SUM/MIN/MAX/COUNT partials (no AVG)";
      }
      // The partial column carries the aggregate's buffer type: the value type for SUM/MIN/MAX
      // (SUM's partial is Flink's nullable-sum buffer, DECIMAL(38, s) for a decimal SUM), bigint
      // for COUNT. Any single-phase-supported value type merges natively.
      SqlTypeName partialType = inputType.getFieldList().get(base + i).getType().getSqlTypeName();
      if (!WindowAggregateMatcher.supportedValueType(partialType)) {
        return "global window aggregate: unsupported partial column type " + partialType;
      }
    }
    return null;
  }

  /**
   * Value-type code per aggregate (matching the native side), recovered from each partial column —
   * the native global builds the same accumulator the local flushed, so the partial's own type
   * (bigint/double/int/narrow/float/decimal) selects it. Counts are bigint. The partials are
   * positional ({@code [grouping…, partial0..partialN-1, slice_end]}), so the i-th partial column
   * is the i-th aggregate's.
   */
  static int[] valueTypes(StreamPhysicalGlobalWindowAggregate aggregate) {
    RelDataType inputType = aggregate.getInput().getRowType();
    int base = aggregate.grouping().length;
    int[] types = new int[aggregate.aggCalls().size()];
    for (int i = 0; i < types.length; i++) {
      types[i] = WindowAggregateMatcher.typeCode(inputType.getFieldList().get(base + i).getType());
    }
    return types;
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
    // The global input is [grouping keys..., one partial per aggregate..., slice_end]; the partials
    // are positional. (The agg calls' argLists are not usable here — Flink expresses each merge
    // aggregate against a single intermediate ref, not the distinct partial columns.)
    int base = aggregate.grouping().length;
    int[] columns = new int[aggregate.aggCalls().size()];
    for (int i = 0; i < columns.length; i++) {
      columns[i] = base + i;
    }
    return columns;
  }

  static int sliceEndColumn(StreamPhysicalGlobalWindowAggregate aggregate) {
    return ((SliceAttachedWindowingStrategy) aggregate.windowing()).getSliceEnd();
  }

  static int[] kinds(StreamPhysicalGlobalWindowAggregate aggregate) {
    int[] kinds = new int[aggregate.aggCalls().size()];
    for (int i = 0; i < kinds.length; i++) {
      kinds[i] = WindowAggregateMatcher.aggregateKind(aggregate.aggCalls().apply(i).getAggregation().getKind());
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
