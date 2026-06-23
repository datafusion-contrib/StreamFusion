package io.github.jordepic.streamfusion.planner;

import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.flink.table.planner.plan.logical.CumulativeWindowSpec;
import org.apache.flink.table.planner.plan.logical.HoppingWindowSpec;
import org.apache.flink.table.planner.plan.logical.SliceAttachedWindowingStrategy;
import org.apache.flink.table.planner.plan.logical.TumblingWindowSpec;
import org.apache.flink.table.planner.plan.logical.WindowSpec;
import org.apache.flink.table.planner.plan.logical.WindowingStrategy;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalGlobalWindowAggregate;

/**
 * Recognizes the global half of a two-phase aggregation: a slice-attached tumbling or hopping
 * window, no extra key or a single integer key, and one or more mergeable aggregates over bigint
 * partial columns (SUM/MIN/MAX — count merges as a sum; no AVG). Its input is the local half's
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
    if (!(windowing instanceof SliceAttachedWindowingStrategy)) {
      return "global window aggregate: requires a slice-attached windowing";
    }
    WindowSpec spec = windowing.getWindow();
    if (spec instanceof HoppingWindowSpec) {
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
    if (aggregate.aggCalls().isEmpty()) {
      return "global window aggregate: needs at least one aggregate";
    }
    for (int column : grouping) {
      if (!WindowAggregateMatcher.supportedKeyType(
          inputType.getFieldList().get(column).getType().getSqlTypeName())) {
        return "global window aggregate: grouping keys must be bigint/int/string";
      }
    }
    for (int i = 0; i < aggregate.aggCalls().size(); i++) {
      AggregateCall call = aggregate.aggCalls().apply(i);
      int kind = WindowAggregateMatcher.aggregateKind(call.getAggregation().getKind());
      // Single-field mergeable partial only: sum (also count's merge), min, max.
      if (call.getArgList().size() != 1 || kind < 0 || kind == WindowAggregateMatcher.KIND_AVG) {
        return "global window aggregate: only single-field SUM/MIN/MAX/COUNT partials (no AVG)";
      }
      SqlTypeName partialType =
          inputType.getFieldList().get(call.getArgList().get(0)).getType().getSqlTypeName();
      if (partialType != SqlTypeName.BIGINT && partialType != SqlTypeName.DOUBLE) {
        return "global window aggregate: partial columns must be bigint/double";
      }
    }
    return null;
  }

  /**
   * Value-type code (matching the native side) recovered from the partials: double if any partial
   * is a double (a sum/min/max over a double value), otherwise bigint. Count partials are bigint
   * either way, so they don't decide it.
   */
  static int valueType(StreamPhysicalGlobalWindowAggregate aggregate) {
    RelDataType inputType = aggregate.getInput().getRowType();
    for (int i = 0; i < aggregate.aggCalls().size(); i++) {
      int partial = aggregate.aggCalls().apply(i).getArgList().get(0);
      if (inputType.getFieldList().get(partial).getType().getSqlTypeName() == SqlTypeName.DOUBLE) {
        return 1;
      }
    }
    return 0;
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
}
