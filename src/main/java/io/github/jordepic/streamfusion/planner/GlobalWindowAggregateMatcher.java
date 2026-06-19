package io.github.jordepic.streamfusion.planner;

import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeName;
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
    WindowingStrategy windowing = aggregate.windowing();
    if (!(windowing instanceof SliceAttachedWindowingStrategy)) {
      return false;
    }
    WindowSpec spec = windowing.getWindow();
    if (spec instanceof HoppingWindowSpec) {
      // The global fans each slice into size/slide windows, which requires slide to divide size.
      HoppingWindowSpec hop = (HoppingWindowSpec) spec;
      if (hop.getSize().toMillis() % hop.getSlide().toMillis() != 0) {
        return false;
      }
    } else if (!(spec instanceof TumblingWindowSpec)) {
      return false;
    }
    int[] grouping = aggregate.grouping();
    RelDataType inputType = aggregate.getInput().getRowType();
    if (aggregate.aggCalls().isEmpty()) {
      return false;
    }
    for (int column : grouping) {
      SqlTypeName type = inputType.getFieldList().get(column).getType().getSqlTypeName();
      if (type != SqlTypeName.BIGINT && type != SqlTypeName.INTEGER) {
        return false;
      }
    }
    for (int i = 0; i < aggregate.aggCalls().size(); i++) {
      AggregateCall call = aggregate.aggCalls().apply(i);
      int kind = WindowAggregateMatcher.aggregateKind(call.getAggregation().getKind());
      // Single-field mergeable partial only: sum (also count's merge), min, max.
      if (call.getArgList().size() != 1 || kind < 0 || kind == WindowAggregateMatcher.KIND_AVG) {
        return false;
      }
      if (inputType.getFieldList().get(call.getArgList().get(0)).getType().getSqlTypeName()
          != SqlTypeName.BIGINT) {
        return false;
      }
    }
    return true;
  }

  /** The full window size in millis (the hopping window's size, not the slice/slide). */
  static long windowMillis(StreamPhysicalGlobalWindowAggregate aggregate) {
    WindowSpec spec = aggregate.windowing().getWindow();
    return spec instanceof HoppingWindowSpec
        ? ((HoppingWindowSpec) spec).getSize().toMillis()
        : ((TumblingWindowSpec) spec).getSize().toMillis();
  }

  /** The slide in millis: equal to the size for tumbling, the real slide for hopping. */
  static long slideMillis(StreamPhysicalGlobalWindowAggregate aggregate) {
    WindowSpec spec = aggregate.windowing().getWindow();
    return spec instanceof HoppingWindowSpec
        ? ((HoppingWindowSpec) spec).getSlide().toMillis()
        : ((TumblingWindowSpec) spec).getSize().toMillis();
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
