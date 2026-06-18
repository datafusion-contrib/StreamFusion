package io.github.jordepic.streamfusion.planner;

import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.flink.table.planner.plan.logical.SliceAttachedWindowingStrategy;
import org.apache.flink.table.planner.plan.logical.TumblingWindowSpec;
import org.apache.flink.table.planner.plan.logical.WindowingStrategy;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalGlobalWindowAggregate;

/**
 * Recognizes the global half of a two-phase tumbling aggregation: a slice-attached tumbling window,
 * no extra key or a single integer key, and one or more mergeable aggregates over bigint partial
 * columns (SUM/MIN/MAX — count merges as a sum; no AVG). Its input is the local half's
 * {@code [key?, partial0..partialN-1, slice_end]}.
 */
final class GlobalWindowAggregateMatcher {

  private GlobalWindowAggregateMatcher() {}

  static boolean matches(StreamPhysicalGlobalWindowAggregate aggregate) {
    WindowingStrategy windowing = aggregate.windowing();
    if (!(windowing instanceof SliceAttachedWindowingStrategy)) {
      return false;
    }
    if (!(windowing.getWindow() instanceof TumblingWindowSpec)) {
      return false;
    }
    int[] grouping = aggregate.grouping();
    RelDataType inputType = aggregate.getInput().getRowType();
    if (grouping.length > 1 || aggregate.aggCalls().isEmpty()) {
      return false;
    }
    if (grouping.length == 1
        && inputType.getFieldList().get(grouping[0]).getType().getSqlTypeName()
            != SqlTypeName.BIGINT) {
      return false;
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

  static long windowMillis(StreamPhysicalGlobalWindowAggregate aggregate) {
    return ((TumblingWindowSpec) aggregate.windowing().getWindow()).getSize().toMillis();
  }

  static int keyColumn(StreamPhysicalGlobalWindowAggregate aggregate) {
    int[] grouping = aggregate.grouping();
    return grouping.length == 1 ? grouping[0] : -1;
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
