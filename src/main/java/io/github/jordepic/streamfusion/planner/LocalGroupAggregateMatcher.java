package io.github.jordepic.streamfusion.planner;

import io.github.jordepic.streamfusion.operator.RowDataArrowConverter;
import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory$;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalLocalGroupAggregate;
import scala.collection.Seq;

/**
 * Recognizes the local half of a two-phase non-windowed {@code GROUP BY}: a stateless per-batch
 * pre-aggregate that emits one partial row per key ({@code [grouping.., partial0..]}) for the
 * {@link GlobalGroupAggregateMatcher global half} to merge. Scope mirrors the single-phase
 * {@link GroupAggregateMatcher}: SUM/MIN/MAX/COUNT/AVG (no distinct) over bigint/int/double values,
 * with grouping keys the boundary carries.
 *
 * <p>The native local emits each aggregate's partial in its running type: {@code SUM/MIN/MAX} keep
 * the value's own type (Flink's SUM partial does not widen — verified against the planner), COUNT is
 * bigint, and an AVG contributes <em>two</em> partial columns — the widened running sum (bigint for
 * integer inputs, double for double, Flink's {@code AvgAggFunction}) and the bigint non-null count.
 * Partials are positional, so the accessors below expand an AVG into its two native states (a
 * widened-sum state plus a COUNT over the same column) and the type checks walk the output row with
 * a per-aggregate offset. The global reads each partial by its declared (Flink) type, so every
 * declared partial column must equal what the native side emits — anything else falls back cleanly
 * rather than mismatching at the boundary.
 */
final class LocalGroupAggregateMatcher {

  private LocalGroupAggregateMatcher() {}

  static boolean matches(StreamPhysicalLocalGroupAggregate agg) {
    RelDataType inputType = agg.getInput().getRowType();
    // The input row crosses into the native local; the partials never reach the host (they flow to
    // the native global), so only the input types must be carriable.
    if (!RowDataArrowConverter.supports(FlinkTypeFactory$.MODULE$.toLogicalRowType(inputType))) {
      return false;
    }
    int[] grouping = agg.grouping();
    for (int column : grouping) {
      if (!WindowAggregateMatcher.supportedGroupingKeyType(
          inputType.getFieldList().get(column).getType().getSqlTypeName())) {
        return false;
      }
    }
    RelDataType outputType = agg.getRowType(); // [grouping.., partial0..]
    Seq<AggregateCall> aggCalls = agg.aggCalls();
    int offset = grouping.length;
    for (int i = 0; i < aggCalls.size(); i++) {
      AggregateCall call = aggCalls.apply(i);
      if (call.isDistinct() || call.isApproximate() || call.filterArg >= 0) {
        return false;
      }
      int kind = WindowAggregateMatcher.aggregateKind(call.getAggregation().getKind());
      if (kind < 0 || call.getArgList().size() > 1) {
        return false; // SUM/MIN/MAX/COUNT/AVG only
      }
      if (kind == WindowAggregateMatcher.KIND_AVG) {
        // AVG spans two positional partials: the widened running sum, then the bigint count.
        if (call.getArgList().isEmpty() || offset + 1 >= outputType.getFieldCount()) {
          return false;
        }
        SqlTypeName valueType =
            inputType.getFieldList().get(call.getArgList().get(0)).getType().getSqlTypeName();
        SqlTypeName widened = widenedSumType(valueType);
        if (widened == null
            || outputType.getFieldList().get(offset).getType().getSqlTypeName() != widened
            || outputType.getFieldList().get(offset + 1).getType().getSqlTypeName()
                != SqlTypeName.BIGINT) {
          return false;
        }
        offset += 2;
        continue;
      }
      SqlTypeName partialType = outputType.getFieldList().get(offset).getType().getSqlTypeName();
      offset++;
      if (kind == WindowAggregateMatcher.KIND_COUNT) {
        // COUNT(*) (empty argList) or COUNT(col); the partial is the bigint running count either way.
        if (partialType != SqlTypeName.BIGINT) {
          return false;
        }
        continue;
      }
      // SUM/MIN/MAX read a typed running value; it must be a running type, and the partial Flink
      // declares must equal the value type, so the native emit and the global read agree.
      SqlTypeName valueType =
          inputType.getFieldList().get(call.getArgList().get(0)).getType().getSqlTypeName();
      if (!isRunningType(valueType) || partialType != valueType) {
        return false;
      }
    }
    return true;
  }

  /** The declared type of an AVG's widened sum partial, or null if the value type isn't admitted. */
  private static SqlTypeName widenedSumType(SqlTypeName valueType) {
    switch (valueType) {
      case BIGINT:
      case INTEGER:
        return SqlTypeName.BIGINT;
      case DOUBLE:
        return SqlTypeName.DOUBLE;
      default:
        return null;
    }
  }

  private static boolean isRunningType(SqlTypeName type) {
    return type == SqlTypeName.BIGINT
        || type == SqlTypeName.INTEGER
        || type == SqlTypeName.DOUBLE;
  }

  /** Expanded native kinds, one entry per partial column (an AVG is a widened sum plus a count). */
  static int[] kinds(StreamPhysicalLocalGroupAggregate agg) {
    List<Integer> kinds = new ArrayList<>();
    Seq<AggregateCall> aggCalls = agg.aggCalls();
    for (int i = 0; i < aggCalls.size(); i++) {
      int kind = WindowAggregateMatcher.aggregateKind(aggCalls.apply(i).getAggregation().getKind());
      if (kind == WindowAggregateMatcher.KIND_AVG) {
        kinds.add(WindowAggregateMatcher.KIND_AVG_PARTIAL_SUM);
        kinds.add(WindowAggregateMatcher.KIND_COUNT);
      } else {
        kinds.add(kind);
      }
    }
    return toArray(kinds);
  }

  /** Expanded value columns (an AVG's sum and count states both read its value column). */
  static int[] valueColumns(StreamPhysicalLocalGroupAggregate agg) {
    List<Integer> columns = new ArrayList<>();
    Seq<AggregateCall> aggCalls = agg.aggCalls();
    for (int i = 0; i < aggCalls.size(); i++) {
      AggregateCall call = aggCalls.apply(i);
      int kind = WindowAggregateMatcher.aggregateKind(call.getAggregation().getKind());
      int column = call.getArgList().isEmpty() ? -1 : call.getArgList().get(0);
      columns.add(column);
      if (kind == WindowAggregateMatcher.KIND_AVG) {
        columns.add(column);
      }
    }
    return toArray(columns);
  }

  /** Expanded value-type codes; an AVG's sum state is typed by its WIDENED partial type. */
  static int[] valueTypeCodes(StreamPhysicalLocalGroupAggregate agg) {
    RelDataType inputType = agg.getInput().getRowType();
    List<Integer> codes = new ArrayList<>();
    Seq<AggregateCall> aggCalls = agg.aggCalls();
    for (int i = 0; i < aggCalls.size(); i++) {
      AggregateCall call = aggCalls.apply(i);
      int kind = WindowAggregateMatcher.aggregateKind(call.getAggregation().getKind());
      if (kind == WindowAggregateMatcher.KIND_AVG) {
        SqlTypeName valueType =
            inputType.getFieldList().get(call.getArgList().get(0)).getType().getSqlTypeName();
        codes.add(valueType == SqlTypeName.DOUBLE ? 1 : 0); // the widened sum: double or bigint
        codes.add(0); // the bigint count
      } else if (call.getArgList().isEmpty()) {
        codes.add(0);
      } else {
        SqlTypeName valueType =
            inputType.getFieldList().get(call.getArgList().get(0)).getType().getSqlTypeName();
        codes.add(valueType == SqlTypeName.DOUBLE ? 1 : valueType == SqlTypeName.INTEGER ? 2 : 0);
      }
    }
    return toArray(codes);
  }

  static int[] keyColumns(StreamPhysicalLocalGroupAggregate agg) {
    return WindowAggregateMatcher.keyColumns(agg.grouping());
  }

  private static int[] toArray(List<Integer> values) {
    int[] array = new int[values.size()];
    for (int i = 0; i < array.length; i++) {
      array[i] = values.get(i);
    }
    return array;
  }
}
