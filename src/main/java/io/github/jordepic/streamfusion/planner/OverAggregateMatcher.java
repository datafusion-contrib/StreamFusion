package io.github.jordepic.streamfusion.planner;

import java.util.List;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.core.Window;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory$;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalOverAggregate;

/**
 * Recognizes the event-time {@code OVER} aggregations the native operator implements: a single
 * window group ordered by a rowtime (local-time-zone) attribute, the default {@code RANGE BETWEEN
 * UNBOUNDED PRECEDING AND CURRENT ROW} frame, optional {@code PARTITION BY} on bigint/int/string/boolean/date/timestamp/decimal
 * keys, and one or more {@code SUM}/{@code MIN}/{@code MAX}/{@code COUNT} aggregates that all read
 * the same bigint/int/double value column. Anything else (ROWS frame, a bounded frame, proctime,
 * AVG, multiple value columns, an unsupported key type) falls back to the host.
 */
final class OverAggregateMatcher {

  private OverAggregateMatcher() {}

  static boolean matches(StreamPhysicalOverAggregate over) {
    return unsupportedReason(over) == null;
  }

  /** The specific reason this OVER is not accelerable, or null if it is. */
  static String unsupportedReason(StreamPhysicalOverAggregate over) {
    Window window = over.logicWindow();
    if (window.groups.size() != 1) {
      return "OVER: only a single window group";
    }
    Window.Group group = window.groups.get(0);
    RelDataType inputType = over.getInput().getRowType();
    for (int key : group.keys.toArray()) {
      if (!WindowAggregateMatcher.supportedGroupingKeyType(
          inputType.getFieldList().get(key).getType().getSqlTypeName())) {
        return "OVER: PARTITION BY only on bigint/int/string/boolean/date/timestamp/decimal keys";
      }
    }
    if (!(group.lowerBound.isUnbounded() && group.lowerBound.isPreceding())
        || !group.upperBound.isCurrentRow()) {
      return "OVER: only the UNBOUNDED PRECEDING .. CURRENT ROW frame";
    }
    Integer order = orderColumn(group);
    if (order == null
        || !FlinkTypeFactory$.MODULE$.isRowtimeIndicatorType(
            inputType.getFieldList().get(order).getType())) {
      return "OVER: requires exactly one ascending event-time (rowtime) order column";
    }
    // Window functions (ROW_NUMBER) are computed per partition with no value column and use a ROWS
    // frame; aggregates use a RANGE frame over a single shared value column.
    if (allWindowFunctions(group)) {
      return null;
    }
    if (group.isRows || valueColumn(group, inputType) == null) {
      return "OVER: aggregates need a RANGE frame over one shared bigint/int/double value column"
          + " (ROWS frame, AVG, or multiple value columns not supported)";
    }
    return null;
  }

  /** Whether every aggregate in the group is a supported non-aggregate window function. */
  private static boolean allWindowFunctions(Window.Group group) {
    for (Window.RexWinAggCall aggCall : group.aggCalls) {
      if (windowFunctionKind(aggCall.getOperator().getKind()) < 0) {
        return false;
      }
    }
    return !group.aggCalls.isEmpty();
  }

  /** Native code for a non-aggregate window function, or -1 if it is not one we implement. */
  private static int windowFunctionKind(SqlKind kind) {
    switch (kind) {
      case ROW_NUMBER:
        return 10;
      case RANK:
        return 11;
      case DENSE_RANK:
        return 12;
      default:
        return -1;
    }
  }

  /** The single ascending ORDER BY column index, or null if the order is not a lone ascending key. */
  private static Integer orderColumn(Window.Group group) {
    List<RelFieldCollation> collations = group.orderKeys.getFieldCollations();
    if (collations.size() != 1
        || collations.get(0).getDirection() != RelFieldCollation.Direction.ASCENDING) {
      return null;
    }
    return collations.get(0).getFieldIndex();
  }

  /**
   * The shared value column index if every aggregate is a supported kind over the same single
   * bigint/int/double input column, or null otherwise.
   */
  private static Integer valueColumn(Window.Group group, RelDataType inputType) {
    Integer column = null;
    for (Window.RexWinAggCall aggCall : group.aggCalls) {
      RexCall call = aggCall;
      int kind = overKind(call.getOperator().getKind());
      if (call.getOperands().size() != 1
          || !(call.getOperands().get(0) instanceof RexInputRef)
          || kind < 0
          || kind == WindowAggregateMatcher.KIND_AVG) {
        return null; // SUM/SUM0/MIN/MAX/COUNT only (no AVG yet), single column argument
      }
      RexNode operand = call.getOperands().get(0);
      int index = ((RexInputRef) operand).getIndex();
      if (column == null) {
        column = index;
      } else if (column != index) {
        return null; // all aggregates must read the same value column
      }
    }
    if (column == null) {
      return null;
    }
    SqlTypeName valueType = inputType.getFieldList().get(column).getType().getSqlTypeName();
    return valueType == SqlTypeName.BIGINT
            || valueType == SqlTypeName.INTEGER
            || valueType == SqlTypeName.DOUBLE
        ? column
        : null;
  }

  static int timeColumn(StreamPhysicalOverAggregate over) {
    return orderColumn(over.logicWindow().groups.get(0));
  }

  /** The PARTITION BY column indices (empty for no partition). */
  static int[] keyColumns(StreamPhysicalOverAggregate over) {
    return over.logicWindow().groups.get(0).keys.toArray();
  }

  /** The shared value column index, or -1 for window-function OVER (e.g. ROW_NUMBER) with no argument. */
  static int valueColumnIndex(StreamPhysicalOverAggregate over) {
    Window.Group group = over.logicWindow().groups.get(0);
    if (allWindowFunctions(group)) {
      return -1;
    }
    return valueColumn(group, over.getInput().getRowType());
  }

  /** Value-type code matching the native side: 0 = bigint, 1 = double, 2 = int (0 when no value). */
  static int valueTypeCode(StreamPhysicalOverAggregate over) {
    int valueColumn = valueColumnIndex(over);
    if (valueColumn < 0) {
      return 0; // window functions ignore the value type
    }
    switch (over.getInput().getRowType().getFieldList().get(valueColumn).getType().getSqlTypeName()) {
      case DOUBLE:
        return 1;
      case INTEGER:
        return 2;
      default:
        return 0;
    }
  }

  static int[] kinds(StreamPhysicalOverAggregate over) {
    List<Window.RexWinAggCall> aggCalls = over.logicWindow().groups.get(0).aggCalls;
    int[] kinds = new int[aggCalls.size()];
    for (int i = 0; i < aggCalls.size(); i++) {
      kinds[i] = kindCode(aggCalls.get(i).getOperator().getKind());
    }
    return kinds;
  }

  /** Native code for an OVER output: a window-function code if it is one, else an aggregate code. */
  private static int kindCode(SqlKind kind) {
    int windowFunction = windowFunctionKind(kind);
    return windowFunction >= 0 ? windowFunction : overKind(kind);
  }

  /**
   * Native aggregate code, mapping Calcite's {@code $SUM0} (the planner decomposes user {@code SUM}
   * into {@code $SUM0} + {@code COUNT}) to the native sum: an OVER frame always contains the current
   * row, so it is never empty and {@code $SUM0} equals {@code SUM}.
   */
  private static int overKind(SqlKind kind) {
    return kind == SqlKind.SUM0 ? 0 : WindowAggregateMatcher.aggregateKind(kind);
  }
}

