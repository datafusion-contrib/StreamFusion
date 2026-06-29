package io.github.jordepic.streamfusion.planner;

import java.util.List;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.core.Window;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexWindowBound;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory$;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalOverAggregate;

/**
 * Recognizes the event-time {@code OVER} aggregations the native operator implements: a single
 * window group ordered by a rowtime (local-time-zone) attribute, optional {@code PARTITION BY} on
 * bigint/int/string/boolean/date/timestamp/decimal keys, and one or more {@code SUM}/{@code MIN}/{@code
 * MAX}/{@code COUNT}/{@code FIRST_VALUE}/{@code LAST_VALUE} aggregates that each read a (possibly
 * different) bigint/int/smallint/tinyint/double/float value column, over one of three frames ending at the current row: {@code RANGE
 * UNBOUNDED PRECEDING} (a persistent running fold), {@code ROWS BETWEEN n PRECEDING AND CURRENT ROW}
 * (recomputed over the row slice), or {@code RANGE BETWEEN INTERVAL n PRECEDING AND CURRENT ROW}
 * (recomputed over the rowtime interval). Anything else (proctime, AVG, multiple value columns, an
 * unsupported key type) falls back.
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
    if (!group.upperBound.isCurrentRow()) {
      return "OVER: only frames ending at CURRENT ROW";
    }
    Integer order = orderColumn(group);
    if (order == null
        || !FlinkTypeFactory$.MODULE$.isRowtimeIndicatorType(
            inputType.getFieldList().get(order).getType())) {
      return "OVER: requires exactly one ascending event-time (rowtime) order column";
    }
    // Window functions (ROW_NUMBER) are computed per partition with no value column over the default
    // ROWS UNBOUNDED PRECEDING frame.
    if (allWindowFunctions(group)) {
      if (group.lowerBound.isUnbounded() && group.lowerBound.isPreceding()) {
        return null;
      }
      return "OVER: window functions need the UNBOUNDED PRECEDING .. CURRENT ROW frame";
    }
    if (valueColumns(group, inputType) == null) {
      return "OVER: aggregates need a single bigint/int/double value-column argument each"
          + " (AVG and COUNT(*) not supported)";
    }
    // Every supported frame ends at CURRENT ROW with a preceding lower bound. ROWS must be a constant
    // n PRECEDING (recomputed over the row slice); RANGE may be UNBOUNDED PRECEDING (the running fold)
    // or a constant INTERVAL PRECEDING (recomputed over the rowtime interval).
    if (!group.lowerBound.isPreceding()) {
      return "OVER: frame must have a PRECEDING lower bound";
    }
    if (group.isRows) {
      if (group.lowerBound.isUnbounded()
          || boundOffset(group.lowerBound, window, inputType.getFieldCount()) == null) {
        return "OVER: ROWS frame must be BETWEEN n PRECEDING AND CURRENT ROW";
      }
    } else if (!group.lowerBound.isUnbounded()
        && boundOffset(group.lowerBound, window, inputType.getFieldCount()) == null) {
      return "OVER: bounded RANGE frame must be BETWEEN INTERVAL n PRECEDING AND CURRENT ROW";
    }
    return null;
  }

  /**
   * Frame shape code matching the native side: 0 = RANGE unbounded preceding (running fold), 1 =
   * bounded ROWS (n preceding rows), 2 = bounded RANGE (a preceding rowtime interval).
   */
  static int frameKind(StreamPhysicalOverAggregate over) {
    Window.Group group = over.logicWindow().groups.get(0);
    if (allWindowFunctions(group)) {
      return 0;
    }
    if (group.isRows) {
      return 1;
    }
    return group.lowerBound.isUnbounded() ? 0 : 2;
  }

  /** The bounded-frame offset: n preceding rows (ROWS) or the preceding interval in millis (RANGE). */
  static long frameOffset(StreamPhysicalOverAggregate over) {
    Window window = over.logicWindow();
    Window.Group group = window.groups.get(0);
    if (allWindowFunctions(group) || (!group.isRows && group.lowerBound.isUnbounded())) {
      return 0;
    }
    Long offset = boundOffset(group.lowerBound, window, over.getInput().getRowType().getFieldCount());
    return offset == null ? 0 : offset;
  }

  /**
   * The constant offset of a {@code n PRECEDING} bound, or null if it is not a numeric constant. The
   * planner hoists the literal into {@link Window#constants} and the bound references it as an input
   * ref past the input columns (index {@code inputFieldCount + constantIndex}).
   */
  private static Long boundOffset(RexWindowBound bound, Window window, int inputFieldCount) {
    RexNode offset = bound.getOffset();
    RexLiteral literal = null;
    if (offset instanceof RexLiteral) {
      literal = (RexLiteral) offset;
    } else if (offset instanceof RexInputRef) {
      int constant = ((RexInputRef) offset).getIndex() - inputFieldCount;
      if (constant >= 0 && constant < window.constants.size()) {
        literal = window.constants.get(constant);
      }
    }
    if (literal == null) {
      return null;
    }
    Number value = literal.getValueAs(Long.class);
    return value == null ? null : value.longValue();
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
   * One value-column index per aggregate if every aggregate is a supported kind over a single
   * bigint/int/double input column (each may read a different one), or null otherwise.
   */
  private static int[] valueColumns(Window.Group group, RelDataType inputType) {
    List<Window.RexWinAggCall> aggCalls = group.aggCalls;
    if (aggCalls.isEmpty()) {
      return null;
    }
    int[] columns = new int[aggCalls.size()];
    for (int a = 0; a < aggCalls.size(); a++) {
      RexCall call = aggCalls.get(a);
      int kind = overKind(call.getOperator().getKind());
      if (call.getOperands().size() != 1
          || !(call.getOperands().get(0) instanceof RexInputRef)
          || kind < 0
          || kind == WindowAggregateMatcher.KIND_AVG) {
        return null; // SUM/SUM0/MIN/MAX/COUNT only (no AVG), a single column argument each
      }
      int index = ((RexInputRef) call.getOperands().get(0)).getIndex();
      if (!supportedValueType(inputType.getFieldList().get(index).getType().getSqlTypeName())) {
        return null;
      }
      columns[a] = index;
    }
    return columns;
  }

  static int timeColumn(StreamPhysicalOverAggregate over) {
    return orderColumn(over.logicWindow().groups.get(0));
  }

  /** The PARTITION BY column indices (empty for no partition). */
  static int[] keyColumns(StreamPhysicalOverAggregate over) {
    return over.logicWindow().groups.get(0).keys.toArray();
  }

  /** One value-column index per aggregate, or empty for window-function OVER (e.g. ROW_NUMBER). */
  static int[] valueColumnIndices(StreamPhysicalOverAggregate over) {
    Window.Group group = over.logicWindow().groups.get(0);
    if (allWindowFunctions(group)) {
      return new int[0];
    }
    return valueColumns(group, over.getInput().getRowType());
  }

  /** The value types an OVER aggregate may read: bigint/int/smallint/tinyint and double/float. */
  private static boolean supportedValueType(SqlTypeName type) {
    return type == SqlTypeName.BIGINT
        || type == SqlTypeName.INTEGER
        || type == SqlTypeName.SMALLINT
        || type == SqlTypeName.TINYINT
        || type == SqlTypeName.DOUBLE
        || type == SqlTypeName.FLOAT;
  }

  /**
   * Value-type code per aggregate matching the native side: 0 = bigint, 1 = double, 2 = int, 4 =
   * smallint, 5 = tinyint, 6 = float.
   */
  static int[] valueTypeCodes(StreamPhysicalOverAggregate over) {
    int[] columns = valueColumnIndices(over);
    RelDataType inputType = over.getInput().getRowType();
    int[] codes = new int[columns.length];
    for (int a = 0; a < columns.length; a++) {
      switch (inputType.getFieldList().get(columns[a]).getType().getSqlTypeName()) {
        case DOUBLE:
          codes[a] = 1;
          break;
        case INTEGER:
          codes[a] = 2;
          break;
        case SMALLINT:
          codes[a] = 4;
          break;
        case TINYINT:
          codes[a] = 5;
          break;
        case FLOAT:
          codes[a] = 6;
          break;
        default:
          codes[a] = 0;
      }
    }
    return codes;
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
    if (kind == SqlKind.SUM0) {
      return 0;
    }
    if (kind == SqlKind.FIRST_VALUE) {
      return 5;
    }
    if (kind == SqlKind.LAST_VALUE) {
      return 6;
    }
    return WindowAggregateMatcher.aggregateKind(kind);
  }
}

