package tech.streamfusion.planner.stream;

import java.util.List;
import java.util.function.Supplier;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.core.Window;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexWindowBound;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.plan.nodes.exec.spec.OverSpec;
import org.apache.flink.table.planner.plan.utils.OverAggregateUtil;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks;
import tech.streamfusion.planner.common.Admission;

/** Normalized time/order/frame contract shared by relational and stock execution-plan adapters. */
final class OverWindowSpec {
  private final int[] keys;
  private final int timeColumn;
  private final boolean processingTime;
  private final boolean rows;
  private final boolean bounded;
  private final long offset;
  private final int frameKind;

  private OverWindowSpec(int[] keys, int timeColumn, boolean processingTime,
      boolean rows, boolean bounded, long offset, boolean ranking) {
    this.keys = keys.clone();
    this.timeColumn = timeColumn;
    this.processingTime = processingTime;
    this.rows = rows;
    this.bounded = bounded;
    this.offset = offset;
    frameKind = ranking ? 0 : bounded ? rows ? 1 : 2 : rows ? 3 : 0;
  }

  static Admission<OverWindowSpec> physical(Window window, RelDataType input, boolean ranking) {
    if (window.groups.size() != 1) return Admission.declined("OVER: only a single window group");
    return physical(window.groups.get(0), window.constants, input, ranking);
  }

  static Admission<OverWindowSpec> physical(
      Window.Group group, List<RexLiteral> constants, RelDataType input, boolean ranking) {
    for (int key : group.keys.toArray())
      if (!WindowAggregateMatcher.supportedGroupingKeyType(input.getFieldList().get(key).getType().getSqlTypeName()))
        return Admission.declined("OVER: unsupported PARTITION BY key type for native scalar carriage");
    Integer order = ascendingOrderColumn(group);
    return create(group.keys.toArray(), FlinkTypeFactory.toLogicalRowType(input),
        order == null ? -1 : order, group.isRows, group.lowerBound, group.upperBound, ranking, false,
        () -> physicalOffset(group.lowerBound, constants, input.getFieldCount()));
  }

  static Admission<OverWindowSpec> compiled(OverSpec spec, RowType input) {
    if (spec.getGroups().size() != 1) return Admission.declined("OVER: only a single window group");
    var group = spec.getGroups().get(0);
    int[] order = group.getSort().getFieldIndices();
    boolean[] ascending = group.getSort().getAscendingOrders();
    int column = order.length == 1 && ascending[0] ? order[0] : -1;
    return create(spec.getPartition().getFieldIndices(), input, column, group.isRows(),
        group.getLowerBound(), group.getUpperBound(), false, true, () -> {
          Object bound = OverAggregateUtil.getBoundary(spec, group.getLowerBound());
          return bound instanceof Long && (long) bound <= 0 && (long) bound != Long.MIN_VALUE
              ? -(long) bound : null;
        });
  }

  private static Admission<OverWindowSpec> create(
      int[] keys, RowType input, int order, boolean rows, RexWindowBound lower, RexWindowBound upper,
      boolean ranking, boolean stockBoundary, Supplier<Long> offset) {
    if (!upper.isCurrentRow()) return Admission.declined("OVER: only frames ending at CURRENT ROW");
    if (order < 0 || order >= input.getFieldCount())
      return Admission.declined("OVER: requires exactly one ascending time order column");
    boolean processing = LogicalTypeChecks.isProctimeAttribute(input.getTypeAt(order));
    if (!processing && !LogicalTypeChecks.isRowtimeAttribute(input.getTypeAt(order)))
      return Admission.declined("OVER: requires exactly one ascending event-time or proctime order column");
    boolean bounded = !lower.isUnbounded();
    if (processing && !rows && bounded)
      return Admission.declined("OVER: a bounded RANGE frame over proctime is not supported (use ROWS or an event-time order)");
    if (ranking && !(lower.isUnbounded() && lower.isPreceding()))
      return Admission.declined("OVER: window functions need the UNBOUNDED PRECEDING .. CURRENT ROW frame");
    if ((!stockBoundary && !lower.isPreceding())
        || stockBoundary && bounded && !(lower.isPreceding() || lower.isCurrentRow()))
      return Admission.declined("OVER: frame must have a PRECEDING lower bound");
    Long width = bounded ? offset.get() : 0L;
    if (width == null)
      return Admission.declined(rows
          ? "OVER: ROWS lower bound must be UNBOUNDED PRECEDING or a constant preceding offset"
          : "OVER: bounded RANGE frame must be BETWEEN INTERVAL n PRECEDING AND CURRENT ROW");
    return Admission.accepted(new OverWindowSpec(keys, order, processing, rows, bounded, width, ranking));
  }

  private static Long physicalOffset(RexWindowBound bound, List<RexLiteral> constants, int inputFields) {
    RexNode offset = bound.getOffset();
    RexLiteral literal = offset instanceof RexLiteral ? (RexLiteral) offset : null;
    if (offset instanceof RexInputRef) {
      int constant = ((RexInputRef) offset).getIndex() - inputFields;
      if (constant >= 0 && constant < constants.size()) literal = constants.get(constant);
    }
    return literal == null ? null : literal.getValueAs(Long.class);
  }

  static Integer ascendingOrderColumn(Window.Group group) {
    List<RelFieldCollation> order = group.orderKeys.getFieldCollations();
    return order.size() == 1 && order.get(0).getDirection() == RelFieldCollation.Direction.ASCENDING
        ? order.get(0).getFieldIndex() : null;
  }

  int[] keyColumns() { return keys.clone(); }
  int timeColumn() { return timeColumn; }
  boolean processingTime() { return processingTime; }
  boolean rows() { return rows; }
  boolean bounded() { return bounded; }
  long offset() { return offset; }
  int frameKind() { return frameKind; }
}
