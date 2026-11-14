package tech.streamfusion.planner.stream;

import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Window;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory$;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalOverAggregate;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalRel;
import org.apache.flink.table.planner.plan.utils.ChangelogPlanUtils;
import tech.streamfusion.planner.ExpressionPlanning;

/**
 * Recognizes one ascending time order and window group over supported partition keys. Numeric
 * aggregates and COUNT(*) run over bounded or unbounded ROWS/RANGE frames ending at CURRENT ROW;
 * bounded RANGE requires event time. Ranking functions require an unbounded frame.
 */
final class OverAggregateMatcher {

  private OverAggregateMatcher() {}

  static boolean matches(StreamPhysicalOverAggregate over) {
    return unsupportedReason(over) == null;
  }

  /** The specific reason this OVER is not accelerable, or null if it is. */
  static String unsupportedReason(StreamPhysicalOverAggregate over) {
    if (!ChangelogPlanUtils.isInsertOnly((StreamPhysicalRel) over.getInput())) {
      return "OVER: requires an insert-only input";
    }
    var frame = windowSpec(over);
    if (!frame.isAccepted()) return frame.reason();
    Window window = over.logicWindow();
    Window.Group group = window.groups.get(0);
    if (!allWindowFunctions(group) && valueColumns(window, group, over.getInput().getRowType()) == null)
      return "OVER: expected COUNT(*) or one supported argument per aggregate; direct AVG requires DECIMAL";
    return null;
  }

  private static tech.streamfusion.planner.common.Admission<OverWindowSpec> windowSpec(
      StreamPhysicalOverAggregate over) {
    Window window = over.logicWindow();
    boolean ranking = window.groups.size() == 1 && allWindowFunctions(window.groups.get(0));
    return OverWindowSpec.physical(window, over.getInput().getRowType(), ranking);
  }

  static int frameKind(StreamPhysicalOverAggregate over) {
    return windowSpec(over).value().frameKind();
  }

  static long frameOffset(StreamPhysicalOverAggregate over) {
    return windowSpec(over).value().offset();
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


  private static int[] valueColumns(Window window, Window.Group group, RelDataType inputType) {
    List<Window.RexWinAggCall> aggCalls = group.aggCalls;
    if (aggCalls.isEmpty()) {
      return null;
    }
    int[] columns = new int[aggCalls.size()];
    for (int a = 0; a < aggCalls.size(); a++) {
      RexCall call = aggCalls.get(a);
      int kind = overKind(call.getOperator().getKind());
      if (call.getKind() == SqlKind.COUNT && call.getOperands().isEmpty()) {
        columns[a] = inputType.getFieldCount() + window.constants.size();
        continue;
      }
      if (call.getOperands().size() != 1
          || !(call.getOperands().get(0) instanceof RexInputRef)
          || kind < 0) {
        return null;
      }
      int index = ((RexInputRef) call.getOperands().get(0)).getIndex();
      RelDataType valueType;
      if (index < inputType.getFieldCount()) {
        valueType = inputType.getFieldList().get(index).getType();
      } else {
        int constant = index - inputType.getFieldCount();
        if (constant < 0 || constant >= window.constants.size()) {
          return null;
        }
        RexLiteral literal = window.constants.get(constant);
        if (!literal.isNull()
            && !supportedValueType(
                literal.getType().getSqlTypeName(), kind, aggCalls.get(a).distinct)) {
          return null;
        }
        valueType = literal.getType();
      }
      if ((kind == 5 || kind == 6)
          && (GroupAggregateMatcher.isTimestampType(valueType.getSqlTypeName())
              || valueType.getSqlTypeName() == SqlTypeName.DATE
              || valueType.getSqlTypeName() == SqlTypeName.TIME)
          && !tech.streamfusion.compat.RuntimeCompat.supportsTemporalOverFirstLast()) {
        return null;
      }
      if (valueType.getSqlTypeName() == SqlTypeName.CHAR
          && (kind == 1 || kind == 2 || kind == 5 || kind == 6)
          && !tech.streamfusion.compat.RuntimeCompat.supportsCharOverValueSelection(
              call.getKind().name(), !group.lowerBound.isUnbounded())) {
        return null;
      }
      if (kind == WindowAggregateMatcher.KIND_AVG
          && valueType.getSqlTypeName() != SqlTypeName.DECIMAL) {
        return null;
      }
      if (valueType.getSqlTypeName() == SqlTypeName.DECIMAL
          && valueType.getScale() == 38
          && (kind == WindowAggregateMatcher.KIND_SUM
              || kind == WindowAggregateMatcher.KIND_SUM_ZERO
              || kind == WindowAggregateMatcher.KIND_AVG)) {
        return null;
      }
      if (!supportedValueType(valueType.getSqlTypeName(), kind, aggCalls.get(a).distinct)
          && !(index >= inputType.getFieldCount()
              && window.constants.get(index - inputType.getFieldCount()).isNull())) {
        return null;
      }
      columns[a] = index;
    }
    return columns;
  }

  static int timeColumn(StreamPhysicalOverAggregate over) {
    return OverWindowSpec.ascendingOrderColumn(over.logicWindow().groups.get(0));
  }

  /** Whether the OVER orders by processing time (arrival order) rather than a rowtime. */
  static boolean isProctime(StreamPhysicalOverAggregate over) {
    Integer order = OverWindowSpec.ascendingOrderColumn(over.logicWindow().groups.get(0));
    return order != null
        && FlinkTypeFactory$.MODULE$.isProctimeIndicatorType(
            over.getInput().getRowType().getFieldList().get(order).getType());
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
    return valueColumns(over.logicWindow(), group, over.getInput().getRowType());
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

  private static boolean supportedValueType(SqlTypeName type, int kind, boolean distinct) {
    if (supportedValueType(type)) return true;
    if (type == SqlTypeName.VARBINARY
        || type.getFamily() == SqlTypeFamily.INTERVAL_DAY_TIME
        || type.getFamily() == SqlTypeFamily.INTERVAL_YEAR_MONTH) {
      return kind == WindowAggregateMatcher.KIND_COUNT && !distinct;
    }
    if (type == SqlTypeName.ARRAY || type == SqlTypeName.MAP || type == SqlTypeName.ROW
        || type == SqlTypeName.MULTISET) {
      return kind == WindowAggregateMatcher.KIND_COUNT && !distinct;
    }
    if (type == SqlTypeName.DATE || type == SqlTypeName.TIME || type == SqlTypeName.BOOLEAN) {
      if (distinct) return kind == WindowAggregateMatcher.KIND_COUNT;
      return kind == WindowAggregateMatcher.KIND_COUNT
          || kind == 1
          || kind == 2
          || kind == 5
          || kind == 6;
    }
    if (type == SqlTypeName.DECIMAL) {
      if (distinct) return kind == WindowAggregateMatcher.KIND_COUNT
          || kind == WindowAggregateMatcher.KIND_SUM
          || kind == WindowAggregateMatcher.KIND_SUM_ZERO
          || kind == WindowAggregateMatcher.KIND_AVG;
      return kind == WindowAggregateMatcher.KIND_SUM
          || kind == WindowAggregateMatcher.KIND_SUM_ZERO
          || kind == WindowAggregateMatcher.KIND_AVG
          || kind == WindowAggregateMatcher.KIND_COUNT
          || kind == 1
          || kind == 2
          || kind == 5
          || kind == 6;
    }
    if (type == SqlTypeName.CHAR
        || type == SqlTypeName.VARCHAR
        || type == SqlTypeName.TIMESTAMP
        || type == SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE) {
      if (distinct) return kind == WindowAggregateMatcher.KIND_COUNT;
      return kind == WindowAggregateMatcher.KIND_COUNT
          || kind == 1
          || kind == 2
          || kind == 5
          || kind == 6;
    }
    return false;
  }

  /** Shared native value codes, including decimal precision/scale and timestamp precision. */
  static int[] valueTypeCodes(StreamPhysicalOverAggregate over) {
    int[] columns = valueColumnIndices(over);
    RelDataType inputType = over.getInput().getRowType();
    int[] codes = new int[columns.length];
    for (int a = 0; a < columns.length; a++) {
      if (columns[a] == inputType.getFieldCount() + over.logicWindow().constants.size()) {
        codes[a] = 0; // The synthetic non-null BIGINT one for COUNT(*).
        continue;
      }
      RelDataType valueType =
          columns[a] < inputType.getFieldCount()
              ? inputType.getFieldList().get(columns[a]).getType()
              : over.logicWindow().constants.get(columns[a] - inputType.getFieldCount()).getType();
      codes[a] = WindowAggregateMatcher.windowValueTypeCode(valueType);
    }
    return codes;
  }

  static int[] kinds(StreamPhysicalOverAggregate over) {
    List<Window.RexWinAggCall> aggCalls = over.logicWindow().groups.get(0).aggCalls;
    int[] kinds = new int[aggCalls.size()];
    for (int i = 0; i < aggCalls.size(); i++) {
      int kind = kindCode(aggCalls.get(i).getOperator().getKind());
      kinds[i] = aggCalls.get(i).distinct ? kind + 100 : kind;
    }
    return kinds;
  }

  /**
   * Native code for an OVER output: a window-function code if it is one, else an aggregate code.
   */
  private static int kindCode(SqlKind kind) {
    int windowFunction = windowFunctionKind(kind);
    return windowFunction >= 0 ? windowFunction : overKind(kind);
  }

  /** Preserves the planner's zero-on-empty reduction independently of ordinary SUM. */
  private static int overKind(SqlKind kind) {
    if (kind == SqlKind.SUM0) {
      return WindowAggregateMatcher.KIND_SUM_ZERO;
    }
    if (kind == SqlKind.FIRST_VALUE) {
      return 5;
    }
    if (kind == SqlKind.LAST_VALUE) {
      return 6;
    }
    return WindowAggregateMatcher.aggregateKind(kind);
  }

  static RelNode substitute(StreamPhysicalOverAggregate over, PlanContext ctx) {
    OverWindowSpec frame = windowSpec(over).value();
    int[] keyColumns = frame.keyColumns();
    RelNode input = ctx.columnarInput(over.getInputs().get(0), keyColumns);
    int inputArity = input.getRowType().getFieldCount();
    List<RexLiteral> constants = new ArrayList<>(over.logicWindow().constants);
    if (over.logicWindow().groups.get(0).aggCalls.stream()
        .anyMatch(call -> call.getKind() == SqlKind.COUNT && call.getOperands().isEmpty())) {
      constants.add(over.getCluster().getRexBuilder().makeBigintLiteral(java.math.BigDecimal.ONE));
    }
    int constantCount = constants.size();
    RelDataType physicalInputType = input.getRowType();
    if (constantCount > 0) {
      org.apache.calcite.rel.type.RelDataTypeFactory.Builder typeBuilder =
          over.getCluster().getTypeFactory().builder();
      List<RexNode> projections = new ArrayList<>();
      List<String> names = new ArrayList<>();
      for (int i = 0; i < inputArity; i++) {
        typeBuilder.add(input.getRowType().getFieldList().get(i));
        projections.add(
            over.getCluster()
                .getRexBuilder()
                .makeInputRef(input.getRowType().getFieldList().get(i).getType(), i));
        names.add(input.getRowType().getFieldNames().get(i));
      }
      for (int i = 0; i < constantCount; i++) {
        RexLiteral constant = constants.get(i);
        String name = "$over_constant_" + i;
        typeBuilder.add(name, constant.getType());
        projections.add(constant);
        names.add(name);
      }
      physicalInputType = typeBuilder.build();
      input =
          new StreamPhysicalNativeCalc(
              over.getCluster(),
              over.getTraitSet(),
              input,
              physicalInputType,
              ExpressionPlanning.encodeProjections(projections, names));
    }
    RelDataType physicalOutputType = over.getRowType();
    if (constantCount > 0) {
      org.apache.calcite.rel.type.RelDataTypeFactory.Builder outputBuilder =
          over.getCluster().getTypeFactory().builder();
      physicalInputType.getFieldList().forEach(outputBuilder::add);
      for (int i = inputArity; i < over.getRowType().getFieldCount(); i++) {
        outputBuilder.add(over.getRowType().getFieldList().get(i));
      }
      physicalOutputType = outputBuilder.build();
    }
    // Always columnar: the keyed shuffle becomes a native exchange (split by the
    // partition keys); the transition pass transposes below it only when the producer is rowwise.
    RelNode nativeOver =
        new StreamPhysicalNativeOverAggregate(
            over.getCluster(),
            over.getTraitSet(),
            input,
            physicalOutputType,
            frame.timeColumn(),
            OverAggregateMatcher.valueColumnIndices(over),
            keyColumns,
            OverAggregateMatcher.valueTypeCodes(over),
            OverAggregateMatcher.kinds(over),
            frame.frameKind(),
            frame.offset(),
            frame.processingTime());
    if (constantCount == 0) {
      return nativeOver;
    }
    List<RexNode> outputProjections = new ArrayList<>();
    for (int i = 0; i < inputArity; i++) {
      outputProjections.add(
          over.getCluster()
              .getRexBuilder()
              .makeInputRef(physicalOutputType.getFieldList().get(i).getType(), i));
    }
    int aggregateCount = over.getRowType().getFieldCount() - inputArity;
    for (int i = 0; i < aggregateCount; i++) {
      int index = inputArity + constantCount + i;
      outputProjections.add(
          over.getCluster()
              .getRexBuilder()
              .makeInputRef(physicalOutputType.getFieldList().get(index).getType(), index));
    }
    return new StreamPhysicalNativeCalc(
        over.getCluster(),
        over.getTraitSet(),
        nativeOver,
        over.getRowType(),
        ExpressionPlanning.encodeProjections(outputProjections, over.getRowType().getFieldNames()));
  }
}
