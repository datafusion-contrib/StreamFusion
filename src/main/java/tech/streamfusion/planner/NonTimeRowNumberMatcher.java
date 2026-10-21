package tech.streamfusion.planner;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Window;
import org.apache.calcite.rel.logical.LogicalCalc;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory$;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalOverAggregate;
import org.apache.flink.table.planner.utils.ShortcutUtils;
import tech.streamfusion.operator.RowDataArrowConverter;

/** The released Flink 2.2 non-time ROWS rank, with row-identity update pairs. */
final class NonTimeRowNumberMatcher {
  private NonTimeRowNumberMatcher() {}

  static boolean hasShape(StreamPhysicalOverAggregate over) {
    Window window = over.logicWindow();
    if (window.groups.size() != 1) return false;
    Window.Group group = window.groups.get(0);
    if (group.aggCalls.size() != 1
        || group.aggCalls.get(0).getKind() != SqlKind.ROW_NUMBER
        || group.orderKeys.getFieldCollations().size() != 1) return false;
    var orderType =
        over.getInput()
            .getRowType()
            .getFieldList()
            .get(group.orderKeys.getFieldCollations().get(0).getFieldIndex())
            .getType();
    return !FlinkTypeFactory$.MODULE$.isRowtimeIndicatorType(orderType)
        && !FlinkTypeFactory$.MODULE$.isProctimeIndicatorType(orderType);
  }

  static boolean matches(StreamPhysicalOverAggregate over) {
    return unsupportedReason(over) == null;
  }

  static String unsupportedReason(StreamPhysicalOverAggregate over) {
    if (!hasShape(over)) return "non-time ROW_NUMBER: requires one ROW_NUMBER group";
    if (!tech.streamfusion.compat.RuntimeCompat.supportsNonTimeOverRowNumber()) {
      return "non-time ROW_NUMBER: unavailable on this released Flink line";
    }
    Window.Group group = over.logicWindow().groups.get(0);
    var inputType = over.getInput().getRowType();
    var order = group.orderKeys.getFieldCollations().get(0);
    RelDataType orderType = inputType.getFieldList().get(order.getFieldIndex()).getType();
    if (order.direction != RelFieldCollation.Direction.ASCENDING
        || !supportedOrderType(orderType)) {
      return "non-time ROW_NUMBER: requires one ascending supported order column";
    }
    if (!group.isRows
        || !group.lowerBound.isUnbounded()
        || !group.lowerBound.isPreceding()
        || !group.upperBound.isCurrentRow()) {
      return "non-time ROW_NUMBER: requires ROWS UNBOUNDED PRECEDING through CURRENT ROW";
    }
    if (ShortcutUtils.unwrapTableConfig(over).getIdleStateRetention().toMillis() > 0) {
      return "non-time ROW_NUMBER: requires disabled idle-state TTL";
    }
    for (int key : group.keys.toArray()) {
      if (!WindowAggregateMatcher.supportedGroupingKeyType(
          inputType.getFieldList().get(key).getType().getSqlTypeName())) {
        return "non-time ROW_NUMBER: unsupported partition key type";
      }
    }
    if (!RowDataArrowConverter.supports(FlinkTypeFactory$.MODULE$.toLogicalRowType(inputType))
        || !RowDataArrowConverter.supports(
            FlinkTypeFactory$.MODULE$.toLogicalRowType(over.getRowType()))) {
      return "non-time ROW_NUMBER: unsupported Arrow row payload";
    }
    if (complexNullsLast(over) && rankPlan(over, null) == null) {
      return "non-time ROW_NUMBER: unsupported complex parent-validity projection";
    }
    return null;
  }

  private static boolean complexNullsLast(StreamPhysicalOverAggregate over) {
    var order = over.logicWindow().groups.get(0).orderKeys.getFieldCollations().get(0);
    var type = over.getInput().getRowType().getFieldList().get(order.getFieldIndex()).getType();
    return (type.isStruct() || type.getComponentType() != null)
        && order.nullDirection == RelFieldCollation.NullDirection.LAST;
  }

  private static boolean supportedOrderType(RelDataType type) {
    return supportedOrderType(type, true);
  }

  private static boolean supportedOrderType(RelDataType type, boolean allowFloating) {
    SqlTypeFamily family = type.getSqlTypeName().getFamily();
    if (family == SqlTypeFamily.INTERVAL_DAY_TIME || family == SqlTypeFamily.INTERVAL_YEAR_MONTH)
      return true;
    switch (type.getSqlTypeName()) {
      case ARRAY:
        return supportedOrderType(type.getComponentType(), false);
      case ROW:
        return type.getFieldList().stream().allMatch(field -> supportedOrderType(field.getType(), false));
      case FLOAT:
      case REAL:
      case DOUBLE:
        return allowFloating;
      case BINARY:
      case VARBINARY:
      case TINYINT:
      case SMALLINT:
      case INTEGER:
      case BIGINT:
      case DECIMAL:
      case TIMESTAMP:
      case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
      case CHAR:
      case VARCHAR:
      case DATE:
      case TIME:
      case BOOLEAN:
        return true;
      default:
        return false;
    }
  }

  static RelNode substitute(StreamPhysicalOverAggregate over, PlanContext ctx) {
    return Objects.requireNonNull(rankPlan(over, ctx));
  }

  private static RelNode rankPlan(StreamPhysicalOverAggregate over, PlanContext ctx) {
    Window.Group group = over.logicWindow().groups.get(0);
    var order = group.orderKeys.getFieldCollations().get(0);
    int[] partitions = group.keys.toArray();
    boolean parentNullFlag = complexNullsLast(over);
    RelNode input = ctx == null ? over.getInput() : ctx.columnarInput(over.getInput(), partitions);
    RelDataType outputType = over.getRowType();
    int arity = input.getRowType().getFieldCount();
    if (parentNullFlag) {
      // Flink always orders nested NULLs first; only the parent follows NULLS LAST.
      // Sort a derived parent flag first, then keep Arrow's nested NULLS FIRST encoding.
      var builder = over.getCluster().getRexBuilder();
      var fields = new ArrayList<RexNode>();
      for (int i = 0; i < arity; i++) fields.add(builder.makeInputRef(input, i));
      var flag =
          builder.makeCall(
              SqlStdOperatorTable.IS_NULL, builder.makeInputRef(input, order.getFieldIndex()));
      fields.add(flag);
      var typeBuilder =
          over.getCluster().getTypeFactory().builder().addAll(input.getRowType().getFieldList());
      typeBuilder.add("$rank_parent_null$", flag.getType());
      input = projection(input, fields, typeBuilder.build());
      if (input == null) return null;
      var rankedType =
          over.getCluster().getTypeFactory().builder().addAll(input.getRowType().getFieldList());
      rankedType.add(over.getRowType().getFieldList().get(arity));
      outputType = rankedType.build();
    }
    RelNode ranked =
        new StreamPhysicalNativeColumnarTopN(
            over.getCluster(),
            over.getTraitSet(),
            input,
            outputType,
            partitions,
            parentNullFlag
                ? new int[] {arity, order.getFieldIndex()}
                : new int[] {order.getFieldIndex()},
            parentNullFlag ? new int[] {1, 1} : new int[] {1},
            parentNullFlag
                ? new int[] {1, 1}
                : new int[] {order.nullDirection == RelFieldCollation.NullDirection.FIRST ? 1 : 0},
            0,
            Long.MAX_VALUE,
            -1,
            true,
            false,
            null,
            true,
            false,
            true);
    if (!parentNullFlag) return ranked;
    var fields = new ArrayList<RexNode>();
    var builder = over.getCluster().getRexBuilder();
    for (int i = 0; i < arity; i++) fields.add(builder.makeInputRef(ranked, i));
    fields.add(builder.makeInputRef(ranked, arity + 1));
    return projection(ranked, fields, over.getRowType());
  }

  private static RelNode projection(RelNode input, List<RexNode> fields, RelDataType outputType) {
    var program =
        RexProgram.create(
            input.getRowType(), fields, null, outputType, input.getCluster().getRexBuilder());
    var calc = LogicalCalc.create(input, program);
    var encoded = RexExpression.encodeCalc(calc);
    return encoded == null
        ? null
        : new StreamPhysicalNativeCalc(
            input.getCluster(), input.getTraitSet(), input, outputType, encoded, program);
  }
}
