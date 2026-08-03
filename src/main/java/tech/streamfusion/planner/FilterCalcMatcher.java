package tech.streamfusion.planner;

import java.util.List;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.rex.RexUtil;

/**
 * Recognizes a filter the native operator can run: a {@link Calc} with an input-ref projection (a
 * column subset/reorder — general projections are separate) whose condition the native expression
 * engine can evaluate, over an input whose every column the whole-row converter handles. The
 * condition is encoded by {@link RexExpression}; anything it cannot encode — an unsupported
 * operation or operand — falls back, as do computed/constant projections and unsupported column
 * types.
 */
final class FilterCalcMatcher {

  private FilterCalcMatcher() {}

  static boolean matches(Calc calc) {
    RexProgram program = calc.getProgram();
    if (program.getCondition() == null || projection(calc) == null) {
      return false;
    }
    if (!convertibleRow(calc.getInput().getRowType())) {
      return false;
    }
    return encodedCondition(calc) != null;
  }

  /** The condition encoded for the native engine, or null if it contains an unsupported operation. */
  static RexExpression encodedCondition(Calc calc) {
    RexProgram program = calc.getProgram();
    RexNode condition = program.expandLocalRef(program.getCondition());
    // Calcite folds `col = literal`, IN, and ranges into a SEARCH/Sarg; expand it back into the
    // comparisons the native engine evaluates. The native side handles arbitrary AND/OR/NOT trees,
    // so no disjunctive-normal-form rewrite is needed.
    RexNode expanded = RexUtil.expandSearch(calc.getCluster().getRexBuilder(), null, condition);
    return RexExpression.encode(expanded);
  }

  /**
   * The output→input column mapping if every projected expression is a plain input column reference
   * (a subset/reorder), or null if any projection is a computed expression or constant.
   */
  static int[] projection(Calc calc) {
    RexProgram program = calc.getProgram();
    List<RexLocalRef> projects = program.getProjectList();
    int[] columns = new int[projects.size()];
    for (int i = 0; i < projects.size(); i++) {
      RexNode expr = program.expandLocalRef(projects.get(i));
      if (!(expr instanceof RexInputRef)) {
        return null;
      }
      columns[i] = ((RexInputRef) expr).getIndex();
    }
    return columns;
  }

  /**
   * Whether every input column has a type the Arrow boundary can carry. Nested ARRAY/MAP/ROW values
   * are admitted recursively because the boundary preserves their complete binary representation.
   */
  static boolean convertibleRow(RelDataType inputType) {
    return inputType.getFieldList().stream().allMatch(f -> isConvertibleType(f.getType()));
  }

  private static boolean isConvertibleType(RelDataType type) {
    switch (type.getSqlTypeName()) {
      case TINYINT:
      case SMALLINT:
      case INTEGER:
      case BIGINT:
      case FLOAT:
      case DOUBLE:
      case BOOLEAN:
      case CHAR:
      case VARCHAR:
      case BINARY:
      case VARBINARY:
      case TIMESTAMP:
      case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
      case DATE:
      case TIME:
      case INTERVAL_YEAR:
      case INTERVAL_YEAR_MONTH:
      case INTERVAL_MONTH:
      case INTERVAL_DAY:
      case INTERVAL_DAY_HOUR:
      case INTERVAL_DAY_MINUTE:
      case INTERVAL_DAY_SECOND:
      case INTERVAL_HOUR:
      case INTERVAL_HOUR_MINUTE:
      case INTERVAL_HOUR_SECOND:
      case INTERVAL_MINUTE:
      case INTERVAL_MINUTE_SECOND:
      case INTERVAL_SECOND:
      case DECIMAL:
        return true;
      case ARRAY:
      case MULTISET:
        return isConvertibleType(type.getComponentType());
      case MAP:
        return isConvertibleType(type.getKeyType()) && isConvertibleType(type.getValueType());
      case ROW:
        return convertibleRow(type);
      default:
        return false;
    }
  }

  static RelNode substitute(Calc calc, PlanContext ctx) {
    RexExpression condition = FilterCalcMatcher.encodedCondition(calc);
    return new StreamPhysicalNativeFilter(
        calc.getCluster(),
        calc.getTraitSet(),
        calc.getInputs().get(0),
        calc.getRowType(),
        FilterCalcMatcher.projection(calc),
        condition.kinds(),
        condition.payload(),
        condition.childCounts(),
        condition.longs(),
        condition.doubles(),
        condition.strings(),
        condition.udfBinding());
  }
}
