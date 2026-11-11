package tech.streamfusion.planner;

import java.util.List;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;

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
    if (CollectionExpressionSemantics.unverifiedInput(calc)
        || CollectionExpressionSemantics.requiresStringCarrierEvaluation(calc)) return false;
    if (!hasFilterShape(calc)) {
      return false;
    }
    if (!convertibleRow(calc.getInput().getRowType())) {
      return false;
    }
    return encodedCondition(calc) != null;
  }

  static boolean hasFilterShape(Calc calc) {
    return calc.getProgram().getCondition() != null && projection(calc) != null;
  }

  /**
   * The condition encoded for the native engine, or null if it contains an unsupported operation.
   */
  static RexExpression encodedCondition(Calc calc) {
    if (CollectionExpressionSemantics.requiresGeneratedEvaluation(calc.getProgram())) return null;
    RexProgram program = calc.getProgram();
    RexNode condition = program.expandLocalRef(program.getCondition());
    // Calcite folds `col = literal`, IN, and ranges into a SEARCH/Sarg; expand it back into the
    // comparisons the native engine evaluates. The native side handles arbitrary AND/OR/NOT trees,
    // so no disjunctive-normal-form rewrite is needed.
    RexNode expanded = SearchArguments.expandSearch(calc.getCluster().getRexBuilder(), null, condition);
    return RexExpression.encode(expanded, calc);
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

  static boolean convertibleRow(RelDataType type) {
    return tech.streamfusion.planner.common.ArrowBoundaryTypes.convertibleRow(type);
  }

  static boolean convertibleRow(RelDataType type, boolean allowStructured) {
    return tech.streamfusion.planner.common.ArrowBoundaryTypes.convertibleRow(type, allowStructured);
  }

  static RelNode substitute(Calc calc, PlanContext ctx) {
    RexExpression condition = FilterCalcMatcher.encodedCondition(calc);
    ctx.functionExecution("NativeFilter", condition);
    StreamPhysicalNativeFilter filter = new StreamPhysicalNativeFilter(calc, condition);
    if (filter.getInput() instanceof StreamPhysicalNativeUnnest) {
      int[] identity =
          java.util.stream.IntStream.range(0, calc.getRowType().getFieldCount()).toArray();
      return filter.withProjection(calc.getTraitSet(), calc.getRowType(), identity);
    }
    return filter;
  }
}
