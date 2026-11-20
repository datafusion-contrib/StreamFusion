package tech.streamfusion.planner.stream;

import java.util.List;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import tech.streamfusion.planner.CollectionExpressionSemantics;
import tech.streamfusion.planner.ExpressionPlanning;
import tech.streamfusion.planner.common.Admission;
import tech.streamfusion.planner.common.EncodedExpression;
import tech.streamfusion.planner.common.PreparedExpression;
import tech.streamfusion.planner.common.StreamRule.PreparedPlan;

/**
 * Recognizes a filter the native operator can run: a {@link Calc} with an input-ref projection (a
 * column subset/reorder — general projections are separate) whose condition the native expression
 * engine can evaluate, over an input whose every column the whole-row converter handles. The
 * condition is encoded by {@link EncodedExpression}; anything it cannot encode — an unsupported
 * operation or operand — falls back, as do computed/constant projections and unsupported column
 * types.
 */
final class FilterCalcMatcher {

  private FilterCalcMatcher() {}

  static boolean matches(Calc calc) {
    return prepareCondition(calc).isAccepted();
  }

  static Admission<PreparedPlan> prepare(Calc calc, PlanContext context) {
    return prepareCondition(calc).map(condition -> () -> substitute(calc, condition, context));
  }

  static boolean hasFilterShape(Calc calc) {
    return calc.getProgram().getCondition() != null && projection(calc) != null;
  }

  /**
   * The condition encoded for the native engine, or null if it contains an unsupported operation.
   */
  static EncodedExpression encodedCondition(Calc calc) {
    var condition = prepareCondition(calc);
    return condition.isAccepted() ? condition.value() : null;
  }

  private static Admission<EncodedExpression> prepareCondition(Calc calc) {
    if (!hasFilterShape(calc)) return Admission.noMatch();
    if (ExpressionPlanning.usesPairwiseStringComparison(calc)) return Admission.noMatch();
    if (CollectionExpressionSemantics.unverifiedInput(calc))
      return Admission.declined("Calc: collection input representation is unverified for this columnar producer");
    if (CollectionExpressionSemantics.requiresStringCarrierEvaluation(calc))
      return Admission.declined("Calc: owned string carriers require generated scalar evaluation");
    if (!convertibleRow(calc.getInput().getRowType()))
      return Admission.declined("Calc: unsupported input column type");
    if (CollectionExpressionSemantics.requiresGeneratedEvaluation(calc.getProgram()))
      return Admission.declined("Calc: collection representations require generated evaluation");
    RexProgram program = calc.getProgram();
    RexNode condition = program.expandLocalRef(program.getCondition());
    RexNode expanded =
        ExpressionPlanning.expandSearch(calc.getCluster().getRexBuilder(), null, condition);
    var encoded = ExpressionPlanning.prepare(expanded, calc);
    return encoded.isAccepted() ? Admission.accepted(PreparedExpression.copyOf(encoded.value()))
        : Admission.declined("Calc: " + encoded.reason());
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
    var prepared = prepare(calc, ctx);
    return prepared.isAccepted() ? prepared.value().build() : null;
  }

  private static RelNode substitute(Calc calc, EncodedExpression condition, PlanContext ctx) {
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
