package tech.streamfusion.planner;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rex.RexNode;
import tech.streamfusion.operator.EncodedPredicate;
import tech.streamfusion.planner.common.EncodedExpression;

/** Shared expression admission without exposing the mutable encoder or stream rewrite rules. */
public final class ExpressionPlanning {
  private ExpressionPlanning() {}

  public static EncodedExpression encodeCalc(Calc calc) {
    return RexExpression.encodeCalc(calc);
  }

  public static EncodedExpression encodePredicate(RexNode condition, RelNode context) {
    return condition == null
        ? null
        : RexExpression.encode(
            SearchArguments.expandSearch(context.getCluster().getRexBuilder(), null, condition),
            context);
  }

  public static EncodedPredicate predicate(EncodedExpression expression) {
    if (expression == null) return EncodedPredicate.NONE;
    return new EncodedPredicate(
        expression.kinds(),
        expression.payload(),
        expression.childCounts(),
        expression.longs(),
        expression.doubles(),
        expression.strings(),
        expression.udfBinding());
  }

  public static boolean unverifiedInput(Calc calc) {
    return CollectionValueIdentity.unverifiedInput(calc);
  }
}
