package tech.streamfusion.planner;

import java.util.List;
import java.util.Set;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import tech.streamfusion.operator.EncodedPredicate;
import tech.streamfusion.planner.common.EncodedExpression;

/** Shared expression admission without exposing the mutable encoder or stream rewrite rules. */
public final class ExpressionPlanning {
  private ExpressionPlanning() {}

  public static RexNode expandSearch(RexBuilder builder, RexProgram program, RexNode expression) {
    return SearchArguments.expandSearch(builder, program, expression);
  }

  public static org.apache.flink.table.planner.codegen.CodeGeneratorContext codeGeneratorContext(
      org.apache.flink.configuration.ReadableConfig config,
      ClassLoader classLoader,
      String defaultClassLoaderTerm,
      boolean recursiveContainers) {
    return new JvmFunctionCodeGeneratorContext(
        config, classLoader, defaultClassLoaderTerm, recursiveContainers);
  }

  public static EncodedExpression encodeCalc(Calc calc) {
    return RexExpression.encodeCalc(calc);
  }

  public static EncodedExpression encodeCalc(Calc calc, boolean javaJsonInput) {
    return RexExpression.encodeCalc(calc, javaJsonInput);
  }

  public static EncodedExpression encode(RexNode expression) {
    return RexExpression.encode(expression);
  }

  public static EncodedExpression encode(RexNode expression, RelNode context) {
    return RexExpression.encode(expression, context);
  }

  public static EncodedExpression encodeUnnestOperand(Calc calc) {
    return RexExpression.encodeUnnestOperand(calc);
  }

  public static EncodedExpression encodeProjections(List<RexNode> projections, List<String> names) {
    return RexExpression.encodeProjections(projections, names);
  }

  public static EncodedExpression encodeWatermark(
      RexNode expression, int rowtimeColumn, boolean epochMillis) {
    return RexExpression.encodeWatermark(expression, rowtimeColumn, epochMillis);
  }

  public static String reasonForCalc(Calc calc) {
    return RexExpression.reasonForCalc(calc);
  }

  public static boolean containsGeneratedCollectionFunction(RexProgram program) {
    return RexExpression.containsGeneratedCollectionFunction(program);
  }

  public static boolean containsGeneratedCollectionFunction(RexNode expression) {
    return RexExpression.containsGeneratedCollectionFunction(expression);
  }

  public static boolean collectionAggregateElement(RelDataType type) {
    return RexExpression.collectionAggregateElement(type);
  }

  public static boolean usesCollectionFunctions(RexProgram program) {
    return RexExpression.usesCollectionFunctions(program);
  }

  public static boolean usesCollectionScalarUdf(RexProgram program) {
    return RexExpression.usesCollectionScalarUdf(program);
  }

  public static boolean containsSqlJson(RexProgram program) {
    return RexExpression.containsSqlJson(program);
  }

  public static boolean isRowCalc(int[] kinds) {
    return RexExpression.isRowCalc(kinds);
  }

  public static Set<Integer> referencedInputs(int[] kinds, int[] payload) {
    return RexExpression.referencedInputs(kinds, payload);
  }

  public static int[] remapInputPayload(int[] kinds, int[] payload, int[] map) {
    return RexExpression.remapInputPayload(kinds, payload, map);
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
    return CollectionExpressionSemantics.unverifiedInput(calc);
  }
}
