package tech.streamfusion.planner;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalCorrelate;

/** Flink-generated builtin expansion for operands whose container representation is observable. */
final class GeneratedUnnestMatcher {
  private GeneratedUnnestMatcher() {}

  static boolean hasShape(StreamPhysicalCorrelate node) {
    if (!(node.scan().getCall() instanceof RexCall)) return false;
    String name = ((RexCall) node.scan().getCall()).getOperator().getName();
    return name.equals("$UNNEST_ROWS$1") || name.equals("$UNNEST_ROWS_WITH_ORDINALITY$1");
  }

  static boolean matches(StreamPhysicalCorrelate node) {
    if (!hasShape(node) || UnnestMatcher.unverifiedNullableRowOrdinality(node)) return false;
    if (!NativeConfig.collectionExpressionsEnabled() && usesCollectionExpression(node))
      return false;
    var join = UnnestMatcher.joinType(node);
    return (join == JoinRelType.INNER || join == JoinRelType.LEFT)
        && FilterCalcMatcher.convertibleRow(node.getInput().getRowType())
        && FilterCalcMatcher.convertibleRow(node.getRowType())
        && verifiedInput(node.getInput())
        && ((RexCall) node.scan().getCall())
            .getOperands().stream().noneMatch(GeneratedUnnestMatcher::containsUserFunction)
        && (!node.condition().isDefined() || !containsUserFunction(node.condition().get()));
  }

  private static boolean usesCollectionExpression(StreamPhysicalCorrelate node) {
    return ((RexCall) node.scan().getCall())
            .getOperands().stream().anyMatch(RexExpression::containsGeneratedCollectionFunction)
        || node.condition().isDefined()
            && RexExpression.containsGeneratedCollectionFunction(node.condition().get());
  }

  private static boolean containsUserFunction(RexNode expression) {
    if (expression instanceof org.apache.calcite.rex.RexFieldAccess)
      return containsUserFunction(
          ((org.apache.calcite.rex.RexFieldAccess) expression).getReferenceExpr());
    if (!(expression instanceof RexCall)) return false;
    var call = (RexCall) expression;
    if (tech.streamfusion.compat.FlinkCompat.scalarFunction(call.getOperator()) != null)
      return true;
    return call.getOperands().stream().anyMatch(GeneratedUnnestMatcher::containsUserFunction);
  }

  static boolean preservedInput(RelNode node) {
    if (node instanceof StreamPhysicalNativeGeneratedUnnest) return true;
    if (node instanceof StreamPhysicalNativeTableFunction)
      return ((StreamPhysicalNativeTableFunction) node).preservesCollections();
    if (node instanceof StreamPhysicalNativeCalc)
      return ((StreamPhysicalNativeCalc) node).preservesCollections();
    if (node instanceof StreamPhysicalRowDataToArrow)
      return ((StreamPhysicalRowDataToArrow) node).preservesCollections();
    if (node instanceof StreamPhysicalNativeDropUpdateBefore
        || node instanceof StreamPhysicalNativeMiniBatchAssigner
        || node instanceof StreamPhysicalNativeWatermarkAssigner)
      return preservedInput(node.getInput(0));
    return false;
  }

  static boolean verifiedInput(RelNode node) {
    if (!(node instanceof ColumnarOutput)) return true;
    if (node instanceof StreamPhysicalNativeGeneratedUnnest) return true;
    if (node instanceof StreamPhysicalNativeTableFunction)
      return ((StreamPhysicalNativeTableFunction) node).preservesCollections();
    if (node instanceof StreamPhysicalNativeCalc)
      return ((StreamPhysicalNativeCalc) node).preservesCollections()
          || node.getRowType().getFieldList().stream()
              .noneMatch(field -> CollectionValueIdentity.sensitive(field.getType()));
    if (node instanceof StreamPhysicalNativeDropUpdateBefore
        || node instanceof StreamPhysicalNativeMiniBatchAssigner
        || node instanceof StreamPhysicalNativeWatermarkAssigner)
      return verifiedInput(node.getInput(0));
    return node.getRowType().getFieldList().stream()
        .noneMatch(field -> CollectionValueIdentity.sensitive(field.getType()));
  }

  static String unsupportedReason(StreamPhysicalCorrelate node) {
    if (!NativeConfig.collectionExpressionsEnabled() && usesCollectionExpression(node))
      return "UNNEST computed collection expressions disabled by"
          + " streamfusion.experimental.collectionExpressions";
    return "UNNEST generated boundary requires convertible types, verified collection input"
        + " representation, and builtin expressions";
  }

  static RelNode substitute(StreamPhysicalCorrelate node, PlanContext context) {
    return new StreamPhysicalNativeGeneratedUnnest(
        node.getCluster(),
        node.getTraitSet(),
        node.getInput(),
        node.getRowType(),
        (RexCall) node.scan().getCall(),
        node.condition().isDefined() ? node.condition().get() : null,
        UnnestMatcher.isLeft(node));
  }
}
