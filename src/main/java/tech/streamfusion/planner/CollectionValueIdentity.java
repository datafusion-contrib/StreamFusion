package tech.streamfusion.planner;

import static tech.streamfusion.planner.CollectionExpressionSemantics.formatsIdentityMultiset;
import static tech.streamfusion.planner.CollectionExpressionSemantics.identityMultiset;
import static tech.streamfusion.planner.CollectionExpressionSemantics.readsSensitiveInput;
import static tech.streamfusion.planner.CollectionExpressionSemantics.requiresCopySemantics;
import static tech.streamfusion.planner.CollectionExpressionSemantics.sensitive;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rel.core.Exchange;
import org.apache.calcite.rex.*;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalDropUpdateBefore;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalGroupAggregate;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalLocalGroupAggregate;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalMiniBatchAssigner;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalSink;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalUnion;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalWatermarkAssigner;

/**
 * Restricts representation-dependent values to boundaries verified to carry their Java semantics.
 */
final class CollectionValueIdentity {
  private CollectionValueIdentity() {}

  static boolean passesCollectionReferences(RelNode input, RexProgram program) {
    if (!(input instanceof ColumnarOutput)
        || program == null
        || program.getCondition() != null
        || unverifiedColumnarProducer(input)) return false;
    return program.getProjectList().stream()
        .map(program::expandLocalRef)
        .allMatch(
            value ->
                value instanceof RexInputRef
                    || !sensitive(value.getType()) && !readsSensitiveInput(value));
  }

  static boolean observesCollectionKeyOrder(RelNode node) {
    if (node instanceof Calc && containsIdentityCollect(node.getInput(0))) {
      var program = ((Calc) node).getProgram();
      if (program.getProjectList().stream()
              .map(program::expandLocalRef)
              .anyMatch(CollectionExpressionSemantics::formatsIdentityMultiset)
          || program.getCondition() != null
              && formatsIdentityMultiset(program.expandLocalRef(program.getCondition())))
        return true;
    }
    return node.getInputs().stream().anyMatch(CollectionValueIdentity::observesCollectionKeyOrder);
  }

  private static boolean containsIdentityCollect(RelNode node) {
    if (node instanceof StreamPhysicalGroupAggregate) {
      var aggregate = (StreamPhysicalGroupAggregate) node;
      for (int i = 0; i < aggregate.aggCalls().size(); i++) {
        var call = aggregate.aggCalls().apply(i);
        if (call.getAggregation()
                == org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable.COLLECT
            && identityMultiset(call.getType())) return true;
      }
    }
    return node.getInputs().stream().anyMatch(CollectionValueIdentity::containsIdentityCollect);
  }

  static boolean unverifiedColumnarProducer(RelNode node) {
    if (!(node instanceof ColumnarOutput)) return false;
    if (scalarArrayProducer(node)) return false;
    if (node instanceof StreamPhysicalRowDataToArrow)
      return !((StreamPhysicalRowDataToArrow) node).preservesCollections();
    if (node instanceof StreamPhysicalNativeGeneratedUnnest) return false;
    if (node instanceof StreamPhysicalNativeJvmGroup
        && ((StreamPhysicalNativeJvmGroup) node).hasCollectionResults()) return false;
    if (node instanceof StreamPhysicalNativeTableFunction
        && ((StreamPhysicalNativeTableFunction) node).preservesCollections()) return false;
    if (node instanceof StreamPhysicalNativeColumnarGroupAggregate
        && ((StreamPhysicalNativeColumnarGroupAggregate) node).preservesCollections()) return false;
    if (node instanceof StreamPhysicalNativeCalc
        && !((StreamPhysicalNativeCalc) node).preservesCollections()) return true;
    if (node instanceof StreamPhysicalNativeCalc
        || node instanceof StreamPhysicalNativeMiniBatchAssigner
        || node instanceof StreamPhysicalNativeDropUpdateBefore
        || node instanceof StreamPhysicalNativeWatermarkAssigner)
      return node.getInputs().stream()
          .anyMatch(CollectionValueIdentity::unverifiedColumnarProducer);
    return true;
  }

  static boolean requiresRowSink(RelNode node) {
    RelNode input = node.getInputs().isEmpty() ? node : node.getInput(0);
    if (tech.streamfusion.arrow.MapListRepresentation.containsMap(
            org.apache.flink.table.planner.calcite.FlinkTypeFactory$.MODULE$.toLogicalRowType(
                input.getRowType()))
        && containsMapListProducer(node)) return true;
    return requiresRepresentedRowSink(node);
  }

  private static boolean containsMapListProducer(RelNode node) {
    return node instanceof StreamPhysicalNativeOverAggregate
            && ((StreamPhysicalNativeOverAggregate) node).usesMapListTransport()
        || (node instanceof StreamPhysicalNativeColumnarTopN
                || node instanceof StreamPhysicalNativeColumnarUpdatingJoin
                || node instanceof StreamPhysicalNativeFirstN)
            && tech.streamfusion.arrow.MapListRepresentation.containsMap(
                org.apache.flink.table.planner.calcite.FlinkTypeFactory$.MODULE$.toLogicalRowType(
                    node.getRowType()))
        || node.getInputs().stream().anyMatch(CollectionValueIdentity::containsMapListProducer);
  }

  private static boolean requiresRepresentedRowSink(RelNode node) {
    if (preservingCollectionProducer(node)
        && node.getRowType().getFieldList().stream().anyMatch(field -> sensitive(field.getType())))
      return true;
    if (node instanceof StreamPhysicalNativeColumnarGroupAggregate
        && ((StreamPhysicalNativeColumnarGroupAggregate) node).preservesCollections()
        && node.getRowType().getFieldList().stream().anyMatch(field -> sensitive(field.getType())))
      return true;
    if (node instanceof StreamPhysicalNativeCalc) {
      var calc = (StreamPhysicalNativeCalc) node;
      if (calc.sourceProgram() != null
          && RexExpression.usesCollectionFunctions(calc.sourceProgram())
          && calc.getRowType().getFieldList().stream()
              .anyMatch(field -> requiresCopySemantics(field.getType()))) return true;
      if (calc.preservesCollections()
          && calc.getRowType().getFieldList().stream()
              .anyMatch(field -> sensitive(field.getType()))) return true;
    }
    return node.getInputs().stream().anyMatch(CollectionValueIdentity::requiresRepresentedRowSink);
  }

  private static boolean preservingCollectionProducer(RelNode node) {
    return node instanceof StreamPhysicalNativeCalc
            && ((StreamPhysicalNativeCalc) node).preservesCollections()
        || node instanceof StreamPhysicalNativeColumnarGroupAggregate
            && ((StreamPhysicalNativeColumnarGroupAggregate) node).hasCollectionResults()
        || node instanceof StreamPhysicalNativeUnnest
            && ((StreamPhysicalNativeUnnest) node).preservesCollections()
        || node instanceof StreamPhysicalNativeFilter
            && ((StreamPhysicalNativeFilter) node).preservesCollections()
        || node instanceof StreamPhysicalNativeGeneratedUnnest
        || node instanceof StreamPhysicalNativeJvmGroup
            && ((StreamPhysicalNativeJvmGroup) node).hasCollectionResults()
        || node instanceof StreamPhysicalNativeTableFunction
            && ((StreamPhysicalNativeTableFunction) node).preservesCollections();
  }

  static boolean scalarArrayProducer(RelNode node) {
    return (node instanceof StreamPhysicalNativeUnnest
                && ((StreamPhysicalNativeUnnest) node).preservesCollections()
            || node instanceof StreamPhysicalNativeFilter
                && ((StreamPhysicalNativeFilter) node).preservesCollections()
            || node instanceof StreamPhysicalNativeCalc
                && ((StreamPhysicalNativeCalc) node).preservesCollections()
            || node instanceof StreamPhysicalNativeColumnarGroupAggregate
                && ((StreamPhysicalNativeColumnarGroupAggregate) node).hasCollectionResults()
            || node instanceof StreamPhysicalNativeJvmGroup
                && ((StreamPhysicalNativeJvmGroup) node).hasCollectionResults())
        && node.getRowType().getFieldList().stream().anyMatch(field -> sensitive(field.getType()))
        && node.getRowType().getFieldList().stream()
            .allMatch(
                field ->
                    !sensitive(field.getType())
                        || org.apache.calcite.sql.type.SqlTypeFamily.CHARACTER.contains(
                            field.getType())
                        || field.getType().getSqlTypeName() == SqlTypeName.ARRAY
                            && field.getType().getComponentType().getComponentType() == null
                            && !requiresCopySemantics(field.getType().getComponentType()));
  }

  private static boolean verifiedCorrelate(
      org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalCorrelate node) {
    return GeneratedUnnestMatcher.matches(node)
        || TableFunctionMatcher.isUserTableFunction(node) && TableFunctionMatcher.matches(node);
  }

  static boolean generatedCollectionCrossesBoundary(RelNode root, boolean finalOutput) {
    return generatedCollectionCrossesBoundary(root, null, finalOutput, false);
  }

  private static boolean generatedCollectionCrossesBoundary(
      RelNode node, RelNode parent, boolean finalOutput, boolean verifiedConsumer) {
    if (preservingCollectionProducer(node)
        && node.getRowType().getFieldList().stream()
            .anyMatch(
                field ->
                    !org.apache.calcite.sql.type.SqlTypeFamily.CHARACTER.contains(field.getType())
                        && sensitive(field.getType()))
        && !(parent == null && finalOutput)
        && !(parent instanceof StreamPhysicalSink)
        && !FlinkPlannerCompat.isLegacyRowSink(parent)
        && !preservingCollectionProducer(parent)
        && !acceptsGeneratedCollections(parent)
        && !(passesGeneratedCollections(parent) && verifiedConsumer)
        && !(parent instanceof StreamPhysicalNativeCalc
            && ((StreamPhysicalNativeCalc) parent).preservesCollections())) return true;
    boolean next =
        acceptsGeneratedCollections(node) || passesGeneratedCollections(node) && verifiedConsumer;
    return node.getInputs().stream()
        .anyMatch(input -> generatedCollectionCrossesBoundary(input, node, false, next));
  }

  private static boolean passesGeneratedCollections(RelNode node) {
    return node instanceof StreamPhysicalNativeUnion
        || node instanceof StreamPhysicalNativeColumnarExchange
        || node instanceof StreamPhysicalNativeMiniBatchAssigner
        || node instanceof StreamPhysicalNativeDropUpdateBefore
        || node instanceof StreamPhysicalNativeWatermarkAssigner;
  }

  private static boolean acceptsGeneratedCollections(RelNode node) {
    return node instanceof StreamPhysicalNativeJvmGroup
            && ((StreamPhysicalNativeJvmGroup) node).requiresCollectionInputs()
        || node instanceof ColumnarInput
            && ((ColumnarInput) node).acceptsCollectionRepresentation();
  }

  static boolean scalarAggregateInputs(
      RelNode input,
      int[] keys,
      int[] values,
      int[] kinds,
      java.util.function.IntPredicate nullOnly) {
    for (int key : keys)
      if (sensitive(input.getRowType().getFieldList().get(key).getType())) return false;
    for (int i = 0; i < values.length; i++)
      if (values[i] >= 0
          && sensitive(input.getRowType().getFieldList().get(values[i]).getType())
          && !nullOnly.test(kinds[i])) return false;
    return true;
  }

  static boolean crossesBoundary(RelNode root, boolean finalOutput) {
    return crossesBoundary(root, null, finalOutput, false);
  }

  private static boolean crossesBoundary(
      RelNode node, RelNode parent, boolean finalOutput, boolean orderedConsumer) {
    if (node instanceof Calc
        && !orderedConsumer
        && !(parent == null && finalOutput)
        && !(parent instanceof StreamPhysicalSink)
        && !(parent
                instanceof
                org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalCorrelate
            && verifiedCorrelate(
                (org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalCorrelate)
                    parent))) {
      var program = ((Calc) node).getProgram();
      if ((RexExpression.usesCollectionFunctions(program)
              || RexExpression.usesCollectionScalarUdf(program))
          && program.getProjectList().stream().anyMatch(ref -> sensitive(ref.getType())))
        return true;
    }
    boolean next =
        node instanceof StreamPhysicalGroupAggregate
                && (GroupAggregateMatcher.preservesComplexValues(
                        (StreamPhysicalGroupAggregate) node)
                    || JvmGroupAggregateMatcher.hasNestedCollectionElements(
                            (StreamPhysicalGroupAggregate) node)
                        && JvmGroupAggregateMatcher.matches((StreamPhysicalGroupAggregate) node))
            || node instanceof StreamPhysicalLocalGroupAggregate
                && LocalGroupAggregateMatcher.preservesComplexValues(
                    (StreamPhysicalLocalGroupAggregate) node)
            || orderedConsumer
                && (node instanceof StreamPhysicalUnion
                        && UnionMatcher.matches((StreamPhysicalUnion) node)
                    || node instanceof Exchange
                    || node instanceof StreamPhysicalMiniBatchAssigner
                    || node instanceof StreamPhysicalDropUpdateBefore
                    || node instanceof StreamPhysicalWatermarkAssigner);
    return node.getInputs().stream().anyMatch(input -> crossesBoundary(input, node, false, next));
  }
}
