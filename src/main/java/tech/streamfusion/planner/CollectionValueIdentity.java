package tech.streamfusion.planner;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rel.core.Exchange;
import org.apache.calcite.rel.type.RelDataType;
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

  static boolean sensitive(RelDataType type) {
    return JsonStringIdentity.containsCharacter(type) || requiresCopySemantics(type);
  }

  private static boolean requiresCopySemantics(RelDataType type) {
    return type.getSqlTypeName() == SqlTypeName.ROW
        || type.getSqlTypeName() == SqlTypeName.STRUCTURED
        || type.getSqlTypeName() == SqlTypeName.MAP
        || type.getSqlTypeName() == SqlTypeName.ARRAY && !type.getComponentType().isNullable()
        || type.getComponentType() != null && requiresCopySemantics(type.getComponentType())
        || type.getKeyType() != null && requiresCopySemantics(type.getKeyType())
        || type.getValueType() != null && requiresCopySemantics(type.getValueType());
  }

  static boolean fixedBinary(RelDataType type) {
    return type.getSqlTypeName() == SqlTypeName.BINARY
        || type.getComponentType() != null && fixedBinary(type.getComponentType())
        || type.getKeyType() != null && fixedBinary(type.getKeyType())
        || type.getValueType() != null && fixedBinary(type.getValueType())
        || type.isStruct()
            && type.getFieldList().stream().anyMatch(field -> fixedBinary(field.getType()));
  }

  static boolean requiresGeneratedEvaluation(RexProgram program) {
    if (!RexExpression.usesCollectionFunctions(program)) return false;
    boolean joins =
        program.getExprList().stream()
            .anyMatch(
                node -> node instanceof RexCall && RexExpression.nativeArrayJoin((RexCall) node));
    if (joins
        && program.getProjectList().stream()
            .map(program::expandLocalRef)
            .anyMatch(
                node ->
                    sensitive(node.getType())
                        && !(node instanceof RexCall
                            && RexExpression.nativeArrayJoin((RexCall) node)))) return true;
    return program.getProjectList().stream()
            .anyMatch(ref -> consumerNeedsGeneration(program.expandLocalRef(ref)))
        || program.getCondition() != null
            && consumerNeedsGeneration(program.expandLocalRef(program.getCondition()));
  }

  private static boolean verifiedConsumer(RexCall call) {
    return RexExpression.nativeArrayJoin(call)
        || RexExpression.nativeObjectUpdate(call)
        || RexExpression.nativeObjectOf(call)
        || RexExpression.nativeStringKeyMapProjection(call)
        || call.getOperator().getName().equals("CARDINALITY")
        || tech.streamfusion.compat.FlinkCompat.scalarFunction(call.getOperator()) == null
            && java.util.Set.of("MAP_KEYS", "MAP_VALUES", "MAP_ENTRIES", "MAP_UNION")
                .contains(call.getOperator().getName())
            && !call.getOperands().isEmpty()
            && call.getOperands().stream()
                .allMatch(arg -> RexExpression.nativeBasicMap(arg.getType()));
  }

  private static boolean consumerNeedsGeneration(RexNode node) {
    if (!(node instanceof RexCall)) return false;
    var call = (RexCall) node;
    return !verifiedConsumer(call)
            && call.getOperands().stream().anyMatch(arg -> sensitive(arg.getType()))
        || call.getOperands().stream().anyMatch(CollectionValueIdentity::consumerNeedsGeneration);
  }

  static boolean unverifiedInput(Calc calc) {
    var program = calc.getProgram();
    return RexExpression.usesCollectionFunctions(program)
        && (program.getProjectList().stream()
                .anyMatch(ref -> readsSensitiveInput(program.expandLocalRef(ref)))
            || program.getCondition() != null
                && readsSensitiveInput(program.expandLocalRef(program.getCondition())))
        && unverifiedColumnarProducer(calc.getInput());
  }

  private static boolean readsSensitiveInput(RexNode node) {
    if (node instanceof RexInputRef) return sensitive(node.getType());
    if (node instanceof RexFieldAccess)
      return readsSensitiveInput(((RexFieldAccess) node).getReferenceExpr());
    if (!(node instanceof RexCall)) return false;
    var call = (RexCall) node;
    if (RexExpression.nativeStringKeyMapProjection(call))
      return call.getOperands().stream().anyMatch(CollectionValueIdentity::readsSensitiveInput);
    return !verifiedConsumer(call)
        && call.getOperands().stream().anyMatch(CollectionValueIdentity::readsSensitiveInput);
  }

  private static boolean unverifiedColumnarProducer(RelNode node) {
    if (!(node instanceof ColumnarOutput)) return false;
    if (node instanceof StreamPhysicalRowDataToArrow)
      return !((StreamPhysicalRowDataToArrow) node).preservesCollections();
    if (node instanceof StreamPhysicalNativeGeneratedUnnest) return false;
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
    if (preservingCorrelate(node)
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

  private static boolean preservingCorrelate(RelNode node) {
    return node instanceof StreamPhysicalNativeGeneratedUnnest
        || node instanceof StreamPhysicalNativeTableFunction
            && ((StreamPhysicalNativeTableFunction) node).preservesCollections();
  }

  private static boolean verifiedCorrelate(
      org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalCorrelate node) {
    return GeneratedUnnestMatcher.matches(node)
        || TableFunctionMatcher.isUserTableFunction(node) && TableFunctionMatcher.matches(node);
  }

  static boolean generatedCorrelateCrossesBoundary(RelNode root, boolean finalOutput) {
    return generatedCorrelateCrossesBoundary(root, null, finalOutput);
  }

  private static boolean generatedCorrelateCrossesBoundary(
      RelNode node, RelNode parent, boolean finalOutput) {
    if (preservingCorrelate(node)
        && node.getRowType().getFieldList().stream()
            .anyMatch(
                field ->
                    !org.apache.calcite.sql.type.SqlTypeFamily.CHARACTER.contains(field.getType())
                        && sensitive(field.getType()))
        && !(parent == null && finalOutput)
        && !(parent instanceof StreamPhysicalSink)
        && !FlinkPlannerCompat.isLegacyRowSink(parent)
        && !preservingCorrelate(parent)
        && !(parent instanceof StreamPhysicalNativeCalc
            && ((StreamPhysicalNativeCalc) parent).preservesCollections())) return true;
    return node.getInputs().stream()
        .anyMatch(input -> generatedCorrelateCrossesBoundary(input, node, false));
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
                && GroupAggregateMatcher.preservesComplexValues((StreamPhysicalGroupAggregate) node)
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
