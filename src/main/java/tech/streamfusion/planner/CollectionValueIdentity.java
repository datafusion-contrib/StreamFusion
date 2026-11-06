package tech.streamfusion.planner;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rel.core.Exchange;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.*;
import org.apache.calcite.sql.SqlKind;
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
        || type.getSqlTypeName() == SqlTypeName.MULTISET
        || type.getSqlTypeName() == SqlTypeName.ARRAY && !type.getComponentType().isNullable()
        || type.getComponentType() != null && requiresCopySemantics(type.getComponentType())
        || type.getKeyType() != null && requiresCopySemantics(type.getKeyType())
        || type.getValueType() != null && requiresCopySemantics(type.getValueType());
  }

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
              .anyMatch(CollectionValueIdentity::formatsIdentityMultiset)
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

  private static boolean identityMultiset(RelDataType type) {
    if (type.getSqlTypeName() == SqlTypeName.MULTISET) {
      var element = type.getComponentType().getSqlTypeName();
      if (element == SqlTypeName.BINARY
          || element == SqlTypeName.VARBINARY
          || RexExpression.collectionAggregateElement(type.getComponentType())) return true;
    }
    return type.getComponentType() != null && identityMultiset(type.getComponentType())
        || type.getKeyType() != null && identityMultiset(type.getKeyType())
        || type.getValueType() != null && identityMultiset(type.getValueType())
        || type.isStruct()
            && type.getFieldList().stream().anyMatch(field -> identityMultiset(field.getType()));
  }

  private static boolean formatsIdentityMultiset(RexNode expression) {
    if (!(expression instanceof RexCall)) return false;
    var call = (RexCall) expression;
    return org.apache.calcite.sql.type.SqlTypeFamily.CHARACTER.contains(call.getType())
            && call.getOperands().stream().anyMatch(value -> identityMultiset(value.getType()))
        || call.getOperands().stream().anyMatch(CollectionValueIdentity::formatsIdentityMultiset);
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
    if (materializedInputCachesAffectReaders(program)) return true;
    boolean materializesStrings =
        program.getProjectList().stream()
                .map(program::expandLocalRef)
                .anyMatch(CollectionValueIdentity::containsStringMaterializer)
            || program.getCondition() != null
                && containsStringMaterializer(program.expandLocalRef(program.getCondition()));
    if (materializesStrings
        && program.getProjectList().stream()
            .map(program::expandLocalRef)
            .anyMatch(
                node ->
                    sensitive(node.getType())
                        && !(node instanceof RexCall
                            && (RexExpression.nativeArrayJoin((RexCall) node)
                                || RexExpression.nativeJsonStringContainer((RexCall) node)))))
      return true;
    return program.getProjectList().stream()
            .anyMatch(ref -> consumerNeedsGeneration(program.expandLocalRef(ref)))
        || program.getCondition() != null
            && consumerNeedsGeneration(program.expandLocalRef(program.getCondition()));
  }

  private static boolean materializedInputCachesAffectReaders(RexProgram program) {
    boolean[] found = new boolean[2];
    var visitor =
        new RexVisitorImpl<Void>(true) {
          @Override
          public Void visitCall(RexCall call) {
            if (jsonStringMaterializesInput(call)) found[0] = true;
            if (RexExpression.nativeRepresentedSplit(call)
                && characterInput(call.getOperands().get(0), false)) {
              RexNode separator = call.getOperands().get(1);
              if (!(separator instanceof RexLiteral)
                  || "".equals(((RexLiteral) separator).getValueAs(String.class))) found[0] = true;
            }
            boolean comparesCollections =
                RexExpression.nativeStringCollectionOperation(call)
                    && java.util.Set.of(
                            "ARRAY_MIN",
                            "ARRAY_MAX",
                            "ARRAY_SORT",
                            "ARRAY_DISTINCT",
                            "ARRAY_CONTAINS",
                            "ARRAY_POSITION",
                            "ARRAY_REMOVE",
                            "ARRAY_UNION",
                            "ARRAY_EXCEPT",
                            "ARRAY_INTERSECT")
                        .contains(call.getOperator().getName());
            boolean comparesValues =
                java.util.Set.of(
                        SqlKind.EQUALS,
                        SqlKind.NOT_EQUALS,
                        SqlKind.GREATER_THAN,
                        SqlKind.GREATER_THAN_OR_EQUAL,
                        SqlKind.LESS_THAN,
                        SqlKind.LESS_THAN_OR_EQUAL,
                        SqlKind.IS_DISTINCT_FROM,
                        SqlKind.IS_NOT_DISTINCT_FROM)
                    .contains(call.getKind());
            if ((comparesCollections
                    || comparesValues
                    || RexExpression.nativeRepresentedMapUnion(call))
                && call.getOperands().stream().anyMatch(arg -> characterInput(arg, true)))
              found[1] = true;
            return super.visitCall(call);
          }
        };
    for (var project : program.getProjectList()) program.expandLocalRef(project).accept(visitor);
    if (program.getCondition() != null)
      program.expandLocalRef(program.getCondition()).accept(visitor);
    return found[0] && found[1];
  }

  private static boolean nativeStringMaterializer(RexCall call) {
    return RexExpression.nativeArrayJoin(call) || jsonStringMaterializesInput(call);
  }

  private static boolean containsStringMaterializer(RexNode node) {
    if (node instanceof RexFieldAccess)
      return containsStringMaterializer(((RexFieldAccess) node).getReferenceExpr());
    if (!(node instanceof RexCall)) return false;
    RexCall call = (RexCall) node;
    return nativeStringMaterializer(call)
        || call.getOperands().stream().anyMatch(CollectionValueIdentity::containsStringMaterializer);
  }

  private static boolean jsonStringMaterializesInput(RexCall call) {
    // Flink serializes through toString(), warming cached aliases seen by later comparisons.
    return RexExpression.nativeJsonStringContainer(call)
        && JsonStringIdentity.containsCharacter(call.getOperands().get(0).getType());
  }

  private static boolean characterInput(RexNode node, boolean skipSplit) {
    if (node instanceof RexInputRef) return JsonStringIdentity.containsCharacter(node.getType());
    if (node instanceof RexFieldAccess)
      return JsonStringIdentity.containsCharacter(node.getType())
          && characterInput(((RexFieldAccess) node).getReferenceExpr(), skipSplit);
    if (!(node instanceof RexCall)) return false;
    RexCall call = (RexCall) node;
    if (skipSplit && RexExpression.nativeRepresentedSplit(call)) return false;
    return call.getOperands().stream().anyMatch(arg -> characterInput(arg, skipSplit));
  }

  private static boolean verifiedConsumer(RexCall call) {
    return RexExpression.nativeJsonStringContainer(call)
        || RexExpression.nativeRepresentedArrayOperation(call)
        || RexExpression.nativeStringCollectionOperation(call)
        || RexExpression.nativeRepresentedSplit(call)
        || RexExpression.nativeStringArrayItem(call)
        || RexExpression.nativeRepresentedMapArrays(call)
        || RexExpression.nativeRepresentedMapUnion(call)
        || RexExpression.nativeRepresentedMapConstructor(call)
        || RexExpression.nativeCollectionCast(call)
        || RexExpression.nativeBasicRow(call)
        || RexExpression.nativeArrayJoin(call)
        || RexExpression.nativeObjectUpdate(call)
        || RexExpression.nativeObjectOf(call)
        || RexExpression.nativeInputMapProjection(call)
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
    if (RexExpression.nativeInputMapProjection(call)
        || RexExpression.nativeRepresentedArrayOperation(call)
        || RexExpression.nativeStringCollectionOperation(call)
        || RexExpression.nativeRepresentedSplit(call)
        || RexExpression.nativeStringArrayItem(call)
        || RexExpression.nativeRepresentedMapArrays(call)
        || RexExpression.nativeRepresentedMapUnion(call)
        || RexExpression.nativeRepresentedMapConstructor(call)
        || RexExpression.nativeCollectionCast(call)
        || RexExpression.nativeBasicRow(call)
        || RexExpression.nativeObjectOf(call)
        || RexExpression.nativeObjectUpdate(call))
      return call.getOperands().stream().anyMatch(CollectionValueIdentity::readsSensitiveInput);
    return !verifiedConsumer(call)
        && call.getOperands().stream().anyMatch(CollectionValueIdentity::readsSensitiveInput);
  }

  static boolean stringCarrierProducer(RelNode node) {
    if (node instanceof StreamPhysicalNativeUnnest || node instanceof StreamPhysicalNativeFilter)
      return stringCarrierProducer(node.getInput(0));
    return node instanceof StreamPhysicalNativeCalc
            && ((StreamPhysicalNativeCalc) node).preservesCollections()
        || node instanceof StreamPhysicalNativeJvmGroup
            && ((StreamPhysicalNativeJvmGroup) node).hasCollectionResults()
        || node instanceof StreamPhysicalNativeGeneratedUnnest;
  }

  static boolean readsStringValue(RexNode expression) {
    if (expression instanceof RexInputRef || expression instanceof RexFieldAccess)
      return org.apache.calcite.sql.type.SqlTypeFamily.CHARACTER.contains(expression.getType());
    if (!(expression instanceof RexCall)) return false;
    var call = (RexCall) expression;
    if ((call.getKind() == SqlKind.IS_NULL || call.getKind() == SqlKind.IS_NOT_NULL)
        && call.getOperands().size() == 1
        && (call.getOperands().get(0) instanceof RexInputRef
            || call.getOperands().get(0) instanceof RexFieldAccess)) return false;
    return call.getOperands().stream().anyMatch(CollectionValueIdentity::readsStringValue);
  }

  static boolean requiresStringCarrierEvaluation(Calc calc) {
    if (!stringCarrierProducer(calc.getInput())) return false;
    var program = calc.getProgram();
    return program.getProjectList().stream()
            .map(program::expandLocalRef)
            .anyMatch(value -> !(value instanceof RexInputRef) && readsStringValue(value))
        || program.getCondition() != null
            && readsStringValue(program.expandLocalRef(program.getCondition()));
  }

  private static boolean unverifiedColumnarProducer(RelNode node) {
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
