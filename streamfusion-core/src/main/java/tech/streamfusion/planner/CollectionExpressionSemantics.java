package tech.streamfusion.planner;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.*;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;

/** Shared collection/string expression rules, independent of native physical-node classes. */
public final class CollectionExpressionSemantics {
  private CollectionExpressionSemantics() {}

  private static boolean unverifiedColumnarInput(RelNode node) {
    if (!(node instanceof ColumnarOutput)) return false;
    return !(node instanceof tech.streamfusion.planner.common.ExpressionInput)
        || ((tech.streamfusion.planner.common.ExpressionInput) node).unverifiedCollectionInput();
  }

  public static boolean sensitive(RelDataType type) {
    return JsonStringIdentity.containsCharacter(type) || requiresCopySemantics(type);
  }

  public static boolean requiresCopySemantics(RelDataType type) {
    return type.getSqlTypeName() == SqlTypeName.ROW
        || type.getSqlTypeName() == SqlTypeName.STRUCTURED
        || type.getSqlTypeName() == SqlTypeName.MAP
        || type.getSqlTypeName() == SqlTypeName.MULTISET
        || type.getSqlTypeName() == SqlTypeName.ARRAY && !type.getComponentType().isNullable()
        || type.getComponentType() != null && requiresCopySemantics(type.getComponentType())
        || type.getKeyType() != null && requiresCopySemantics(type.getKeyType())
        || type.getValueType() != null && requiresCopySemantics(type.getValueType());
  }

  public static boolean identityMultiset(RelDataType type) {
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

  public static boolean formatsIdentityMultiset(RexNode expression) {
    if (!(expression instanceof RexCall)) return false;
    var call = (RexCall) expression;
    return org.apache.calcite.sql.type.SqlTypeFamily.CHARACTER.contains(call.getType())
            && call.getOperands().stream().anyMatch(value -> identityMultiset(value.getType()))
        || call.getOperands().stream()
            .anyMatch(CollectionExpressionSemantics::formatsIdentityMultiset);
  }

  public static boolean fixedBinary(RelDataType type) {
    return type.getSqlTypeName() == SqlTypeName.BINARY
        || type.getComponentType() != null && fixedBinary(type.getComponentType())
        || type.getKeyType() != null && fixedBinary(type.getKeyType())
        || type.getValueType() != null && fixedBinary(type.getValueType())
        || type.isStruct()
            && type.getFieldList().stream().anyMatch(field -> fixedBinary(field.getType()));
  }

  public static boolean requiresGeneratedEvaluation(RexProgram program) {
    if (!RexExpression.usesCollectionFunctions(program)) return false;
    if (materializedInputCachesAffectReaders(program)) return true;
    boolean materializesStrings =
        program.getProjectList().stream()
                .map(program::expandLocalRef)
                .anyMatch(CollectionExpressionSemantics::containsStringMaterializer)
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
        || call.getOperands().stream()
            .anyMatch(CollectionExpressionSemantics::containsStringMaterializer);
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
        || call.getOperands().stream()
            .anyMatch(CollectionExpressionSemantics::consumerNeedsGeneration);
  }

  public static boolean unverifiedInput(Calc calc) {
    var program = calc.getProgram();
    return RexExpression.usesCollectionFunctions(program)
        && (program.getProjectList().stream()
                .anyMatch(ref -> readsSensitiveInput(program.expandLocalRef(ref)))
            || program.getCondition() != null
                && readsSensitiveInput(program.expandLocalRef(program.getCondition())))
        && unverifiedColumnarInput(calc.getInput());
  }

  public static boolean readsSensitiveInput(RexNode node) {
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
      return call.getOperands().stream()
          .anyMatch(CollectionExpressionSemantics::readsSensitiveInput);
    return !verifiedConsumer(call)
        && call.getOperands().stream().anyMatch(CollectionExpressionSemantics::readsSensitiveInput);
  }

  public static boolean stringCarrierProducer(RelNode node) {
    return node instanceof tech.streamfusion.planner.common.ExpressionInput
        && ((tech.streamfusion.planner.common.ExpressionInput) node).carriesStringValues();
  }

  public static boolean readsStringValue(RexNode expression) {
    if (expression instanceof RexInputRef || expression instanceof RexFieldAccess)
      return org.apache.calcite.sql.type.SqlTypeFamily.CHARACTER.contains(expression.getType());
    if (!(expression instanceof RexCall)) return false;
    var call = (RexCall) expression;
    if ((call.getKind() == SqlKind.IS_NULL || call.getKind() == SqlKind.IS_NOT_NULL)
        && call.getOperands().size() == 1
        && (call.getOperands().get(0) instanceof RexInputRef
            || call.getOperands().get(0) instanceof RexFieldAccess)) return false;
    return call.getOperands().stream().anyMatch(CollectionExpressionSemantics::readsStringValue);
  }

  public static boolean requiresStringCarrierEvaluation(Calc calc) {
    if (!stringCarrierProducer(calc.getInput())) return false;
    var program = calc.getProgram();
    return program.getProjectList().stream()
            .map(program::expandLocalRef)
            .anyMatch(value -> !(value instanceof RexInputRef) && readsStringValue(value))
        || program.getCondition() != null
            && readsStringValue(program.expandLocalRef(program.getCondition()));
  }
}
