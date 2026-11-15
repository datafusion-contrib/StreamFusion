package tech.streamfusion.planner;

import static tech.streamfusion.planner.ExpressionTypeSemantics.*;
import static tech.streamfusion.planner.NumericExpressionSemantics.*;

import java.util.Set;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.*;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeFamily;
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
          || CollectionExpressionSemantics.collectionAggregateElement(type.getComponentType()))
        return true;
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
    if (!CollectionExpressionSemantics.usesCollectionFunctions(program)) return false;
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
                            && (CollectionExpressionSemantics.nativeArrayJoin((RexCall) node)
                                || CollectionExpressionSemantics.nativeJsonStringContainer(
                                    (RexCall) node))))) return true;
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
            if (CollectionExpressionSemantics.nativeRepresentedSplit(call)
                && characterInput(call.getOperands().get(0), false)) {
              RexNode separator = call.getOperands().get(1);
              if (!(separator instanceof RexLiteral)
                  || "".equals(((RexLiteral) separator).getValueAs(String.class))) found[0] = true;
            }
            boolean comparesCollections =
                CollectionExpressionSemantics.nativeStringCollectionOperation(call)
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
                    || CollectionExpressionSemantics.nativeRepresentedMapUnion(call))
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
    return CollectionExpressionSemantics.nativeArrayJoin(call) || jsonStringMaterializesInput(call);
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
    return CollectionExpressionSemantics.nativeJsonStringContainer(call)
        && JsonStringIdentity.containsCharacter(call.getOperands().get(0).getType());
  }

  private static boolean characterInput(RexNode node, boolean skipSplit) {
    if (node instanceof RexInputRef) return JsonStringIdentity.containsCharacter(node.getType());
    if (node instanceof RexFieldAccess)
      return JsonStringIdentity.containsCharacter(node.getType())
          && characterInput(((RexFieldAccess) node).getReferenceExpr(), skipSplit);
    if (!(node instanceof RexCall)) return false;
    RexCall call = (RexCall) node;
    if (skipSplit && CollectionExpressionSemantics.nativeRepresentedSplit(call)) return false;
    return call.getOperands().stream().anyMatch(arg -> characterInput(arg, skipSplit));
  }

  private static boolean verifiedConsumer(RexCall call) {
    return CollectionExpressionSemantics.nativeJsonStringContainer(call)
        || CollectionExpressionSemantics.nativeRepresentedArrayOperation(call)
        || CollectionExpressionSemantics.nativeStringCollectionOperation(call)
        || CollectionExpressionSemantics.nativeRepresentedSplit(call)
        || CollectionExpressionSemantics.nativeStringArrayItem(call)
        || CollectionExpressionSemantics.nativeRepresentedMapArrays(call)
        || CollectionExpressionSemantics.nativeRepresentedMapUnion(call)
        || CollectionExpressionSemantics.nativeRepresentedMapConstructor(call)
        || CollectionExpressionSemantics.nativeCollectionCast(call)
        || CollectionExpressionSemantics.nativeBasicRow(call)
        || CollectionExpressionSemantics.nativeArrayJoin(call)
        || CollectionExpressionSemantics.nativeObjectUpdate(call)
        || CollectionExpressionSemantics.nativeObjectOf(call)
        || CollectionExpressionSemantics.nativeInputMapProjection(call)
        || call.getOperator().getName().equals("CARDINALITY")
        || tech.streamfusion.compat.FlinkCompat.scalarFunction(call.getOperator()) == null
            && java.util.Set.of("MAP_KEYS", "MAP_VALUES", "MAP_ENTRIES", "MAP_UNION")
                .contains(call.getOperator().getName())
            && !call.getOperands().isEmpty()
            && call.getOperands().stream()
                .allMatch(arg -> CollectionExpressionSemantics.nativeBasicMap(arg.getType()));
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
    return CollectionExpressionSemantics.usesCollectionFunctions(program)
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
    if (CollectionExpressionSemantics.nativeInputMapProjection(call)
        || CollectionExpressionSemantics.nativeRepresentedArrayOperation(call)
        || CollectionExpressionSemantics.nativeStringCollectionOperation(call)
        || CollectionExpressionSemantics.nativeRepresentedSplit(call)
        || CollectionExpressionSemantics.nativeStringArrayItem(call)
        || CollectionExpressionSemantics.nativeRepresentedMapArrays(call)
        || CollectionExpressionSemantics.nativeRepresentedMapUnion(call)
        || CollectionExpressionSemantics.nativeRepresentedMapConstructor(call)
        || CollectionExpressionSemantics.nativeCollectionCast(call)
        || CollectionExpressionSemantics.nativeBasicRow(call)
        || CollectionExpressionSemantics.nativeObjectOf(call)
        || CollectionExpressionSemantics.nativeObjectUpdate(call))
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

  static final Set<String> GENERATED_COLLECTIONS =
      Set.of(
          "ARRAY_APPEND",
          "ARRAY_PREPEND",
          "ARRAY_CONTAINS",
          "ARRAY_POSITION",
          "ARRAY_REMOVE",
          "ARRAY_REVERSE",
          "ARRAY_SLICE",
          "ARRAY_SORT",
          "ARRAY_UNION",
          "ARRAY_CONCAT",
          "ARRAY_EXCEPT",
          "ARRAY_INTERSECT",
          "ARRAY_JOIN",
          "ARRAY_MIN",
          "ARRAY_MAX",
          "ARRAY_DISTINCT",
          "MAP_KEYS",
          "MAP_VALUES",
          "MAP_ENTRIES",
          "MAP_FROM_ARRAYS",
          "MAP_UNION",
          "ELEMENT",
          "CARDINALITY",
          "ARRAY",
          "MAP",
          "ROW",
          "OBJECT_OF",
          "OBJECT_UPDATE");

  static boolean collectionAggregateElement(RelDataType type) {
    return tech.streamfusion.arrow.ArrowConversion.nestedScalarUdfsEnabled()
        && java.util.Set.of(SqlTypeName.MAP, SqlTypeName.MULTISET).contains(type.getSqlTypeName())
        && rowCalcTypeCode(type, true) >= 0;
  }

  static boolean nativeBasicRow(RexCall call) {
    return call.getKind() == SqlKind.ROW
        && tech.streamfusion.compat.FlinkCompat.scalarFunction(call.getOperator()) == null
        && !call.getOperands().isEmpty()
        && call.getOperands().stream()
            .allMatch(
                arg ->
                    java.util.Set.of(
                            SqlTypeName.BOOLEAN,
                            SqlTypeName.TINYINT,
                            SqlTypeName.SMALLINT,
                            SqlTypeName.INTEGER,
                            SqlTypeName.BIGINT,
                            SqlTypeName.DECIMAL,
                            SqlTypeName.CHAR,
                            SqlTypeName.VARCHAR)
                        .contains(arg.getType().getSqlTypeName()));
  }

  static boolean containsGeneratedCollectionFunction(RexProgram program) {
    if (!NativeConfig.collectionExpressionsEnabled()) return false;
    boolean[] userFunction = {false};
    var visitor =
        new org.apache.calcite.rex.RexVisitorImpl<Void>(true) {
          @Override
          public Void visitCall(RexCall call) {
            if (tech.streamfusion.compat.FlinkCompat.scalarFunction(call.getOperator()) != null)
              userFunction[0] = true;
            return super.visitCall(call);
          }
        };
    for (RexNode node : program.getExprList()) node.accept(visitor);
    if (userFunction[0]) return false;
    return usesCollectionFunctions(program);
  }

  static boolean usesCollectionFunctions(RexProgram program) {
    if (!NativeConfig.collectionExpressionsEnabled()) return false;
    return program.getCondition() != null
            && containsGeneratedCollectionFunction(program.expandLocalRef(program.getCondition()))
        || program.getProjectList().stream()
            .anyMatch(ref -> containsGeneratedCollectionFunction(program.expandLocalRef(ref)));
  }

  static boolean containsGeneratedCollectionFunction(RexNode node) {
    if (node instanceof RexFieldAccess)
      return containsGeneratedCollectionFunction(((RexFieldAccess) node).getReferenceExpr());
    if (!(node instanceof RexCall)) return false;
    RexCall call = (RexCall) node;
    return GENERATED_COLLECTIONS.contains(call.getOperator().getName())
        || nativeJsonStringContainer(call)
        || standaloneCollectionExpression(call)
        || call.getOperands().stream()
            .anyMatch(CollectionExpressionSemantics::containsGeneratedCollectionFunction);
  }

  static boolean directBinaryMultisetCase(RexCall call) {
    return call.getKind() == SqlKind.CASE
        && call.getType().getSqlTypeName() == SqlTypeName.MULTISET
        && call.getType().getComponentType().getSqlTypeName() == SqlTypeName.BINARY;
  }

  static boolean standaloneCollectionExpression(RexCall call) {
    if (directBinaryMultisetCase(call)) return true;
    if ((call.getKind() == SqlKind.CAST
            || call.getOperator()
                == org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable.TRY_CAST)
        && call.getOperands().size() == 1) {
      var source = call.getOperands().get(0).getType().getSqlTypeName();
      return Set.of(
                  SqlTypeName.ARRAY,
                  SqlTypeName.MAP,
                  SqlTypeName.MULTISET,
                  SqlTypeName.ROW,
                  SqlTypeName.STRUCTURED)
              .contains(source)
          && (source == call.getType().getSqlTypeName()
              || isCharacter(call)
              || (source == SqlTypeName.ROW || source == SqlTypeName.STRUCTURED)
                  && (call.getType().getSqlTypeName() == SqlTypeName.ROW
                      || call.getType().getSqlTypeName() == SqlTypeName.STRUCTURED));
    }
    if (!"SPLIT".equals(call.getOperator().getName())
        || call.getOperands().size() != 2
        || tech.streamfusion.compat.FlinkCompat.scalarFunction(call.getOperator()) != null
        || !isCharacter(call.getOperands().get(0))
        || !isCharacter(call.getOperands().get(1))) return false;
    var separator = call.getOperands().get(1);
    if (!(separator instanceof RexLiteral)) return true;
    String value = ((RexLiteral) separator).getValueAs(String.class);
    return value == null || value.isEmpty();
  }

  static boolean nativeBasicArray(RelDataType type) {
    return type.getSqlTypeName() == SqlTypeName.ARRAY
        && type.getComponentType().isNullable()
        && java.util.Set.of(
                SqlTypeName.BOOLEAN,
                SqlTypeName.TINYINT,
                SqlTypeName.SMALLINT,
                SqlTypeName.INTEGER,
                SqlTypeName.BIGINT,
                SqlTypeName.DECIMAL)
            .contains(type.getComponentType().getSqlTypeName());
  }

  static boolean nativeRepresentedArrayOperation(RexCall call) {
    var args = call.getOperands();
    if (tech.streamfusion.compat.FlinkCompat.scalarFunction(call.getOperator()) != null
        || args.isEmpty()
        || !Set.of("ARRAY_REVERSE", "ARRAY_SLICE").contains(call.getOperator().getName()))
      return false;
    var type = args.get(0).getType();
    return type.getSqlTypeName() == SqlTypeName.ARRAY
        && type.getComponentType().isNullable()
        && !CollectionExpressionSemantics.fixedBinary(type)
        && nestedFieldSupported(type.getComponentType(), true)
        && CollectionExpressionSemantics.sensitive(type);
  }

  static boolean nativeStringArray(RelDataType type) {
    return type.getSqlTypeName() == SqlTypeName.ARRAY
        && type.getComponentType().isNullable()
        && SqlTypeFamily.CHARACTER.contains(type.getComponentType());
  }

  static boolean nativeStringCollectionOperation(RexCall call) {
    if (call.getKind() == SqlKind.ARRAY_VALUE_CONSTRUCTOR)
      return nativeStringArray(call.getType())
          && call.getOperands().stream()
              .allMatch(arg -> arg instanceof RexInputRef || arg instanceof RexLiteral);
    return tech.streamfusion.compat.FlinkCompat.scalarFunction(call.getOperator()) == null
        && !call.getOperands().isEmpty()
        && nativeStringArray(call.getOperands().get(0).getType())
        && Set.of(
                "ARRAY_APPEND",
                "ARRAY_PREPEND",
                "ARRAY_CONTAINS",
                "ARRAY_POSITION",
                "ARRAY_REMOVE",
                "ARRAY_DISTINCT",
                "ARRAY_UNION",
                "ARRAY_CONCAT",
                "ARRAY_EXCEPT",
                "ARRAY_INTERSECT",
                "ARRAY_MIN",
                "ARRAY_MAX",
                "ARRAY_SORT",
                "ELEMENT")
            .contains(call.getOperator().getName());
  }

  static boolean nativeRepresentedSplit(RexCall call) {
    if (!call.getOperator().getName().equals("SPLIT")
        || tech.streamfusion.compat.FlinkCompat.scalarFunction(call.getOperator()) != null
        || call.getOperands().size() != 2
        || call.getOperands().stream().anyMatch(arg -> !isCharacter(arg))) return false;
    RexNode separator = call.getOperands().get(1);
    if (!(separator instanceof RexLiteral)) return true;
    String text = ((RexLiteral) separator).getValueAs(String.class);
    return text == null || text.isEmpty();
  }

  static boolean nativeStringArrayItem(RexCall call) {
    return call.getKind() == SqlKind.ITEM
        && call.getOperands().size() == 2
        && nativeStringArray(call.getOperands().get(0).getType())
        && call.getOperands().get(1).getType().getSqlTypeName() == SqlTypeName.INTEGER;
  }

  static boolean nativeRepresentedMapArrays(RexCall call) {
    return call.getOperator().getName().equals("MAP_FROM_ARRAYS")
        && tech.streamfusion.compat.FlinkCompat.scalarFunction(call.getOperator()) == null
        && call.getOperands().size() == 2
        && call.getOperands().stream()
            .allMatch(arg -> nativeBasicArray(arg.getType()) || nativeStringArray(arg.getType()))
        && call.getOperands().stream().anyMatch(arg -> nativeStringArray(arg.getType()));
  }

  static boolean nativeStringMap(RelDataType type) {
    return type.getSqlTypeName() == SqlTypeName.MAP
        && type.getKeyType().isNullable()
        && SqlTypeFamily.CHARACTER.contains(type.getKeyType())
        && type.getValueType().isNullable()
        && Set.of(
                SqlTypeName.BOOLEAN,
                SqlTypeName.TINYINT,
                SqlTypeName.SMALLINT,
                SqlTypeName.INTEGER,
                SqlTypeName.BIGINT,
                SqlTypeName.DECIMAL,
                SqlTypeName.CHAR,
                SqlTypeName.VARCHAR)
            .contains(type.getValueType().getSqlTypeName());
  }

  static boolean nativeRepresentedMapUnion(RexCall call) {
    return call.getOperator().getName().equals("MAP_UNION")
        && tech.streamfusion.compat.FlinkCompat.scalarFunction(call.getOperator()) == null
        && !call.getOperands().isEmpty()
        && call.getOperands().stream()
            .allMatch(
                arg ->
                    nativeStringMap(arg.getType())
                        && org.apache.calcite.sql.type.SqlTypeUtil.equalSansNullability(
                            call.getOperands().get(0).getType(), arg.getType()));
  }

  static boolean nativeConstructorField(RelDataType type) {
    return hostSupportsConstructorField(type)
        && Set.of(
                SqlTypeName.BOOLEAN,
                SqlTypeName.TINYINT,
                SqlTypeName.SMALLINT,
                SqlTypeName.INTEGER,
                SqlTypeName.BIGINT,
                SqlTypeName.DECIMAL,
                SqlTypeName.CHAR,
                SqlTypeName.VARCHAR)
            .contains(type.getSqlTypeName());
  }

  static boolean nativeRepresentedMapConstructor(RexCall call) {
    if (call.getKind() != SqlKind.MAP_VALUE_CONSTRUCTOR
        || call.getOperands().isEmpty()
        || call.getOperands().size() % 2 != 0
        || !(JsonStringIdentity.containsCharacter(call.getType())
            || containsRowType(call.getType()))) return false;
    Set<RexNode> keys = new java.util.HashSet<>();
    for (int i = 0; i < call.getOperands().size(); i++) {
      RexNode node = call.getOperands().get(i);
      RelDataType type = node.getType();
      if (!(nativeConstructorField(type)
          || type.getSqlTypeName() == SqlTypeName.ROW
              && type.getFieldList().stream()
                  .allMatch(field -> nativeConstructorField(field.getType())))) return false;
      if (node instanceof RexInputRef && !type.isNullable()) return false;
      if (i % 2 == 0
          && (!(node instanceof RexLiteral || node instanceof RexInputRef) || !keys.add(node)))
        return false;
    }
    return true;
  }

  static boolean nativeBasicMap(RelDataType type) {
    return type.getSqlTypeName() == SqlTypeName.MAP
        && java.util.stream.Stream.of(type.getKeyType(), type.getValueType())
            .allMatch(
                t ->
                    t.isNullable()
                        && java.util.Set.of(
                                SqlTypeName.BOOLEAN,
                                SqlTypeName.TINYINT,
                                SqlTypeName.SMALLINT,
                                SqlTypeName.INTEGER,
                                SqlTypeName.BIGINT,
                                SqlTypeName.DECIMAL)
                            .contains(t.getSqlTypeName()));
  }

  static boolean nativeInputMapProjection(RexCall call) {
    if (tech.streamfusion.compat.FlinkCompat.scalarFunction(call.getOperator()) != null
        || !java.util.Set.of("MAP_KEYS", "MAP_VALUES", "MAP_ENTRIES")
            .contains(call.getOperator().getName())
        || call.getOperands().size() != 1
        || !(call.getOperands().get(0) instanceof RexInputRef
            || call.getOperands().get(0) instanceof RexCall
                && (nativeRepresentedMapConstructor((RexCall) call.getOperands().get(0))
                    || nativeCollectionCast((RexCall) call.getOperands().get(0))))) return false;
    var type = call.getOperands().get(0).getType();
    if (call.getOperands().get(0) instanceof RexCall) return true;
    if (type.getSqlTypeName() == SqlTypeName.MAP
        && type.getKeyType().isNullable()
        && type.getValueType().isNullable()
        && !CollectionExpressionSemantics.fixedBinary(type)
        && nestedFieldSupported(type.getKeyType(), true)
        && nestedFieldSupported(type.getValueType(), true)) return true;
    if (type.getSqlTypeName() != SqlTypeName.MAP || !type.getValueType().isNullable()) return false;
    boolean basicValue =
        java.util.Set.of(
                SqlTypeName.BOOLEAN,
                SqlTypeName.TINYINT,
                SqlTypeName.SMALLINT,
                SqlTypeName.INTEGER,
                SqlTypeName.BIGINT,
                SqlTypeName.DECIMAL)
            .contains(type.getValueType().getSqlTypeName());
    boolean binaryTransport = tech.streamfusion.arrow.ArrowConversion.nestedScalarUdfsEnabled();
    return type.getKeyType().getSqlTypeName() == SqlTypeName.VARCHAR
            && (basicValue
                || "MAP_VALUES".equals(call.getOperator().getName())
                    && type.getValueType().getSqlTypeName() == SqlTypeName.BINARY
                    && binaryTransport)
        || type.getKeyType().getSqlTypeName() == SqlTypeName.BINARY
            && type.getKeyType().isNullable()
            && basicValue
            && binaryTransport
            && "MAP_KEYS".equals(call.getOperator().getName());
  }

  static boolean nativeObjectUpdate(RexCall call) {
    var args = call.getOperands();
    if (!call.getOperator().getName().equals("OBJECT_UPDATE")
        || tech.streamfusion.compat.FlinkCompat.scalarFunction(call.getOperator()) != null
        || args.size() < 3
        || args.size() % 2 != 1
        || !(args.get(0) instanceof RexCall)
        || !nativeObjectOf((RexCall) args.get(0))) return false;
    var original = (RexCall) args.get(0);
    for (int i = 2; i < original.getOperands().size(); i += 2)
      if (!(original.getOperands().get(i) instanceof RexLiteral
          || original.getOperands().get(i) instanceof RexInputRef)) return false;
    for (int i = 1; i < args.size(); i += 2) {
      if (!(args.get(i) instanceof RexLiteral)
          || ((RexLiteral) args.get(i)).isNull()
          || !(args.get(i + 1) instanceof RexLiteral || args.get(i + 1) instanceof RexInputRef))
        return false;
    }
    return true;
  }

  static boolean nativeObjectOf(RexCall call) {
    var args = call.getOperands();
    if (!call.getOperator().getName().equals("OBJECT_OF")
        || tech.streamfusion.compat.FlinkCompat.scalarFunction(call.getOperator()) != null
        || args.size() < 3
        || args.size() % 2 != 1
        || !(args.get(0) instanceof RexLiteral)
        || ((RexLiteral) args.get(0)).isNull()
        || call.getType().getFieldCount() != (args.size() - 1) / 2) return false;
    for (int i = 1; i < args.size(); i += 2) {
      if (!(args.get(i) instanceof RexLiteral)
          || ((RexLiteral) args.get(i)).isNull()
          || !java.util.Set.of(
                  SqlTypeName.BOOLEAN,
                  SqlTypeName.TINYINT,
                  SqlTypeName.SMALLINT,
                  SqlTypeName.INTEGER,
                  SqlTypeName.BIGINT,
                  SqlTypeName.DECIMAL,
                  SqlTypeName.CHAR,
                  SqlTypeName.VARCHAR)
              .contains(args.get(i + 1).getType().getSqlTypeName())) return false;
    }
    return true;
  }

  static boolean nativeArrayJoin(RexCall call) {
    var args = call.getOperands();
    return call.getOperator().getName().equals("ARRAY_JOIN")
        && tech.streamfusion.compat.FlinkCompat.scalarFunction(call.getOperator()) == null
        && args.size() >= 2
        && args.size() <= 3
        && args.get(0).getType().getSqlTypeName() == SqlTypeName.ARRAY
        && args.get(0).getType().getComponentType().isNullable()
        && org.apache.calcite.sql.type.SqlTypeFamily.CHARACTER.contains(
            args.get(0).getType().getComponentType())
        && args.stream()
            .skip(1)
            .allMatch(
                arg -> org.apache.calcite.sql.type.SqlTypeFamily.CHARACTER.contains(arg.getType()));
  }

  static boolean nativeJsonStringContainer(RexCall call) {
    if (!NativeConfig.collectionExpressionsEnabled()
        || !call.getOperator().getName().equals("JSON_STRING")
        || call.getOperands().size() != 1
        || !(call.getOperands().get(0) instanceof RexInputRef)) return false;
    RelDataType type = call.getOperands().get(0).getType();
    return (type.getSqlTypeName() == SqlTypeName.ARRAY || type.getSqlTypeName() == SqlTypeName.ROW)
        && nativeJsonContainerType(type, 0);
  }

  static boolean nativeJsonContainerType(RelDataType type, int depth) {
    if (depth > 32) return false;
    switch (type.getSqlTypeName()) {
      case ARRAY:
        return type.getComponentType().isNullable()
            && nativeJsonContainerType(type.getComponentType(), depth + 1);
      case ROW:
        return type.getFieldList().stream()
            .allMatch(
                field ->
                    field.getType().isNullable()
                        && !JsonStringIdentity.containsUnpairedSurrogate(field.getName())
                        && field.getName().chars().noneMatch(c -> c < 32 || c == '"' || c == '\\')
                        && nativeJsonContainerType(field.getType(), depth + 1));
      case DECIMAL:
        return tech.streamfusion.compat.JsonRuntimeCompat.PRESERVES_DECIMAL_SCALE;
      case CHAR:
      case VARCHAR:
      case BOOLEAN:
      case TINYINT:
      case SMALLINT:
      case INTEGER:
      case BIGINT:
        return true;
      default:
        return false;
    }
  }

  static boolean nativeCollectionCast(RexCall call) {
    if (!(call.getKind() == SqlKind.CAST
            || call.getOperator()
                == org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable.TRY_CAST)
        || call.getOperands().size() != 1) return false;
    RelDataType source = call.getOperands().get(0).getType(), target = call.getType();
    if (source.getSqlTypeName() == SqlTypeName.ARRAY) {
      if (target.getSqlTypeName() == SqlTypeName.ARRAY)
        return nativeContainerElementCast(source.getComponentType(), target.getComponentType());
      return SqlTypeFamily.CHARACTER.contains(target)
          && nativeArrayFormat(source.getComponentType());
    }
    if (source.getSqlTypeName() == SqlTypeName.MAP && target.getSqlTypeName() == SqlTypeName.MAP) {
      return source.getKeyType().isNullable()
          && target.getKeyType().isNullable()
          && source.getValueType().isNullable()
          && target.getValueType().isNullable()
          && ((SqlTypeFamily.CHARACTER.contains(source.getKeyType())
                  && org.apache.calcite.sql.type.SqlTypeUtil.equalSansNullability(
                      source.getKeyType(), target.getKeyType()))
              || SqlTypeFamily.INTEGER.contains(source.getKeyType())
                  && SqlTypeFamily.INTEGER.contains(target.getKeyType()))
          && nativeContainerElementCast(source.getValueType(), target.getValueType());
    }
    return source.getSqlTypeName() == SqlTypeName.MULTISET
        && target.getSqlTypeName() == SqlTypeName.MULTISET
        && source.getComponentType().isNullable()
        && target.getComponentType().isNullable()
        && SqlTypeFamily.INTEGER.contains(source.getComponentType())
        && SqlTypeFamily.INTEGER.contains(target.getComponentType());
  }

  static boolean nativeContainerElementCast(RelDataType source, RelDataType target) {
    if (!source.isNullable() || !target.isNullable()) return false;
    if (source.getSqlTypeName() == SqlTypeName.ARRAY
        && target.getSqlTypeName() == SqlTypeName.ARRAY)
      return nativeContainerElementCast(source.getComponentType(), target.getComponentType());
    return nativeExactArrayCast(source, target)
        || SqlTypeFamily.CHARACTER.contains(source) && SqlTypeFamily.INTEGER.contains(target)
        || (SqlTypeFamily.INTEGER.contains(source)
                || source.getSqlTypeName() == SqlTypeName.DECIMAL)
            && target.getSqlTypeName() == SqlTypeName.BOOLEAN;
  }

  static boolean nativeArrayFormat(RelDataType type) {
    if (!type.isNullable()) return false;
    return type.getSqlTypeName() == SqlTypeName.ARRAY
        ? nativeArrayFormat(type.getComponentType())
        : SqlTypeFamily.INTEGER.contains(type)
            || type.getSqlTypeName() == SqlTypeName.DECIMAL
            || type.getSqlTypeName() == SqlTypeName.BOOLEAN;
  }

  static boolean nativeExactArrayCast(RelDataType source, RelDataType target) {
    if (source.getSqlTypeName() == SqlTypeName.ARRAY
        && target.getSqlTypeName() == SqlTypeName.ARRAY) {
      return source.getComponentType().isNullable()
          && target.getComponentType().isNullable()
          && nativeExactArrayCast(source.getComponentType(), target.getComponentType());
    }
    return (SqlTypeFamily.INTEGER.contains(source)
            || source.getSqlTypeName() == SqlTypeName.DECIMAL)
        && (SqlTypeFamily.INTEGER.contains(target)
            || target.getSqlTypeName() == SqlTypeName.DECIMAL);
  }
}
