package tech.streamfusion.planner;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexCallBinding;
import org.apache.calcite.rex.RexInputRef;
import org.apache.flink.table.functions.SpecializedFunction;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.planner.functions.bridging.BridgingSqlFunction;
import org.apache.flink.table.planner.functions.inference.OperatorBindingCallContext;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalCorrelate;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalRel;
import org.apache.flink.table.planner.plan.utils.ChangelogPlanUtils;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.TypeInferenceUtil;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.types.Row;

/** Admission for released Flink JVM table-function generation inside a columnar boundary. */
final class TableFunctionMatcher {
  private TableFunctionMatcher() {}

  static boolean isUserTableFunction(StreamPhysicalCorrelate node) {
    if (!(node.scan().getCall() instanceof RexCall)) return false;
    var operator = ((RexCall) node.scan().getCall()).getOperator();
    return operator instanceof BridgingSqlFunction
        && ((BridgingSqlFunction) operator).getDefinition() instanceof TableFunction;
  }

  static boolean matches(StreamPhysicalCorrelate node) {
    var function =
        ((BridgingSqlFunction) ((RexCall) node.scan().getCall()).getOperator()).getDefinition();
    if (function instanceof SpecializedFunction) return false;
    JoinRelType join = UnnestMatcher.joinType(node);
    return (join == JoinRelType.INNER || join == JoinRelType.LEFT)
        && verifiedConversions(node)
        && (scalarBoundary(node)
            || GeneratedUnnestMatcher.verifiedInput(node.getInput())
            || projectedSource(node.getInput()) != null)
        && JvmFunctionTypes.nestedRow(node.getInput().getRowType())
        && JvmFunctionTypes.nestedRow(node.getRowType())
        && ((RexCall) node.scan().getCall())
            .getOperands().stream().allMatch(arg -> JvmFunctionTypes.nested(arg.getType()));
  }

  private static boolean scalarBoundary(StreamPhysicalCorrelate node) {
    return JvmFunctionTypes.scalarRow(node.getInput().getRowType())
        && JvmFunctionTypes.scalarRow(node.getRowType())
        && ((RexCall) node.scan().getCall())
            .getOperands().stream().allMatch(arg -> JvmFunctionTypes.scalar(arg.getType()));
  }

  private static boolean verifiedConversions(StreamPhysicalCorrelate node) {
    RexCall call = (RexCall) node.scan().getCall();
    boolean scalar = scalarBoundary(node);
    if (scalar && !aliasedProducer(node.getInput())) return true;
    try {
      var inferred = conversions(call);
      return (scalar
              || (inferred.getExpectedArgumentTypes().stream()
                      .allMatch(TableFunctionMatcher::verifiedArgumentConversion)
                  && verifiedOutputConversion(inferred.getOutputDataType())))
          && (inferred.getExpectedArgumentTypes().stream()
                  .noneMatch(TableFunctionMatcher::argumentAliases)
              || !aliasedProducer(node.getInput()));
    } catch (RuntimeException failure) {
      return false;
    }
  }

  private static boolean verifiedArgumentConversion(DataType type) {
    return verifiedConversion(type, false);
  }

  private static boolean temporalConversion(DataType type) {
    Class<?> conversion = type.getConversionClass();
    switch (type.getLogicalType().getTypeRoot()) {
      case DATE:
        return conversion == Integer.class
            || conversion == java.sql.Date.class
            || (!type.getLogicalType().isNullable() && conversion == int.class);
      case TIME_WITHOUT_TIME_ZONE:
        return conversion == Integer.class
            || conversion == Long.class
            || conversion == java.sql.Time.class
            || (!type.getLogicalType().isNullable()
                && (conversion == int.class || conversion == long.class));
      case TIMESTAMP_WITHOUT_TIME_ZONE:
        return conversion == java.sql.Timestamp.class;
      case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
        return conversion == Integer.class
            || conversion == Long.class
            || conversion == java.sql.Timestamp.class
            || (!type.getLogicalType().isNullable()
                && (conversion == int.class || conversion == long.class));
      case INTERVAL_YEAR_MONTH:
        return conversion == Integer.class
            || (!type.getLogicalType().isNullable() && conversion == int.class);
      case INTERVAL_DAY_TIME:
        return conversion == Long.class
            || (!type.getLogicalType().isNullable() && conversion == long.class);
      default:
        return false;
    }
  }

  private static boolean verifiedOutputConversion(DataType type) {
    if (type.getLogicalType().is(LogicalTypeRoot.ROW) && type.getConversionClass() == Row.class)
      return type.getChildren().stream().allMatch(TableFunctionMatcher::verifiedResultConversion);
    return verifiedResultConversion(type);
  }

  private static boolean verifiedResultConversion(DataType type) {
    return verifiedConversion(type, true);
  }

  private static boolean verifiedConversion(DataType type, boolean result) {
    return verifiedConversion(type, result, false);
  }

  private static boolean verifiedConversion(DataType type, boolean result, boolean encoded) {
    Class<?> conversion = type.getConversionClass();
    switch (type.getLogicalType().getTypeRoot()) {
      case ROW:
        if (conversion != Row.class) return false;
        break;
      case MAP:
        if (conversion != Map.class) return false;
        break;
      case ARRAY:
        if (!conversion.isArray()
            && !(result ? List.class.isAssignableFrom(conversion) : conversion == List.class))
          return false;
        if (conversion.isArray() && conversion.getComponentType().isPrimitive()) {
          var element = type.getChildren().get(0);
          if (!primitiveConversion(element)
              && !(primitiveTemporalArray(element) && (!result || encoded))) return false;
        }
        break;
      default:
        return conversion == type.getLogicalType().getDefaultConversion()
            || primitiveConversion(type)
            || temporalConversion(type);
    }
    // Non-identity nested converters materialize arrays/maps in Flink's binary writers.
    boolean copied =
        encoded
            || type.getLogicalType().is(LogicalTypeRoot.ARRAY)
            || type.getLogicalType().is(LogicalTypeRoot.MAP);
    return type.getChildren().stream().allMatch(child -> verifiedConversion(child, result, copied));
  }

  private static boolean argumentAliases(DataType type) {
    if (type.getLogicalType().is(LogicalTypeRoot.BINARY)
        || type.getLogicalType().is(LogicalTypeRoot.VARBINARY)) return true;
    if (type.getLogicalType().is(LogicalTypeRoot.ARRAY)
        && type.getConversionClass().isArray()
        && identity(type.getChildren().get(0))) return true;
    return type.getChildren().stream().anyMatch(TableFunctionMatcher::argumentAliases);
  }

  private static boolean resultAliases(DataType type) {
    switch (type.getLogicalType().getTypeRoot()) {
      case ARRAY:
        var child = type.getChildren().get(0);
        // Non-identity elements are serialized; List.toArray copies the outer List itself.
        if (!identity(child)) return false;
        return type.getConversionClass().isArray() || resultAliases(child);
      case MAP:
        if (type.getChildren().stream().anyMatch(field -> !identity(field))) return false;
        break;
      default:
        break;
    }
    if (type.getLogicalType().is(LogicalTypeRoot.BINARY)
        || type.getLogicalType().is(LogicalTypeRoot.VARBINARY)) return true;
    return type.getChildren().stream().anyMatch(TableFunctionMatcher::resultAliases);
  }

  private static boolean identity(DataType type) {
    return org.apache.flink.table.data.conversion.DataStructureConverters.getConverter(type)
        .isIdentityConversion();
  }

  private static boolean aliasedProducer(RelNode node) {
    if (node instanceof StreamPhysicalNativeTableFunction
        && ((StreamPhysicalNativeTableFunction) node).returnsAliasedValues()) return true;
    return node.getInputs().stream().anyMatch(TableFunctionMatcher::aliasedProducer);
  }

  static boolean returnsAliasedValues(RexCall call) {
    try {
      return resultAliases(conversions(call).getOutputDataType());
    } catch (RuntimeException failure) {
      return true;
    }
  }

  private static TypeInferenceUtil.Result conversions(RexCall call) {
    var function = (BridgingSqlFunction) call.getOperator();
    var context =
        new OperatorBindingCallContext(
            function.getDataTypeFactory(),
            function.getDefinition(),
            RexCallBinding.create(function.getTypeFactory(), call, null, Collections.emptyList()),
            call.getType());
    return TypeInferenceUtil.runTypeInference(function.getTypeInference(), context, null);
  }

  private static boolean primitiveTemporalArray(DataType element) {
    switch (element.getLogicalType().getTypeRoot()) {
      case DATE:
      case TIME_WITHOUT_TIME_ZONE:
      case INTERVAL_YEAR_MONTH:
        return element.getConversionClass() == int.class && temporalConversion(element);
      case INTERVAL_DAY_TIME:
        return element.getConversionClass() == long.class && temporalConversion(element);
      default:
        return false;
    }
  }

  private static boolean primitiveConversion(DataType type) {
    if (type.getLogicalType().isNullable()) return false;
    Class<?> primitive;
    switch (type.getLogicalType().getTypeRoot()) {
      case BOOLEAN:
        primitive = boolean.class;
        break;
      case TINYINT:
        primitive = byte.class;
        break;
      case SMALLINT:
        primitive = short.class;
        break;
      case INTEGER:
        primitive = int.class;
        break;
      case BIGINT:
        primitive = long.class;
        break;
      case FLOAT:
        primitive = float.class;
        break;
      case DOUBLE:
        primitive = double.class;
        break;
      default:
        return false;
    }
    return type.getConversionClass() == primitive;
  }

  private static final class ProjectedSource {
    final RelNode source;
    final org.apache.calcite.rel.type.RelDataType type;

    ProjectedSource(RelNode source, org.apache.calcite.rel.type.RelDataType type) {
      this.source = source;
      this.type = type;
    }
  }

  private static boolean copyStableProjectionType(org.apache.calcite.rel.type.RelDataType type) {
    switch (type.getSqlTypeName()) {
      case BOOLEAN:
      case TINYINT:
      case SMALLINT:
      case INTEGER:
      case BIGINT:
        return true;
      case ROW:
        return type.getFieldList().stream()
            .allMatch(field -> copyStableProjectionType(field.getType()));
      default:
        return false;
    }
  }

  private static ProjectedSource projectedSource(RelNode input) {
    if (input.getRowType().getFieldList().stream()
        .anyMatch(field -> !copyStableProjectionType(field.getType()))) return null;
    RelNode current = input;
    int[] mapping =
        java.util.stream.IntStream.range(0, input.getRowType().getFieldCount()).toArray();
    while (current instanceof StreamPhysicalNativeCalc) {
      var program = ((StreamPhysicalNativeCalc) current).sourceProgram();
      if (program == null || program.getCondition() != null) return null;
      int[] columns = new int[program.getProjectList().size()];
      for (int i = 0; i < columns.length; i++) {
        var expression = program.expandLocalRef(program.getProjectList().get(i));
        if (expression instanceof RexCall
            && expression.getKind() == org.apache.calcite.sql.SqlKind.AS)
          expression = ((RexCall) expression).getOperands().get(0);
        if (!(expression instanceof RexInputRef)) return null;
        columns[i] = ((RexInputRef) expression).getIndex();
      }
      mapping = java.util.Arrays.stream(mapping).map(index -> columns[index]).toArray();
      RelNode source = current.getInput(0);
      if (source instanceof StreamPhysicalRowDataToArrow) source = source.getInput(0);
      if (!source.getRowType().equals(program.getInputRowType())) return null;
      if (source instanceof StreamPhysicalNativeCalc) {
        current = source;
        continue;
      }
      if (source instanceof ColumnarOutput) return null;
      var type = input.getCluster().getTypeFactory().builder();
      var selected = new java.util.HashSet<Integer>();
      for (int i = 0; i < mapping.length; i++) {
        if (!selected.add(mapping[i])) return null;
        var field = source.getRowType().getFieldList().get(mapping[i]);
        if (!input.getRowType().getFieldList().get(i).getType().equals(field.getType()))
          return null;
        // Correlate reads ordinals and applies its declared output aliases. Entry pruning needs
        // source names so its owned copy selects the same fields across stacked projections.
        type.add(field.getName(), field.getType());
      }
      return new ProjectedSource(source, type.build());
    }
    return null;
  }

  static String unsupportedReason(StreamPhysicalCorrelate node) {
    return "correlate: only non-specialized JVM table functions with INNER/LEFT and CHAR/VARCHAR,"
        + " numeric, boolean, binary, interval, temporal or recursively verified"
        + " ARRAY/MAP/ROW input, output and argument types with verified external Java"
        + " conversions (recursive external arrays, Lists, ROW/MAP, List implementation results,"
        + " non-null primitive arrays and verified primitive/temporal scalar bridges),"
        + " verified input representation and safe ownership (chained mutable aliases and"
        + " unencoded temporal-array results excluded) are supported";
  }

  static RelNode substitute(StreamPhysicalCorrelate node, PlanContext context) {
    RelNode input = node.getInput();
    var projected = scalarBoundary(node) ? null : projectedSource(input);
    if (projected != null) {
      RelNode source = projected.source;
      boolean kinds =
          source instanceof StreamPhysicalRel
              && !ChangelogPlanUtils.isInsertOnly((StreamPhysicalRel) source);
      input =
          new StreamPhysicalRowDataToArrow(
              source.getCluster(), source.getTraitSet(), source, kinds, projected.type, true);
    }
    return new StreamPhysicalNativeTableFunction(
        node.getCluster(),
        node.getTraitSet(),
        input,
        node.getRowType(),
        (RexCall) node.scan().getCall(),
        node.condition().isDefined() ? node.condition().get() : null,
        UnnestMatcher.joinType(node) == JoinRelType.LEFT);
  }
}
