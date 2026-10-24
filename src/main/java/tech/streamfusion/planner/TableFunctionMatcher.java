package tech.streamfusion.planner;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
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
import org.apache.flink.table.types.logical.LogicalTypeFamily;
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
    if (scalarBoundary(node)) return true;
    var function = (BridgingSqlFunction) call.getOperator();
    try {
      var context =
          new OperatorBindingCallContext(
              function.getDataTypeFactory(),
              function.getDefinition(),
              RexCallBinding.create(function.getTypeFactory(), call, null, Collections.emptyList()),
              call.getType());
      var inferred = TypeInferenceUtil.runTypeInference(function.getTypeInference(), context, null);
      return inferred.getExpectedArgumentTypes().stream()
              .allMatch(TableFunctionMatcher::verifiedArgumentConversion)
          && verifiedOutputConversion(inferred.getOutputDataType());
    } catch (RuntimeException failure) {
      return false;
    }
  }

  private static boolean verifiedArgumentConversion(DataType type) {
    return verifiedConversion(type) || primitiveConversion(type) || epochDayConversion(type);
  }

  private static boolean epochDayConversion(DataType type) {
    return type.getLogicalType().is(LogicalTypeRoot.DATE)
        && !type.getLogicalType().isNullable()
        && type.getConversionClass() == int.class;
  }

  private static boolean verifiedOutputConversion(DataType type) {
    if (type.getLogicalType().is(LogicalTypeRoot.ROW) && type.getConversionClass() == Row.class)
      return type.getChildren().stream().allMatch(TableFunctionMatcher::verifiedResultConversion);
    return verifiedResultConversion(type);
  }

  private static boolean verifiedResultConversion(DataType type) {
    if (verifiedConversion(type) || primitiveConversion(type) || epochDayConversion(type))
      return true;
    if (!type.getLogicalType().is(LogicalTypeRoot.ARRAY)
        || type.getConversionClass() != ArrayList.class) return false;
    var element = type.getChildren().get(0);
    return defaultScalarConversion(element);
  }

  private static boolean verifiedConversion(DataType type) {
    if (defaultConversions(type)) return true;
    if (!type.getLogicalType().is(LogicalTypeRoot.ARRAY)) return false;
    var element = type.getChildren().get(0);
    if (type.getConversionClass() == List.class) return defaultScalarConversion(element);
    return type.getConversionClass().isArray()
        && type.getConversionClass().getComponentType().isPrimitive()
        && primitiveConversion(element);
  }

  private static boolean defaultScalarConversion(DataType type) {
    return type.getLogicalType().is(LogicalTypeFamily.PREDEFINED) && defaultConversions(type);
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

  private static boolean defaultConversions(DataType type) {
    return type.getConversionClass() == type.getLogicalType().getDefaultConversion()
        && type.getChildren().stream().allMatch(TableFunctionMatcher::defaultConversions);
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
        + " conversions (default containers, scalar Lists, scalar ArrayList results,"
        + " non-null primitive arrays or direct non-null boolean/numeric primitive scalar"
        + " arguments or results, or non-null DATE/int arguments or results) and input"
        + " representation are verified";
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
