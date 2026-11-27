package tech.streamfusion.planner.stream;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.functions.BuiltInFunctionDefinition;
import org.apache.flink.table.functions.FunctionKind;
import org.apache.flink.table.functions.UserDefinedFunctionHelper;
import org.apache.flink.table.planner.codegen.calls.BridgingFunctionGenUtil.DefaultExpressionEvaluatorFactory;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.functions.bridging.BridgingSqlFunction;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalCorrelate;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalSink;
import org.apache.flink.table.planner.utils.ShortcutUtils;
import org.apache.flink.table.runtime.functions.table.BuiltInTableFunction;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.TypeInferenceUtil;
import org.apache.flink.table.types.logical.LogicalTypeFamily;
import tech.streamfusion.planner.ColumnarInput;
import tech.streamfusion.planner.ColumnarOutput;
import tech.streamfusion.planner.JvmFunctionConversions;
import tech.streamfusion.planner.JvmFunctionTypes;

/** Admission for synchronous built-ins using Flink's internal character string conversions. */
final class BuiltinTableFunctionMatcher {
  private BuiltinTableFunctionMatcher() {}

  static boolean hasShape(StreamPhysicalCorrelate node) {
    if (!(node.scan().getCall() instanceof RexCall) || GeneratedUnnestMatcher.hasShape(node))
      return false;
    var operator = ((RexCall) node.scan().getCall()).getOperator();
    if (!(operator instanceof BridgingSqlFunction)) return false;
    var definition = ((BridgingSqlFunction) operator).getDefinition();
    return definition instanceof BuiltInFunctionDefinition
        && definition.getKind() == FunctionKind.TABLE;
  }

  static boolean matches(StreamPhysicalCorrelate node) {
    var join = UnnestMatcher.joinType(node);
    if ((join != JoinRelType.INNER && join != JoinRelType.LEFT)
        || node.condition().isDefined()
        || node.getInput() instanceof ColumnarOutput
        || !JvmFunctionTypes.scalarRow(node.getInput().getRowType())
        || !JvmFunctionTypes.scalarRow(node.getRowType())) return false;

    var result = node.scan().getRowType();
    var call = (RexCall) node.scan().getCall();
    return result.getFieldCount() == 1
        && SqlTypeFamily.CHARACTER.contains(result.getFieldList().get(0).getType())
        && !call.getOperands().isEmpty()
        && call.getOperands().stream().allMatch(BuiltinTableFunctionMatcher::stringOperand)
        && verifiedConversions(node, call);
  }

  private static boolean stringOperand(RexNode operand) {
    return SqlTypeFamily.CHARACTER.contains(operand.getType())
        && (operand instanceof RexLiteral
            || operand instanceof RexInputRef
            || (operand instanceof RexFieldAccess
                && ((RexFieldAccess) operand).getReferenceExpr() instanceof RexCorrelVariable));
  }

  private static boolean verifiedConversions(StreamPhysicalCorrelate node, RexCall call) {
    var function = (BridgingSqlFunction) call.getOperator();
    var definition = (BuiltInFunctionDefinition) function.getDefinition();
    if (definition.getRuntimeClass().isEmpty()) return false;
    try {
      // The SQL definition supplies logical types. Flink selects the Java conversions only
      // after specialization, using the original call, including its literal arguments.
      var context = JvmFunctionConversions.callContext(call);
      var config = ShortcutUtils.unwrapTableConfig(node);
      var evaluatorFactory =
          new DefaultExpressionEvaluatorFactory(
              config, ShortcutUtils.unwrapClassLoader(node), function.getRexFactory());
      var runtime =
          UserDefinedFunctionHelper.createSpecializedFunction(
              function.getName(),
              definition,
              context,
              PlannerBase.class.getClassLoader(),
              config,
              evaluatorFactory);
      if (!(runtime instanceof BuiltInTableFunction)) return false;
      var inference = runtime.getTypeInference(function.getDataTypeFactory());
      var inferred = TypeInferenceUtil.runTypeInference(inference, context, null);
      return inferred.getExpectedArgumentTypes().stream()
              .allMatch(BuiltinTableFunctionMatcher::internalString)
          && internalString(inferred.getOutputDataType());
    } catch (RuntimeException failure) {
      return false;
    }
  }

  private static boolean internalString(DataType type) {
    return type.getLogicalType().is(LogicalTypeFamily.CHARACTER_STRING)
        && type.getConversionClass() == StringData.class;
  }

  static boolean crossesStringBoundary(RelNode node, boolean finalRowOutput) {
    if (node instanceof StreamPhysicalNativeTableFunction
        && ((StreamPhysicalNativeTableFunction) node).usesInternalStrings()) return !finalRowOutput;
    boolean rowSink = node instanceof StreamPhysicalSink && !(node instanceof ColumnarInput);
    return node.getInputs().stream().anyMatch(input -> crossesStringBoundary(input, rowSink));
  }

  static String unsupportedReason(StreamPhysicalCorrelate node) {
    return "correlate: builtin table functions require INNER or LEFT ON TRUE, scalar row input,"
        + " and character column or literal arguments with verified internal StringData"
        + " conversions and one atomic character result";
  }
}
