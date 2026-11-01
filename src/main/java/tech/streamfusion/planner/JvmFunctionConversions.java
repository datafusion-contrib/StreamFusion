package tech.streamfusion.planner;

import java.util.Collections;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexCallBinding;
import org.apache.flink.table.planner.functions.bridging.BridgingSqlFunction;
import org.apache.flink.table.planner.functions.inference.OperatorBindingCallContext;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.TypeInferenceUtil;

/** Checks the actual released Java conversions selected by custom function inference. */
final class JvmFunctionConversions {
  private JvmFunctionConversions() {}

  static boolean defaults(RexCall call) {
    if (!(call.getOperator() instanceof BridgingSqlFunction)) return false;
    var function = (BridgingSqlFunction) call.getOperator();
    try {
      var context =
          new OperatorBindingCallContext(
              function.getDataTypeFactory(),
              function.getDefinition(),
              RexCallBinding.create(function.getTypeFactory(), call, null, Collections.emptyList()),
              call.getType());
      var inferred = TypeInferenceUtil.runTypeInference(function.getTypeInference(), context, null);
      return inferred.getExpectedArgumentTypes().stream().allMatch(JvmFunctionConversions::defaults)
          && defaults(inferred.getOutputDataType());
    } catch (RuntimeException failure) {
      return false;
    }
  }

  private static boolean defaults(DataType type) {
    return type.getConversionClass() == type.getLogicalType().getDefaultConversion()
        && type.getChildren().stream().allMatch(JvmFunctionConversions::defaults);
  }
}
