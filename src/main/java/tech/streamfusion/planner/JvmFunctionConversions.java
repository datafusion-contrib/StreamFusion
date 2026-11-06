package tech.streamfusion.planner;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexCallBinding;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.flink.table.data.conversion.DataStructureConverters;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.planner.functions.bridging.BridgingSqlFunction;
import org.apache.flink.table.planner.functions.inference.OperatorBindingCallContext;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalCorrelate;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.TypeInferenceUtil;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.types.Row;

/** Verifies the released external Java conversions selected for JVM function calls. */
final class JvmFunctionConversions {
  private JvmFunctionConversions() {}

  static boolean defaults(RexCall call) {
    if (!(call.getOperator() instanceof BridgingSqlFunction)) return false;
    try {
      var inferred = infer(call);
      return inferred.getExpectedArgumentTypes().stream().allMatch(JvmFunctionConversions::defaults)
          && defaults(inferred.getOutputDataType());
    } catch (RuntimeException failure) {
      return false;
    }
  }

  static boolean verifiedScalar(RexCall call) {
    if (!(call.getOperator() instanceof BridgingSqlFunction)
        || !JvmFunctionTypes.nested(call.getType())
        || call.getOperands().stream().anyMatch(arg -> !JvmFunctionTypes.nested(arg.getType())))
      return false;
    try {
      var inferred = infer(call);
      return inferred.getExpectedArgumentTypes().stream()
              .allMatch(JvmFunctionConversions::verifiedArgumentConversion)
          && verifiedResultConversion(inferred.getOutputDataType());
    } catch (RuntimeException failure) {
      return false;
    }
  }

  static TypeInferenceUtil.Result infer(RexCall call) {
    var function = (BridgingSqlFunction) call.getOperator();
    var context =
        new OperatorBindingCallContext(
            function.getDataTypeFactory(),
            function.getDefinition(),
            RexCallBinding.create(function.getTypeFactory(), call, null, Collections.emptyList()),
            call.getType());
    return TypeInferenceUtil.runTypeInference(function.getTypeInference(), context, null);
  }

  static boolean verifiedArgumentConversion(DataType type) {
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

  static boolean verifiedResultConversion(DataType type) {
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

  static boolean argumentAliases(DataType type) {
    if (type.getLogicalType().is(LogicalTypeRoot.BINARY)
        || type.getLogicalType().is(LogicalTypeRoot.VARBINARY)) return true;
    if (type.getLogicalType().is(LogicalTypeRoot.ARRAY)
        && type.getConversionClass().isArray()
        && identity(type.getChildren().get(0))) return true;
    return type.getChildren().stream().anyMatch(JvmFunctionConversions::argumentAliases);
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
    return type.getChildren().stream().anyMatch(JvmFunctionConversions::resultAliases);
  }

  private static boolean identity(DataType type) {
    return DataStructureConverters.getConverter(type).isIdentityConversion();
  }

  static boolean aliasedProducer(RelNode node) {
    if (node instanceof StreamPhysicalNativeTableFunction
        && ((StreamPhysicalNativeTableFunction) node).returnsAliasedValues()) return true;
    if (node instanceof StreamPhysicalCorrelate) {
      var call = ((StreamPhysicalCorrelate) node).scan().getCall();
      if (call instanceof RexCall
          && ((RexCall) call).getOperator() instanceof BridgingSqlFunction
          && ((BridgingSqlFunction) ((RexCall) call).getOperator()).getDefinition()
              instanceof TableFunction
          && returnsAliasedValues((RexCall) call)) return true;
    }
    if (node instanceof StreamPhysicalNativeCalc
        && scalarResultsAlias(((StreamPhysicalNativeCalc) node).sourceProgram())) return true;
    if (node instanceof Calc && scalarResultsAlias(((Calc) node).getProgram())) return true;
    return node.getInputs().stream().anyMatch(JvmFunctionConversions::aliasedProducer);
  }

  private static boolean scalarResultsAlias(RexProgram program) {
    if (program == null) return false;
    boolean[] aliased = {false};
    var visitor =
        new RexVisitorImpl<Void>(true) {
          @Override
          public Void visitCall(RexCall call) {
            if (tech.streamfusion.compat.FlinkCompat.scalarFunction(call.getOperator()) != null
                && returnsAliasedValues(call)) aliased[0] = true;
            return super.visitCall(call);
          }
        };
    for (var expression : program.getExprList()) expression.accept(visitor);
    return aliased[0];
  }

  static boolean returnsAliasedValues(RexCall call) {
    try {
      return resultAliases(infer(call).getOutputDataType());
    } catch (RuntimeException failure) {
      return true;
    }
  }

  static boolean preservesArguments(RexCall call, boolean aliasedInput) {
    if (!aliasedInput) return true;
    if (!(call.getOperator() instanceof BridgingSqlFunction)) return false;
    try {
      return infer(call).getExpectedArgumentTypes().stream()
          .noneMatch(JvmFunctionConversions::argumentAliases);
    } catch (RuntimeException failure) {
      return false;
    }
  }

  private static boolean defaults(DataType type) {
    return type.getConversionClass() == type.getLogicalType().getDefaultConversion()
        && type.getChildren().stream().allMatch(JvmFunctionConversions::defaults);
  }
}
