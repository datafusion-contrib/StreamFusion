package tech.streamfusion.planner;

import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.util.List;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;

/** Resolves released Flink scalar overloads without registering or encoding a function. */
final class UdfCallResolver {
  private UdfCallResolver() {}

  /** The best public {@code eval} for the operands, or null when the Java overload is ambiguous. */
  static Method resolveEval(Object function, List<RexNode> args) {
    Method match = null;
    int bestScore = Integer.MIN_VALUE;
    for (Method method : function.getClass().getMethods()) {
      if (!method.getName().equals("eval")) {
        continue;
      }
      Class<?>[] parameters = method.getParameterTypes();
      int fixedArity = method.isVarArgs() ? parameters.length - 1 : parameters.length;
      if ((!method.isVarArgs() && parameters.length != args.size()) || args.size() < fixedArity) {
        continue;
      }
      int score = method.isVarArgs() ? 0 : 1;
      boolean compatible = true;
      for (int i = 0; i < args.size(); i++) {
        Class<?> parameter =
            i < fixedArity ? parameters[i] : parameters[parameters.length - 1].getComponentType();
        int argumentScore = udfParameterScore(parameter, args.get(i).getType());
        if (argumentScore < 0) {
          compatible = false;
          break;
        }
        score += argumentScore;
      }
      if (!compatible || score < bestScore) {
        continue;
      }
      if (score == bestScore) {
        match = null;
      } else {
        match = method;
        bestScore = score;
      }
    }
    return match;
  }

  static Class<?> defaultExternalClass(RelDataType type) {
    return org.apache.flink.table.types.utils.TypeConversions.fromLogicalToDataType(
            org.apache.flink.table.planner.calcite.FlinkTypeFactory.toLogicalType(type))
        .getConversionClass();
  }

  static int udfParameterScore(Class<?> parameter, RelDataType type) {
    if (parameter.isPrimitive()) {
      if (parameter == int.class) parameter = Integer.class;
      else if (parameter == long.class) parameter = Long.class;
      else if (parameter == double.class) parameter = Double.class;
      else if (parameter == float.class) parameter = Float.class;
      else if (parameter == short.class) parameter = Short.class;
      else if (parameter == byte.class) parameter = Byte.class;
      else if (parameter == boolean.class) parameter = Boolean.class;
    }
    Class<?> argument;
    switch (type.getSqlTypeName()) {
      case ARRAY:
        return parameter == defaultExternalClass(type) ? 2 : -1;
      case CHAR:
      case VARCHAR:
        argument = String.class;
        break;
      case BIGINT:
        argument = Long.class;
        break;
      case INTEGER:
        argument = Integer.class;
        break;
      case DOUBLE:
        argument = Double.class;
        break;
      case FLOAT:
      case REAL:
        argument = Float.class;
        break;
      case SMALLINT:
        argument = Short.class;
        break;
      case TINYINT:
        argument = Byte.class;
        break;
      case BOOLEAN:
        argument = Boolean.class;
        break;
      case DECIMAL:
        argument = BigDecimal.class;
        break;
      case VARBINARY:
        argument = byte[].class;
        break;
      case ROW:
        argument = org.apache.flink.types.Row.class;
        break;
      case MAP:
        argument = java.util.Map.class;
        break;
      case DATE:
        return parameter == java.time.LocalDate.class ? 2 : -1;
      case TIME:
        return parameter == java.time.LocalTime.class ? 2 : -1;
      case TIMESTAMP:
        return parameter == java.time.LocalDateTime.class ? 2 : -1;
      case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
        return parameter == java.time.Instant.class ? 2 : -1;
      default:
        return -1;
    }
    if (parameter == argument) {
      return 2;
    }
    return parameter.isAssignableFrom(argument) ? 1 : -1;
  }
}
