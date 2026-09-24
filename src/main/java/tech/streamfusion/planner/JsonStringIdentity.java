package tech.streamfusion.planner;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalCalc;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalSink;

/** Keeps sensitive Java UTF-16 results inside a generated expression or at the final output boundary. */
final class JsonStringIdentity {
  private JsonStringIdentity() {}

  static boolean containsSensitiveString(RexNode expression) {
    if (expression instanceof RexLiteral literal
        && !literal.isNull()
        && SqlTypeFamily.CHARACTER.contains(literal.getType())) {
      return containsUnpairedSurrogate(literal.getValueAs(String.class));
    }
    if (expression instanceof RexFieldAccess access) {
      return containsSensitiveString(access.getReferenceExpr());
    }
    if (!(expression instanceof RexCall call)) return false;
    if (call.getOperator().getName().equals("REGEXP_EXTRACT_ALL")
        || call.getOperator().getName().equals("STR_TO_MAP")) return true;
    if (SqlTypeFamily.CHARACTER.contains(call.getType())
        && (call.getOperator().getName().equals("JSON_VALUE")
            || call.getOperator().getName().equals("JSON_QUERY")
            || call.getOperator().getName().equals("JSON_UNQUOTE")
            || call.getOperator().getName().equals("PRINTF")
            // Runtime charsets include CESU-8, which can return isolated UTF-16 surrogates.
            || call.getOperator().getName().equals("DECODE")
                && call.getOperands().size() == 2
                && !(call.getOperands().get(1) instanceof RexLiteral)
            || call.getOperator().getName().equals("FROM_BASE64"))) {
      return true;
    }
    return call.getOperands().stream().anyMatch(JsonStringIdentity::containsSensitiveString);
  }

  static boolean containsUnpairedSurrogate(String value) {
    for (int i = 0; i < value.length(); i++) {
      char unit = value.charAt(i);
      if (Character.isHighSurrogate(unit)) {
        if (++i == value.length() || !Character.isLowSurrogate(value.charAt(i))) return true;
      } else if (Character.isLowSurrogate(unit)) {
        return true;
      }
    }
    return false;
  }

  static boolean crossesOperatorBoundary(RelNode root, boolean finalOutput) {
    return crossesOperatorBoundary(root, null, finalOutput);
  }

  private static boolean crossesOperatorBoundary(
      RelNode node, RelNode parent, boolean finalOutput) {
    if (node instanceof StreamPhysicalCalc calc
        && !(parent == null && finalOutput)
        && !(parent instanceof StreamPhysicalSink)) {
      var program = calc.getProgram();
      for (var projection : program.getProjectList()) {
        RexNode expression = program.expandLocalRef(projection);
        if (containsCharacter(expression.getType()) && containsSensitiveString(expression))
          return true;
      }
    }
    for (RelNode input : node.getInputs()) {
      if (crossesOperatorBoundary(input, node, false)) return true;
    }
    return false;
  }

  static boolean containsBinaryString(RexNode expression) {
    if (expression instanceof RexFieldAccess access) {
      return containsBinaryString(access.getReferenceExpr());
    }
    return expression instanceof RexCall call
        && (call.getOperator().getName().equals("FROM_BASE64")
            || call.getOperands().stream().anyMatch(JsonStringIdentity::containsBinaryString));
  }

  static boolean projectsBinaryString(RelNode node) {
    if (node instanceof StreamPhysicalCalc calc) {
      var program = calc.getProgram();
      for (var projection : program.getProjectList()) {
        RexNode expression = program.expandLocalRef(projection);
        if (containsCharacter(expression.getType()) && containsBinaryString(expression)) return true;
      }
    }
    return node.getInputs().stream().anyMatch(JsonStringIdentity::projectsBinaryString);
  }

  static boolean containsCharacter(RelDataType type) {
    if (SqlTypeFamily.CHARACTER.contains(type)) return true;
    if (type.getComponentType() != null && containsCharacter(type.getComponentType())) return true;
    if (type.getKeyType() != null && containsCharacter(type.getKeyType())) return true;
    if (type.getValueType() != null && containsCharacter(type.getValueType())) return true;
    return type.isStruct()
        && type.getFieldList().stream().anyMatch(field -> containsCharacter(field.getType()));
  }
}
