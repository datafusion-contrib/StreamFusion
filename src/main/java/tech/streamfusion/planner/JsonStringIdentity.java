package tech.streamfusion.planner;

import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Set;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Union;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalCalc;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalSink;

/** Keeps sensitive Java UTF-16 results inside a generated expression or at the final output boundary. */
final class JsonStringIdentity {
  private JsonStringIdentity() {}

  static boolean containsSensitiveString(RexNode expression) {
    if (expression instanceof RexLiteral
        && !((RexLiteral) expression).isNull()
        && SqlTypeFamily.CHARACTER.contains(((RexLiteral) expression).getType())) {
      RexLiteral literal = ((RexLiteral) expression);

      return containsUnpairedSurrogate(literal.getValueAs(String.class));
    }
    if (expression instanceof RexFieldAccess) {
      RexFieldAccess access = ((RexFieldAccess) expression);

      return containsSensitiveString(access.getReferenceExpr());
    }
    if (!(expression instanceof RexCall)) return false;
    RexCall call = ((RexCall) expression);

    if (call.getOperator().getName().equals("REGEXP_EXTRACT_ALL")
        || call.getOperator().getName().equals("STR_TO_MAP")) return true;
    if (SqlTypeFamily.CHARACTER.contains(call.getType())
        && (call.getOperator().getName().equals("JSON_VALUE")
            || call.getOperator().getName().equals("JSON_QUERY")
            || call.getOperator().getName().equals("JSON_UNQUOTE")
            || call.getOperator().getName().equals("PRINTF")
            || call.getOperator().getName().equals("LPAD")
            || call.getOperator().getName().equals("RPAD")
            || call.getOperator().getName().equals("OVERLAY")
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

  private static boolean containsDecodedBinaryCast(RexNode expression) {
    if (expression instanceof RexFieldAccess)
      return containsDecodedBinaryCast(((RexFieldAccess) expression).getReferenceExpr());
    if (!(expression instanceof RexCall)) return false;
    var call = (RexCall) expression;
    boolean decoded = (call.getKind() == SqlKind.CAST
            || call.getOperator()
                == org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable.TRY_CAST)
        && SqlTypeFamily.CHARACTER.contains(call.getType())
        && call.getOperands().size() == 1
        && SqlTypeFamily.BINARY.contains(call.getOperands().get(0).getType());
    return decoded || call.getOperands().stream().anyMatch(JsonStringIdentity::containsDecodedBinaryCast);
  }

  static boolean crossesOperatorBoundary(RelNode root, boolean finalOutput) {
    return finalUnionJsonInputs(root, finalOutput) == null;
  }

  static Set<RelNode> finalUnionJsonInputs(RelNode root, boolean finalOutput) {
    Set<RelNode> calcs = Collections.newSetFromMap(new IdentityHashMap<>());
    return collectUnionProjections(root, null, finalOutput, finalOutput, calcs) ? calcs : null;
  }

  private static boolean collectUnionProjections(
      RelNode node, RelNode parent, boolean finalOutput, boolean forwarding, Set<RelNode> calcs) {
    if (node instanceof StreamPhysicalCalc
        && !(parent == null && finalOutput)
        && !(parent instanceof StreamPhysicalSink)) {
      StreamPhysicalCalc calc = ((StreamPhysicalCalc) node);

      var program = calc.getProgram();
      for (var projection : program.getProjectList()) {
        RexNode expression = program.expandLocalRef(projection);
        // A decoded cast is Java-backed in Flink; Arrow UTF-8 alone cannot preserve its cache
        // for representation-sensitive downstream comparators. Fused consumers stay in the Calc.
        if (containsCharacter(expression.getType())
            && (containsSensitiveString(expression) || containsDecodedBinaryCast(expression))) {
          if (!forwarding || !preservableJsonValue(expression)) return false;
          calcs.add(node);
        }
      }
      if (calcs.contains(node)) {
        for (var projection : program.getProjectList()) {
          RexNode expression = program.expandLocalRef(projection);
          if (preservableJsonValue(expression)
              || expression instanceof RexLiteral && ((RexLiteral) expression).isNull()) continue;
          if (containsCharacter(expression.getType()) || hasCharacterInput(expression)) return false;
        }
        if (program.getCondition() != null
            && hasCharacterInput(program.expandLocalRef(program.getCondition()))) return false;
      }
    }
    boolean next = node instanceof StreamPhysicalSink
        || forwarding && node instanceof Union && ((Union) node).all;
    for (RelNode input : node.getInputs()) {
      if (!collectUnionProjections(input, node, false, next, calcs)) return false;
    }
    return true;
  }

  private static boolean hasCharacterInput(RexNode expression) {
    if (expression instanceof RexInputRef) return containsCharacter(expression.getType());
    if (expression instanceof RexFieldAccess)
      return hasCharacterInput(((RexFieldAccess) expression).getReferenceExpr());
    return expression instanceof RexCall
        && ((RexCall) expression).getOperands().stream().anyMatch(JsonStringIdentity::hasCharacterInput);
  }

  private static boolean preservableJsonValue(RexNode expression) {
    if (!(expression instanceof RexCall)
        || !SqlTypeFamily.CHARACTER.contains(expression.getType())) return false;
    var call = (RexCall) expression;
    return call.getOperator().getName().equals("JSON_VALUE")
        && call.getOperands().get(0) instanceof RexInputRef
        && call.getOperands().stream().noneMatch(operand -> operand instanceof RexLiteral
            && containsSensitiveString(operand));
  }

  static boolean containsBinaryString(RexNode expression) {
    if (expression instanceof RexFieldAccess) {
      RexFieldAccess access = ((RexFieldAccess) expression);

      return containsBinaryString(access.getReferenceExpr());
    }
    return expression instanceof RexCall
        && (((RexCall) expression).getOperator().getName().equals("FROM_BASE64")
            || ((RexCall) expression)
                .getOperands().stream().anyMatch(JsonStringIdentity::containsBinaryString));
  }

  static boolean projectsBinaryString(RelNode node) {
    if (node instanceof StreamPhysicalCalc) {
      StreamPhysicalCalc calc = ((StreamPhysicalCalc) node);

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
