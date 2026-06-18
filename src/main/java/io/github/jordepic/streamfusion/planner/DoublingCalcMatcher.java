package io.github.jordepic.streamfusion.planner;

import java.util.List;
import java.util.Objects;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;

/**
 * Recognizes the one projection the native operator currently implements: a single int column
 * multiplied by two, with no filter. The native operator computes exactly this, so substituting it
 * only when the plan matches keeps results identical and lets everything else fall back to the
 * host engine.
 */
final class DoublingCalcMatcher {

  private DoublingCalcMatcher() {}

  static boolean matches(Calc calc) {
    RexProgram program = calc.getProgram();
    if (program.getCondition() != null) {
      return false;
    }
    RelDataType inputType = calc.getInput().getRowType();
    if (inputType.getFieldCount() != 1
        || inputType.getFieldList().get(0).getType().getSqlTypeName() != SqlTypeName.INTEGER) {
      return false;
    }
    List<RexLocalRef> projects = program.getProjectList();
    if (projects.size() != 1) {
      return false;
    }
    return isInputTimesTwo(program.expandLocalRef(projects.get(0)));
  }

  private static boolean isInputTimesTwo(RexNode expr) {
    if (!(expr instanceof RexCall)) {
      return false;
    }
    RexCall call = (RexCall) expr;
    if (call.getKind() != SqlKind.TIMES || call.getOperands().size() != 2) {
      return false;
    }
    RexNode left = call.getOperands().get(0);
    RexNode right = call.getOperands().get(1);
    return (isInputRefZero(left) && isLiteralTwo(right))
        || (isInputRefZero(right) && isLiteralTwo(left));
  }

  private static boolean isInputRefZero(RexNode node) {
    return node instanceof RexInputRef && ((RexInputRef) node).getIndex() == 0;
  }

  private static boolean isLiteralTwo(RexNode node) {
    return node instanceof RexLiteral
        && Objects.equals(((RexLiteral) node).getValueAs(Integer.class), 2);
  }
}
