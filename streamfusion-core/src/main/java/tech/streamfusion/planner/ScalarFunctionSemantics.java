package tech.streamfusion.planner;

import static tech.streamfusion.planner.ExpressionTypeSemantics.*;
import static tech.streamfusion.planner.NumericExpressionSemantics.*;

import java.util.Locale;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;

/** Chooses exact builtin evaluation from semantic facts, independently of wire storage. */
final class ScalarFunctionSemantics {
  private ScalarFunctionSemantics() {}

  static boolean nativeFromBase64(RexCall call) {
    return call.getOperator()
            == org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable.FROM_BASE64
        && call.getOperands().size() == 1
        && (SqlTypeFamily.CHARACTER.contains(call.getOperands().get(0).getType())
            || SqlTypeFamily.BINARY.contains(call.getOperands().get(0).getType()));
  }

  static boolean needsExactScalarFunction(RexCall call) {
    if (call.getKind() == SqlKind.LIKE && call.getOperands().size() == 3
        || call.getKind() == SqlKind.SIMILAR) return true;
    if (call.getOperands().isEmpty()) return false;
    SqlTypeName input = call.getOperands().get(0).getType().getSqlTypeName();
    boolean integral;
    switch (input) {
      case TINYINT:
      case SMALLINT:
      case INTEGER:
      case BIGINT:
        integral = true;
        break;
      default:
        integral = false;
        break;
    }
    boolean exact = integral || input == SqlTypeName.DECIMAL;
    if (call.getKind() == SqlKind.MINUS_PREFIX)
      return exact && !nativeIntegralNegate(call) && !nativeDecimalNegate(call);
    switch (call.getOperator().getName().toUpperCase(Locale.ROOT)) {
      case "ABS":
        return exact && !nativeIntegralAbs(call) && !nativeDecimalAbs(call);
      case "SIGN":
        return exact && !nativeIntegralSign(call) && !nativeDecimalSign(call);
      case "FLOOR":
      case "CEIL":
      case "CEILING":
        return integral && call.getOperands().size() == 1 && !nativeIntegralRounding(call);
      case "TRUNCATE":
        return (integral
                || SqlTypeFamily.APPROXIMATE_NUMERIC.contains(call.getOperands().get(0).getType()))
            && !nativeIntegralRounding(call)
            && !nativeIntegralTruncatePosition(call);
      case "ROUND":
        return input != SqlTypeName.DECIMAL
            && SqlTypeFamily.NUMERIC.contains(call.getOperands().get(0).getType())
            && !nativeIntegralRounding(call)
            && !nativeIntegralRoundPosition(call)
            && !NativeConfig.allowsIncompatible("ROUND");
      case "EXP":
      case "LN":
      case "LOG10":
      case "SIN":
      case "COS":
      case "TAN":
      case "ASIN":
      case "ACOS":
      case "ATAN":
        return SqlTypeFamily.NUMERIC.contains(call.getOperands().get(0).getType())
            && !NativeConfig.allowsIncompatible(call.getOperator().getName());
      case "COSH":
      case "SINH":
      case "TANH":
      case "COT":
      case "ATAN2":
      case "DEGREES":
      case "RADIANS":
      case "LOG":
      case "LOG2":
        return SqlTypeFamily.NUMERIC.contains(call.getOperands().get(0).getType());
      case "TRY_CAST":
        return input == SqlTypeName.BOOLEAN
            && SqlTypeFamily.CHARACTER.contains(call.getType())
            && !nativeBooleanCharacterCast(call);
      case "REGEXP":
      case "REGEXP_REPLACE":
      case "REGEXP_COUNT":
      case "REGEXP_INSTR":
      case "REGEXP_SUBSTR":
        return true;
      case "REGEXP_EXTRACT_ALL":
      case "STR_TO_MAP":
        return true;
      case "PARSE_URL":
        return true;
      case "PRINTF":
        return nativePrintf(call) == 0;
      case "STARTSWITH":
      case "ENDSWITH":
        return false;
      case "ELT":
        return input == SqlTypeName.INTEGER && SqlTypeFamily.BINARY.contains(call.getType());
      case "FROM_BASE64":
        return !nativeFromBase64(call);
      case "IS_DECIMAL":
      case "IS_DIGIT":
      case "IS_ALPHA":
        return !nativeStringClassification(call) && !nativeNumericClassification(call);
      case "SHA2":
        return call.getOperands().size() == 2
            && (!(call.getOperands().get(1) instanceof RexLiteral)
                || ((RexLiteral) call.getOperands().get(1)).isNull());
      case "SPLIT_INDEX":
        return call.getOperands().size() == 3
            && SqlTypeFamily.INTEGER.contains(call.getOperands().get(1).getType());
      case "BTRIM":
      case "LTRIM":
      case "RTRIM":
        return call.getOperands().size() == 2 && !(call.getOperands().get(1) instanceof RexLiteral);
      case "TRIM":
        return isRuntimeSqlTrim(call);
      case "GREATEST":
      case "LEAST":
        return SqlTypeFamily.CHARACTER.contains(call.getType())
            || temporalTypeCode(call.getType()) >= 0
            || call.getType().getSqlTypeName() == SqlTypeName.DECIMAL
                && !nativeMixedExactExtremum(call)
                && call.getOperands().stream()
                    .anyMatch(
                        operand ->
                            !org.apache.calcite.sql.type.SqlTypeUtil.equalSansNullability(
                                operand.getType(), call.getType()));
      default:
        return false;
    }
  }

  static boolean containsExactScalarFunction(RexNode node) {
    return node instanceof RexCall
        && (needsExactScalarFunction(((RexCall) node))
            || ((RexCall) node)
                .getOperands().stream()
                    .anyMatch(ScalarFunctionSemantics::containsExactScalarFunction));
  }

  static boolean isRuntimeSqlTrim(RexCall call) {
    return call.getOperator()
            == org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable.TRIM
        && call.getOperands().size() == 3
        && call.getOperands().get(0) instanceof RexLiteral
        && !(call.getOperands().get(1) instanceof RexLiteral)
        && isCharacter(call.getOperands().get(1))
        && isCharacter(call.getOperands().get(2));
  }
}
