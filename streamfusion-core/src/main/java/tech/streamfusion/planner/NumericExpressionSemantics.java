package tech.streamfusion.planner;

import static tech.streamfusion.planner.ExpressionTypeSemantics.*;

import java.util.List;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import tech.streamfusion.planner.common.CastTarget;
import tech.streamfusion.planner.common.ScalarOp;

/** Pure admission and value-range proofs for exact numeric and formatting expressions. */
final class NumericExpressionSemantics {
  private NumericExpressionSemantics() {}

  static boolean isAsciiLiteralResult(RexNode node) {
    if (node instanceof RexLiteral) {
      RexLiteral literal = ((RexLiteral) node);

      String value = literal.getValueAs(String.class);
      return value == null || value.chars().allMatch(ch -> ch < 128);
    }
    if (node instanceof RexCall && ((RexCall) node).getKind() == SqlKind.CASE) {
      RexCall call = ((RexCall) node);

      List<RexNode> operands = call.getOperands();
      for (int i = 1; i < operands.size() - 1; i += 2) {
        if (!isAsciiLiteralResult(operands.get(i))) {
          return false;
        }
      }
      return isAsciiLiteralResult(operands.get(operands.size() - 1));
    }
    return false;
  }

  static boolean nativeMixedExactExtremum(RexCall call) {
    RelDataType result = call.getType();
    String name = call.getOperator().getName();
    if (!("GREATEST".equalsIgnoreCase(name) || "LEAST".equalsIgnoreCase(name))
        || result.getSqlTypeName() != SqlTypeName.DECIMAL
        || call.getOperands().size() < 2) return false;
    for (RexNode operand : call.getOperands()) {
      if (!losslessExactDecimalLeaf(operand, result)) return false;
      RelDataType source = operand.getType();
      if (source.getSqlTypeName() == SqlTypeName.DECIMAL) {
        // Released numericCasting dereferences a DECIMAL before checking its NULL flag.
        if (source.isNullable()
            && !org.apache.calcite.sql.type.SqlTypeUtil.equalSansNullability(source, result)) {
          return false;
        }
      }
    }
    return true;
  }

  static boolean losslessExactDecimalLeaf(RexNode operand, RelDataType result) {
    if (!(operand instanceof RexInputRef || operand instanceof RexLiteral)) return false;
    RelDataType source = operand.getType();
    int integerDigits = result.getPrecision() - result.getScale();
    if (SqlTypeFamily.INTEGER.contains(source)) return integerDigits >= source.getPrecision();
    return source.getSqlTypeName() == SqlTypeName.DECIMAL
        && source.getScale() <= result.getScale()
        && source.getPrecision() - source.getScale() <= integerDigits;
  }

  static boolean nativeIntegralNegate(RexCall call) {
    return call.getOperator()
            == org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable.UNARY_MINUS
        && call.getOperands().size() == 1
        && SqlTypeFamily.INTEGER.contains(call.getOperands().get(0).getType())
        && call.getType().getSqlTypeName() == call.getOperands().get(0).getType().getSqlTypeName()
        && nativeIntegralNegateOperand(call.getOperands().get(0));
  }

  static boolean nativeIntegralNegateOperand(RexNode node) {
    if (node instanceof RexInputRef || node instanceof RexLiteral) return true;
    if (!(node instanceof RexCall)) return false;
    RexCall call = (RexCall) node;
    if (nativeIntegralNegate(call)
        || nativeIntegralArithmetic(call, NumericExpressionSemantics::nativeIntegralNegateOperand))
      return true;
    return call.getKind() == SqlKind.CAST
        && call.getOperands().size() == 1
        && SqlTypeFamily.INTEGER.contains(call.getType())
        && wideningTargetCode(
                call.getOperands().get(0).getType().getSqlTypeName(),
                call.getType().getSqlTypeName())
            >= 0
        && nativeIntegralNegateOperand(call.getOperands().get(0));
  }

  static boolean nativeBooleanCharacterCast(RexCall call) {
    return call.getOperands().size() == 1
        && SqlTypeFamily.CHARACTER.contains(call.getType())
        && nativeBooleanTextOperand(call.getOperands().get(0));
  }

  static boolean nativeBooleanTextOperand(RexNode operand) {
    return nativeBooleanFormattingOperand(
        operand,
        producer ->
            nativeIntegralBooleanTryCast(producer)
                || nativeDecimalBooleanTryCast(producer)
                || nativeDirectDecimalPredicate(producer));
  }

  static int nativePrintf(RexCall call) {
    if (!(call.getOperator()
        instanceof org.apache.flink.table.planner.functions.bridging.BridgingSqlFunction)) return 0;
    var definition =
        ((org.apache.flink.table.planner.functions.bridging.BridgingSqlFunction) call.getOperator())
            .getDefinition();
    if (!(definition instanceof org.apache.flink.table.functions.BuiltInFunctionDefinition)
        || !"PRINTF"
            .equalsIgnoreCase(
                ((org.apache.flink.table.functions.BuiltInFunctionDefinition) definition).getName())
        || call.getOperands().isEmpty()
        || !(call.getOperands().get(0) instanceof RexLiteral)) return 0;
    var format = (RexLiteral) call.getOperands().get(0);
    if (!isCharacter(format) || format.isNull()) return 0;
    boolean[] booleanArguments = new boolean[call.getOperands().size() - 1];
    boolean[] stringArguments = new boolean[booleanArguments.length];
    boolean hasBoolean = false;
    for (int i = 1; i < call.getOperands().size(); i++) {
      RexNode operand = call.getOperands().get(i);
      if (operand.getType().getSqlTypeName() == SqlTypeName.BOOLEAN) {
        if (!nativeBooleanTextOperand(operand)) return 0;
        booleanArguments[i - 1] = true;
        hasBoolean = true;
      } else if (nativeIntegerTextProducer(operand) != null) {
        stringArguments[i - 1] = true;
      } else if (!SqlTypeFamily.INTEGER.contains(operand.getType())
          || !nativeIntegralRoundingOperand(operand)) return 0;
    }
    String pattern = format.getValueAs(String.class);
    return IntegerPrintfFormat.supports(pattern, booleanArguments, stringArguments)
        ? (hasBoolean ? ScalarOp.PRINTF_BOOLEAN_ARGUMENTS : ScalarOp.PRINTF_INTEGER)
        : 0;
  }

  static RexNode nativeIntegerTextProducer(RexNode operand) {
    if (!(operand instanceof RexCall)) return null;
    RexNode integer = ScalarCastRules.integerTextOperand((RexCall) operand);
    return integer != null && nativeIntegralRoundingOperand(integer) ? integer : null;
  }

  static RexNode nativeIntegerTextDecimal(RexCall call) {
    if (!(call.getOperator()
                == org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable.CAST
            || call.getOperator()
                == org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable.TRY_CAST)
        || call.getOperands().size() != 1
        || call.getType().getSqlTypeName() != SqlTypeName.DECIMAL) return null;
    RexNode integer = nativeIntegerTextProducer(call.getOperands().get(0));
    if (integer == null) return null;
    int digits;
    switch (integer.getType().getSqlTypeName()) {
      case TINYINT:
        digits = 3;
        break;
      case SMALLINT:
        digits = 5;
        break;
      case INTEGER:
        digits = 10;
        break;
      case BIGINT:
        digits = 19;
        break;
      default:
        return null;
    }
    // Full-width capacity makes parsing total; integer text has no extra leading zeros.
    return call.getType().getPrecision() - call.getType().getScale() >= digits ? integer : null;
  }

  static int nativePrimitiveCase(RexCall call) {
    int op =
        call.getOperator()
                == org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable.UPPER
            ? 50
            : call.getOperator()
                    == org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable.LOWER
                ? 51
                : 0;
    if (op == 0
        || call.getOperands().size() != 1
        || !(call.getOperands().get(0) instanceof RexCall)) return 0;
    var input = (RexCall) call.getOperands().get(0);
    if (nativeIntegerTextProducer(input) != null) return op;
    if (nativePrimitiveBooleanText(input)) return op;
    // Primitive conversions cannot write i/I. Exclude literals that observe Turkish/Azeri casing.
    return nativePrintf(input) != 0
            && ((RexLiteral) input.getOperands().get(0))
                    .getValueAs(String.class)
                    .indexOf(op == ScalarOp.UPPER ? 'i' : 'I')
                < 0
        ? op
        : 0;
  }

  static boolean nativePrimitiveBooleanText(RexCall call) {
    return (call.getOperator()
                == org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable.CAST
            || call.getOperator()
                == org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable.TRY_CAST)
        && call.getType().getSqlTypeName() == SqlTypeName.VARCHAR
        && call.getType().getPrecision() == Integer.MAX_VALUE
        && nativeBooleanCharacterCast(call);
  }

  static boolean nativePrimitiveAsciiResult(RexNode node) {
    if (!isCharacter(node)) return false;
    if (node instanceof RexLiteral) return isAsciiLiteralResult(node);
    if (!(node instanceof RexCall)) return false;
    var call = (RexCall) node;
    return nativeIntegerTextProducer(call) != null
        || nativePrimitiveBooleanText(call)
        || nativePrintf(call) != 0
        || nativePrimitiveCase(call) != 0;
  }

  static Boolean primitiveBooleanTextLiteral(RexNode operand, boolean lowercase) {
    if (!(operand instanceof RexLiteral)
        || !isCharacter(operand)
        || ((RexLiteral) operand).isNull()) return null;
    String value = ((RexLiteral) operand).getValueAs(String.class);
    if (operand.getType().getSqlTypeName() == SqlTypeName.CHAR
        && operand.getType().getPrecision() != value.length()) return null;
    if (value.equals(lowercase ? "true" : "TRUE")) return true;
    if (value.equals(lowercase ? "false" : "FALSE")) return false;
    return null;
  }

  static boolean nativePrimitivePrintfComparison(RexCall call) {
    var operator = call.getOperator();
    if (call.getOperands().size() != 2
        || !(operator == org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable.EQUALS
            || operator
                == org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable.NOT_EQUALS
            || operator
                == org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable.LESS_THAN
            || operator
                == org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable
                    .LESS_THAN_OR_EQUAL
            || operator
                == org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable.GREATER_THAN
            || operator
                == org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable
                    .GREATER_THAN_OR_EQUAL)) return false;
    boolean formatted = false;
    for (RexNode operand : call.getOperands()) {
      if (!isCharacter(operand)) return false;
      if (operand instanceof RexLiteral && isAsciiLiteralResult(operand)) continue;
      if (!(operand instanceof RexCall) || nativePrintf((RexCall) operand) == 0) return false;
      formatted = true;
    }
    return formatted;
  }

  static boolean nativeIntegralBooleanTryCast(RexCall call) {
    return call.getOperator()
            == org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable.TRY_CAST
        && call.getOperands().size() == 1
        && call.getType().getSqlTypeName() == SqlTypeName.BOOLEAN
        && SqlTypeFamily.INTEGER.contains(call.getOperands().get(0).getType())
        && nativeIntegralRoundingOperand(call.getOperands().get(0));
  }

  static boolean nativeDecimalBooleanTryCast(RexCall call) {
    return call.getOperator()
            == org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable.TRY_CAST
        && call.getOperands().size() == 1
        && call.getType().getSqlTypeName() == SqlTypeName.BOOLEAN
        && call.getOperands().get(0).getType().getSqlTypeName() == SqlTypeName.DECIMAL
        && (call.getOperands().get(0) instanceof RexInputRef
            || call.getOperands().get(0) instanceof RexLiteral);
  }

  static boolean nativeDirectDecimalPredicate(RexCall call) {
    switch (call.getKind()) {
      case IS_NULL:
      case IS_NOT_NULL:
        return call.getOperands().size() == 1
            && nativeDirectDecimalOperand(call.getOperands().get(0));
      case EQUALS:
      case NOT_EQUALS:
      case LESS_THAN:
      case LESS_THAN_OR_EQUAL:
      case GREATER_THAN:
      case GREATER_THAN_OR_EQUAL:
        return call.getOperands().size() == 2
            && call.getOperands().stream()
                .allMatch(NumericExpressionSemantics::nativeDirectDecimalOperand)
            && org.apache.calcite.sql.type.SqlTypeUtil.equalSansNullability(
                call.getOperands().get(0).getType(), call.getOperands().get(1).getType());
      default:
        return false;
    }
  }

  static boolean nativeDirectDecimalOperand(RexNode node) {
    return node.getType().getSqlTypeName() == SqlTypeName.DECIMAL
        && (node instanceof RexInputRef || node instanceof RexLiteral);
  }

  static boolean nativeBooleanFormattingOperand(RexNode node) {
    return nativeBooleanFormattingOperand(node, call -> false);
  }

  static boolean nativeBooleanFormattingOperand(
      RexNode node, java.util.function.Predicate<RexCall> additionalProducer) {
    if (node.getType().getSqlTypeName() != SqlTypeName.BOOLEAN) return false;
    if (node instanceof RexInputRef || node instanceof RexLiteral) return true;
    if (!(node instanceof RexCall)) return false;
    var call = (RexCall) node;
    if (additionalProducer.test(call)) return true;
    switch (call.getKind()) {
      case IS_NULL:
      case IS_NOT_NULL:
        return call.getOperands().size() == 1
            && (nativeBooleanFormattingOperand(call.getOperands().get(0), additionalProducer)
                || SqlTypeFamily.INTEGER.contains(call.getOperands().get(0).getType())
                    && nativeIntegralRoundingOperand(call.getOperands().get(0)));
      case NOT:
      case IS_TRUE:
      case IS_NOT_TRUE:
      case IS_FALSE:
      case IS_NOT_FALSE:
        return call.getOperands().size() == 1
            && nativeBooleanFormattingOperand(call.getOperands().get(0), additionalProducer);
      case AND:
      case OR:
        return call.getOperands().size() >= 2
            && call.getOperands().stream()
                .allMatch(operand -> nativeBooleanFormattingOperand(operand, additionalProducer));
      case EQUALS:
      case NOT_EQUALS:
      case LESS_THAN:
      case LESS_THAN_OR_EQUAL:
      case GREATER_THAN:
      case GREATER_THAN_OR_EQUAL:
        return call.getOperands().size() == 2
            && call.getOperands().stream()
                .allMatch(
                    operand ->
                        SqlTypeFamily.INTEGER.contains(operand.getType())
                            && nativeIntegralRoundingOperand(operand));
      default:
        return false;
    }
  }

  static boolean nativeIntegralRounding(RexCall call) {
    return (call.getOperator()
                == org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable.FLOOR
            || call.getOperator()
                == org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable.CEIL
            || call.getOperator()
                == org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable.ROUND
            || call.getOperator()
                == org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable.TRUNCATE)
        && call.getOperands().size() == 1
        && SqlTypeFamily.INTEGER.contains(call.getOperands().get(0).getType())
        && call.getType().getSqlTypeName() == call.getOperands().get(0).getType().getSqlTypeName()
        && (call.getOperator()
                != org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable.TRUNCATE
            || call.getType().getSqlTypeName() == SqlTypeName.INTEGER
            || call.getType().getSqlTypeName() == SqlTypeName.BIGINT)
        && nativeIntegralRoundingOperand(call.getOperands().get(0));
  }

  static boolean nativeStringClassification(RexCall call) {
    return call.getOperands().size() == 1 && isCharacter(call.getOperands().get(0));
  }

  static boolean nativeNumericClassification(RexCall call) {
    if (call.getOperands().size() != 1) return false;
    RexNode operand = call.getOperands().get(0);
    return (SqlTypeFamily.INTEGER.contains(operand.getType())
            || operand.getType().getSqlTypeName() == SqlTypeName.DECIMAL)
        && (operand instanceof RexInputRef
            || operand instanceof RexLiteral
            || SqlTypeFamily.INTEGER.contains(operand.getType())
                && nativeIntegralRoundingOperand(operand));
  }

  static boolean nativeIntegralRoundPosition(RexCall call) {
    return call.getOperator()
            == org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable.ROUND
        && nativeIntegralRoundingPosition(call);
  }

  static boolean nativeIntegralTruncatePosition(RexCall call) {
    return call.getOperator()
            == org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable.TRUNCATE
        && (call.getType().getSqlTypeName() == SqlTypeName.INTEGER
            || call.getType().getSqlTypeName() == SqlTypeName.BIGINT)
        && nativeIntegralRoundingPosition(call);
  }

  static boolean nativeIntegralRoundingPosition(RexCall call) {
    if (call.getOperands().size() != 2
        || !SqlTypeFamily.INTEGER.contains(call.getOperands().get(0).getType())
        || call.getType().getSqlTypeName() != call.getOperands().get(0).getType().getSqlTypeName()
        || call.getOperands().get(1).getType().getSqlTypeName() != SqlTypeName.INTEGER
        || !nativeIntegralRoundingOperand(call.getOperands().get(0))) return false;
    RexNode position = call.getOperands().get(1);
    if (nativeBoundedIntegralPosition(position)) return true;
    IntegralPositionRange range = integralPositionRange(position);
    return range != null && (range.isNullOnly() || range.lower >= -38 && range.upper <= 38);
  }

  static boolean nativeBoundedIntegralPosition(RexNode node) {
    return nativeBoundedIntegralCase(node, value -> value >= -38 && value <= 38);
  }

  static boolean nativeBoundedIntegralCase(RexNode node, java.util.function.IntPredicate admitted) {
    if (node.getType().getSqlTypeName() != SqlTypeName.INTEGER) return false;
    if (node instanceof RexLiteral) {
      var position = (RexLiteral) node;
      Integer value = position.getValueAs(Integer.class);
      return position.isNull() || value != null && admitted.test(value);
    }
    if (!(node instanceof RexCall)) return false;
    var call = (RexCall) node;
    if (call.getKind() != SqlKind.CASE
        || call.getOperands().size() < 3
        || call.getOperands().size() % 2 != 1) return false;
    for (int i = 0; i < call.getOperands().size() - 1; i += 2) {
      if (!nativeBooleanFormattingOperand(call.getOperands().get(i))
          || !nativeBoundedIntegralCase(call.getOperands().get(i + 1), admitted)) return false;
    }
    return nativeBoundedIntegralCase(
        call.getOperands().get(call.getOperands().size() - 1), admitted);
  }

  static IntegralPositionRange integralPositionRange(RexNode node) {
    if (node.getType().getSqlTypeName() != SqlTypeName.INTEGER) return null;
    if (node instanceof RexLiteral) {
      var literal = (RexLiteral) node;
      if (literal.isNull()) return new IntegralPositionRange(1, 0);
      Integer value = literal.getValueAs(Integer.class);
      return value == null ? null : new IntegralPositionRange(value, value);
    }
    if (node instanceof RexCall) {
      var call = (RexCall) node;
      if (call.getKind() == SqlKind.CASE
          && call.getOperands().size() >= 3
          && call.getOperands().size() % 2 == 1) {
        int lower = Integer.MAX_VALUE;
        int upper = Integer.MIN_VALUE;
        for (int i = 0; i < call.getOperands().size(); i++) {
          if (i % 2 == 0 && i < call.getOperands().size() - 1) {
            if (!nativeBooleanFormattingOperand(call.getOperands().get(i))) return null;
          } else {
            IntegralPositionRange branch = integralPositionRange(call.getOperands().get(i));
            if (branch == null) return null;
            if (!branch.isNullOnly()) {
              lower = Math.min(lower, branch.lower);
              upper = Math.max(upper, branch.upper);
            }
          }
        }
        return new IntegralPositionRange(lower, upper);
      }
      boolean greatest =
          isBuiltinDefinition(
              call, org.apache.flink.table.functions.BuiltInFunctionDefinitions.GREATEST);
      boolean least =
          isBuiltinDefinition(
              call, org.apache.flink.table.functions.BuiltInFunctionDefinitions.LEAST);
      if ((greatest || least) && call.getOperands().size() >= 2) {
        int lower = greatest ? Integer.MIN_VALUE : Integer.MAX_VALUE;
        int upper = lower;
        boolean nullOnly = false;
        for (RexNode operand : call.getOperands()) {
          IntegralPositionRange range = integralPositionRange(operand);
          if (range == null) return null;
          nullOnly |= range.isNullOnly();
          lower = greatest ? Math.max(lower, range.lower) : Math.min(lower, range.lower);
          upper = greatest ? Math.max(upper, range.upper) : Math.min(upper, range.upper);
        }
        return nullOnly ? new IntegralPositionRange(1, 0) : new IntegralPositionRange(lower, upper);
      }
    }
    return nativeIntegralRoundingOperand(node)
        ? new IntegralPositionRange(Integer.MIN_VALUE, Integer.MAX_VALUE)
        : null;
  }

  static boolean nativeIntegralRoundingOperand(RexNode node) {
    if (node instanceof RexInputRef || node instanceof RexLiteral) return true;
    if (!(node instanceof RexCall)) return false;
    RexCall call = (RexCall) node;
    if (nativeIntegralRounding(call)
        || nativeIntegralRoundPosition(call)
        || nativeIntegralTruncatePosition(call)
        || nativeIntegralArithmetic(
            call, NumericExpressionSemantics::nativeIntegralRoundingOperand)) return true;
    return call.getKind() == SqlKind.CAST
        && call.getOperands().size() == 1
        && SqlTypeFamily.INTEGER.contains(call.getType())
        && wideningTargetCode(
                call.getOperands().get(0).getType().getSqlTypeName(),
                call.getType().getSqlTypeName())
            >= 0
        && nativeIntegralRoundingOperand(call.getOperands().get(0));
  }

  static boolean nativeDecimalSign(RexCall call) {
    var type = call.getType();
    return call.getOperator()
            == org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable.SIGN
        && call.getOperands().size() == 1
        && type.getSqlTypeName() == SqlTypeName.DECIMAL
        && (type.getPrecision() > type.getScale() || type.getPrecision() > 18)
        && org.apache.calcite.sql.type.SqlTypeUtil.equalSansNullability(
            type, call.getOperands().get(0).getType())
        && nativeDecimalSignOperand(call.getOperands().get(0));
  }

  static boolean nativeDecimalSignOperand(RexNode node) {
    if (node.getType().getSqlTypeName() != SqlTypeName.DECIMAL) return false;
    if (node instanceof RexInputRef || node instanceof RexLiteral) return true;
    if (!(node instanceof RexCall)) return false;
    RexCall call = (RexCall) node;
    if (nativeDecimalSign(call) || nativeDecimalAbs(call) || nativeDecimalNegate(call)) return true;
    if (isDecimalArithmetic(call)
        && (call.getKind() == SqlKind.PLUS
            || call.getKind() == SqlKind.MINUS
            || call.getKind() == SqlKind.TIMES)) {
      return call.getOperands().stream()
          .allMatch(NumericExpressionSemantics::nativeDecimalSignOperand);
    }
    if (call.getKind() != SqlKind.CAST || call.getOperands().size() != 1) return false;
    var source = call.getOperands().get(0).getType();
    var target = call.getType();
    return source.getSqlTypeName() == SqlTypeName.DECIMAL
        && target.getSqlTypeName() == SqlTypeName.DECIMAL
        && source.getScale() == target.getScale()
        && source.getPrecision() <= target.getPrecision()
        && nativeDecimalSignOperand(call.getOperands().get(0));
  }

  static boolean nativeIntegralSign(RexCall call) {
    return call.getOperator()
            == org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable.SIGN
        && call.getOperands().size() == 1
        && java.util.Set.of(SqlTypeName.INTEGER, SqlTypeName.BIGINT)
            .contains(call.getOperands().get(0).getType().getSqlTypeName())
        && call.getType().getSqlTypeName() == call.getOperands().get(0).getType().getSqlTypeName()
        && nativeIntegralSignOperand(call.getOperands().get(0));
  }

  static boolean nativeIntegralSignOperand(RexNode node) {
    if (node instanceof RexInputRef || node instanceof RexLiteral) return true;
    if (!(node instanceof RexCall)) return false;
    RexCall call = (RexCall) node;
    if (nativeIntegralSign(call)
        || nativeIntegralArithmetic(call, NumericExpressionSemantics::nativeIntegralSignOperand))
      return true;
    return call.getKind() == SqlKind.CAST
        && call.getOperands().size() == 1
        && SqlTypeFamily.INTEGER.contains(call.getType())
        && wideningTargetCode(
                call.getOperands().get(0).getType().getSqlTypeName(),
                call.getType().getSqlTypeName())
            >= 0
        && nativeIntegralSignOperand(call.getOperands().get(0));
  }

  static boolean nativeDecimalAbs(RexCall call) {
    return nativeDecimalArithmeticUnary(
        call, org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable.ABS);
  }

  static boolean nativeDecimalNegate(RexCall call) {
    return nativeDecimalArithmeticUnary(
        call, org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable.UNARY_MINUS);
  }

  static boolean nativeDecimalArithmeticUnary(RexCall call, SqlOperator operator) {
    return nativeDecimalUnary(
        call, operator, node -> nativeDecimalArithmeticUnaryOperand(node, operator));
  }

  static boolean nativeDecimalUnary(RexCall call, SqlOperator operator) {
    return nativeDecimalUnary(call, operator, node -> nativeDecimalUnaryOperand(node, operator));
  }

  static boolean nativeDecimalUnary(
      RexCall call, SqlOperator operator, java.util.function.Predicate<RexNode> operand) {
    return call.getOperator() == operator
        && call.getOperands().size() == 1
        && call.getType().getSqlTypeName() == SqlTypeName.DECIMAL
        && org.apache.calcite.sql.type.SqlTypeUtil.equalSansNullability(
            call.getType(), call.getOperands().get(0).getType())
        && operand.test(call.getOperands().get(0));
  }

  static boolean nativeDecimalArithmeticUnaryOperand(RexNode node, SqlOperator operator) {
    if (node.getType().getSqlTypeName() != SqlTypeName.DECIMAL) return false;
    if (node instanceof RexInputRef || node instanceof RexLiteral) return true;
    if (!(node instanceof RexCall)) return false;
    RexCall call = (RexCall) node;
    if (nativeDecimalArithmeticUnary(call, operator)
        || (operator == org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable.ABS
            && nativeDecimalArithmeticUnary(
                call,
                org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable.UNARY_MINUS))
        || (operator
                == org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable.UNARY_MINUS
            && nativeDecimalArithmeticUnary(
                call, org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable.ABS))) {
      return true;
    }
    if (isDecimalArithmetic(call)
        && (call.getKind() == SqlKind.PLUS
            || call.getKind() == SqlKind.MINUS
            || call.getKind() == SqlKind.TIMES)) {
      return call.getOperands().stream()
          .allMatch(operand -> nativeDecimalArithmeticUnaryOperand(operand, operator));
    }
    if (call.getKind() != SqlKind.CAST || call.getOperands().size() != 1) return false;
    RelDataType input = call.getOperands().get(0).getType();
    return input.getSqlTypeName() == SqlTypeName.DECIMAL
        && call.getType().getPrecision() >= input.getPrecision()
        && call.getType().getScale() == input.getScale()
        && nativeDecimalArithmeticUnaryOperand(call.getOperands().get(0), operator);
  }

  static boolean nativeDecimalUnaryOperand(RexNode node, SqlOperator operator) {
    if (node instanceof RexInputRef || node instanceof RexLiteral) return true;
    if (!(node instanceof RexCall)) return false;
    RexCall call = (RexCall) node;
    if (nativeDecimalUnary(call, operator)
        || (operator == org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable.ABS
            && nativeDecimalUnary(
                call,
                org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable.UNARY_MINUS))
        || (operator
                == org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable.UNARY_MINUS
            && nativeDecimalUnary(
                call, org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable.ABS))) {
      return true;
    }
    if (call.getKind() != SqlKind.CAST || call.getOperands().size() != 1) return false;
    RelDataType input = call.getOperands().get(0).getType();
    return input.getSqlTypeName() == SqlTypeName.DECIMAL
        && call.getType().getSqlTypeName() == SqlTypeName.DECIMAL
        && call.getType().getPrecision() >= input.getPrecision()
        && call.getType().getScale() == input.getScale()
        && nativeDecimalUnaryOperand(call.getOperands().get(0), operator);
  }

  static boolean nativeDecimalFormattingArithmetic(RexNode node, boolean arithmeticUnary) {
    if (node.getType().getSqlTypeName() != SqlTypeName.DECIMAL) return false;
    if (node instanceof RexInputRef || node instanceof RexLiteral) return true;
    if (!(node instanceof RexCall)) return false;
    RexCall call = (RexCall) node;
    if (arithmeticUnary && (nativeDecimalAbs(call) || nativeDecimalNegate(call))) return true;
    if (isDecimalArithmetic(call)
        && (call.getKind() == SqlKind.PLUS
            || call.getKind() == SqlKind.MINUS
            || call.getKind() == SqlKind.TIMES)) {
      return call.getOperands().stream()
          .allMatch(operand -> nativeDecimalFormattingArithmetic(operand, arithmeticUnary));
    }
    return nativeDecimalUnaryOperand(
            node, org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable.ABS)
        || nativeDecimalUnaryOperand(
            node, org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable.UNARY_MINUS);
  }

  static boolean nativeIntegralAbs(RexCall call) {
    return call.getOperator()
            == org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable.ABS
        && call.getOperands().size() == 1
        && SqlTypeFamily.INTEGER.contains(call.getOperands().get(0).getType())
        && call.getType().getSqlTypeName() == call.getOperands().get(0).getType().getSqlTypeName()
        && nativeIntegralAbsOperand(call.getOperands().get(0));
  }

  static boolean nativeIntegralAbsOperand(RexNode node) {
    if (node instanceof RexInputRef || node instanceof RexLiteral) return true;
    if (!(node instanceof RexCall)) return false;
    RexCall call = (RexCall) node;
    if (nativeIntegralAbs(call)
        || nativeIntegralArithmetic(call, NumericExpressionSemantics::nativeIntegralAbsOperand))
      return true;
    return call.getKind() == SqlKind.CAST
        && call.getOperands().size() == 1
        && SqlTypeFamily.INTEGER.contains(call.getType())
        && wideningTargetCode(
                call.getOperands().get(0).getType().getSqlTypeName(),
                call.getType().getSqlTypeName())
            >= 0
        && nativeIntegralAbsOperand(call.getOperands().get(0));
  }

  static boolean nativeIntegralArithmetic(
      RexCall call, java.util.function.Predicate<RexNode> operand) {
    if (call.getOperands().size() != 2 || !SqlTypeFamily.INTEGER.contains(call.getType()))
      return false;
    switch (call.getKind()) {
      case PLUS:
      case MINUS:
      case TIMES:
        return call.getOperands().stream()
            .allMatch(node -> SqlTypeFamily.INTEGER.contains(node.getType()) && operand.test(node));
      default:
        return false;
    }
  }

  static boolean containsMetadataDependentDecimalRounding(RexNode node) {
    return node instanceof RexCall
        && (isDecimalIntegralRounding(((RexCall) node))
            || isDecimalRuntimeRounding(((RexCall) node))
            || ((RexCall) node)
                .getOperands().stream()
                    .anyMatch(
                        NumericExpressionSemantics::containsMetadataDependentDecimalRounding));
  }

  static boolean containsCompactDecimalSign(RexNode node) {
    if (!(node instanceof RexCall)) return false;
    RexCall call = (RexCall) node;
    var type = call.getType();
    // Compact SIGN can retain +/-1 with a declared DECIMAL(p,p). Reconstructing that
    // intermediate from BigDecimal would reject its precision before the cast sees it.
    return call.getOperator()
                == org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable.SIGN
            && type.getSqlTypeName() == SqlTypeName.DECIMAL
            && type.getPrecision() <= 18
            && type.getPrecision() == type.getScale()
        || call.getOperands().stream()
            .anyMatch(NumericExpressionSemantics::containsCompactDecimalSign);
  }

  static boolean isDecimalIntegralRounding(RexCall call) {
    return (call.getKind() == SqlKind.FLOOR || call.getKind() == SqlKind.CEIL)
        && call.getOperands().size() == 1
        && call.getOperands().get(0).getType().getSqlTypeName() == SqlTypeName.DECIMAL;
  }

  static boolean isDecimalRuntimeRounding(RexCall call) {
    return (call.getOperator()
                == org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable.ROUND
            || call.getOperator()
                == org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable.TRUNCATE)
        && call.getOperands().size() == 2
        && call.getOperands().get(0).getType().getSqlTypeName() == SqlTypeName.DECIMAL
        && !(call.getOperands().get(1) instanceof RexLiteral)
        && isInt32(call.getOperands().get(1));
  }

  static boolean containsRuntimeDecimalRounding(RexNode node) {
    return node instanceof RexCall
        && (isDecimalRuntimeRounding(((RexCall) node))
            || ((RexCall) node)
                .getOperands().stream()
                    .anyMatch(NumericExpressionSemantics::containsRuntimeDecimalRounding));
  }

  static boolean unnormalizedDecimalRounding(RexNode node) {
    if (node.getType().getSqlTypeName() != SqlTypeName.DECIMAL || !(node instanceof RexCall))
      return false;
    RexCall call = ((RexCall) node);

    if (isDecimalIntegralRounding(call) || isDecimalRuntimeRounding(call)) return true;
    if (call.getOperator() == org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable.IF)
      return false;
    if (call.getKind() == SqlKind.CAST
        && !org.apache.calcite.sql.type.SqlTypeUtil.equalSansNullability(
            call.getOperands().get(0).getType(), call.getType())) return false;
    {
      boolean switchResult2;
      switch (call.getKind()) {
        case PLUS:
        case MINUS:
        case TIMES:
        case DIVIDE:
        case MOD:
          switchResult2 = true;
          break;
        default:
          switchResult2 = false;
          break;
      }
      if (switchResult2) return false;
    }
    // Flink rounding can change DecimalData precision and scale. Selection and sign-only
    // consumers retain that metadata; Arrow normalization would change the host writer behavior.
    return call.getOperands().stream()
        .anyMatch(NumericExpressionSemantics::unnormalizedDecimalRounding);
  }

  static boolean isTimestampIdentityCast(RexCall call) {
    if (call.getKind() != SqlKind.CAST || call.getOperands().size() != 1) {
      return false;
    }
    RelDataType source = call.getOperands().get(0).getType();
    return (source.getSqlTypeName() == SqlTypeName.TIMESTAMP
            || source.getSqlTypeName() == SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE)
        && source.getSqlTypeName() == call.getType().getSqlTypeName()
        && call.getType().getPrecision() >= source.getPrecision();
  }

  static boolean containsDecimalUdf(RexNode node) {
    if (!(node instanceof RexCall)) return false;
    RexCall call = ((RexCall) node);

    if (call.getType().getSqlTypeName() == SqlTypeName.DECIMAL
        && tech.streamfusion.compat.FlinkCompat.scalarFunction(call.getOperator()) != null) {
      return true;
    }
    return call.getOperands().stream().anyMatch(NumericExpressionSemantics::containsDecimalUdf);
  }

  static boolean containsScalarUdf(RexNode node) {
    if (!(node instanceof RexCall)) return false;
    RexCall call = ((RexCall) node);

    if (tech.streamfusion.compat.FlinkCompat.scalarFunction(call.getOperator()) != null) {
      return true;
    }
    return call.getOperands().stream().anyMatch(NumericExpressionSemantics::containsScalarUdf);
  }

  static boolean isExactDoubleTruncate(RexCall call) {
    return call.getOperator()
            == org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable.TRUNCATE
        && call.getType().getSqlTypeName() == SqlTypeName.DOUBLE
        && (call.getOperands().size() == 1 || call.getOperands().size() == 2)
        && call.getOperands().get(0).getType().getSqlTypeName() == SqlTypeName.DOUBLE
        && (call.getOperands().size() == 1
            || call.getOperands().get(1).getType().getSqlTypeName() == SqlTypeName.INTEGER);
  }

  static org.apache.calcite.rex.RexShuttle exactDoubleTruncateCalls(Calc calc) {
    var builder = calc.getCluster().getRexBuilder();
    return new org.apache.calcite.rex.RexShuttle() {
      private org.apache.calcite.sql.SqlOperator helper;

      @Override
      public RexNode visitCall(RexCall original) {
        var call = (RexCall) super.visitCall(original);
        if (!isExactDoubleTruncate(call)) return call;
        if (helper == null) {
          var definition = new ExactDoubleTruncateFunction();
          helper =
              org.apache.flink.table.planner.functions.bridging.BridgingSqlFunction.of(
                  calc.getCluster(),
                  org.apache.flink.table.catalog.ContextResolvedFunction.anonymous(definition));
        }
        var operands = new java.util.ArrayList<>(call.getOperands());
        if (operands.size() == 1) {
          operands.add(
              builder.makeLiteral(
                  0, builder.getTypeFactory().createSqlType(SqlTypeName.INTEGER), true));
        }
        return builder.makeCall(call.getType(), helper, operands);
      }
    };
  }

  static boolean requiresDecimalCastFusion(RexNode node) {
    if (node instanceof RexFieldAccess) {
      return requiresDecimalCastFusion(((RexFieldAccess) node).getReferenceExpr());
    }
    if (!(node instanceof RexCall)) return false;
    RexCall call = (RexCall) node;
    if (call.getKind() == SqlKind.CAST
        && call.getOperands().size() == 1
        && SqlTypeFamily.CHARACTER.contains(call.getType())) {
      RexNode operand = call.getOperands().get(0);
      if (operand.getType().getSqlTypeName() == SqlTypeName.DECIMAL
          && !operand.getType().isNullable()
          && !nativeDecimalUnaryOperand(
              operand, org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable.ABS)
          && !nativeDecimalUnaryOperand(
              operand,
              org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable.UNARY_MINUS)) {
        return true;
      }
    }
    return call.getOperands().stream()
        .anyMatch(NumericExpressionSemantics::requiresDecimalCastFusion);
  }

  static boolean requiresDecimalProjectionDefaults(RexNode node) {
    return !node.getType().isNullable()
        && (node.getType().getSqlTypeName() == SqlTypeName.BOOLEAN
            || SqlTypeFamily.INTEGER.contains(node.getType())
            || SqlTypeFamily.APPROXIMATE_NUMERIC.contains(node.getType())
            || SqlTypeFamily.CHARACTER.contains(node.getType()))
        && (containsNonNullableDecimalArithmetic(node)
            || containsNonNullableDecimalNullProducer(node));
  }

  static boolean containsNonNullableDecimalArithmetic(RexNode node) {
    if (node instanceof RexFieldAccess) {
      return containsNonNullableDecimalArithmetic(((RexFieldAccess) node).getReferenceExpr());
    }
    if (!(node instanceof RexCall)) return false;
    RexCall call = (RexCall) node;
    if (call.getOperator()
            == org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable.TRY_CAST
        && call.getOperands().size() == 1
        && call.getType().isNullable()
        && SqlTypeFamily.CHARACTER.contains(call.getType())
        && nativeDecimalFormattingArithmetic(call.getOperands().get(0), true)) {
      // Verified character TRY_CAST resolves arithmetic NULLs before a consumer sees a value.
      return false;
    }
    return isDecimalArithmetic(call) && !call.getType().isNullable()
        || call.getOperands().stream()
            .anyMatch(NumericExpressionSemantics::containsNonNullableDecimalArithmetic);
  }

  static boolean containsNonNullableDecimalNullProducer(RexNode node) {
    if (node instanceof RexFieldAccess) {
      return containsNonNullableDecimalNullProducer(((RexFieldAccess) node).getReferenceExpr());
    }
    if (!(node instanceof RexCall)) return false;
    RexCall call = (RexCall) node;
    switch (call.getKind()) {
      case IS_NULL:
      case IS_NOT_NULL:
      case IS_TRUE:
      case IS_NOT_TRUE:
      case IS_FALSE:
      case IS_NOT_FALSE:
        // These consumers resolve the NULL flag rather than leaving a default result term.
        return false;
      default:
        return !call.getType().isNullable()
                && (decimalCastCanOverflow(call) || noncompactDecimalSignCanReturnNull(call))
            || call.getOperands().stream()
                .anyMatch(NumericExpressionSemantics::containsNonNullableDecimalNullProducer);
    }
  }

  static boolean noncompactDecimalSignCanReturnNull(RexCall call) {
    RelDataType type = call.getType();
    return call.getOperator()
            == org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable.SIGN
        && call.getOperands().size() == 1
        && type.getSqlTypeName() == SqlTypeName.DECIMAL
        && type.getPrecision() > 18
        && type.getPrecision() == type.getScale();
  }

  static boolean decimalCastCanOverflow(RexCall call) {
    if (call.getKind() != SqlKind.CAST
        || call.getType().getSqlTypeName() != SqlTypeName.DECIMAL
        || call.getOperands().size() != 1) return false;
    RelDataType input = call.getOperands().get(0).getType();
    if (SqlTypeFamily.APPROXIMATE_NUMERIC.contains(input)) return true;
    boolean decimal = input.getSqlTypeName() == SqlTypeName.DECIMAL;
    if (!decimal && !SqlTypeFamily.INTEGER.contains(input)) return false;
    int inputScale = decimal ? input.getScale() : 0;
    int inputDigits = input.getPrecision() - inputScale;
    int outputDigits = call.getType().getPrecision() - call.getType().getScale();
    // Reducing scale can round into another integer digit even when the integer widths match.
    return outputDigits < inputDigits
        || outputDigits == inputDigits && call.getType().getScale() < inputScale;
  }

  /**
   * The target type code for a widening numeric cast {@code source → target}, or -1 if not safe.
   */
  static int wideningTargetCode(SqlTypeName source, SqlTypeName target) {
    int from = numericRank(source);
    int to = numericRank(target);
    if (from < 0 || to < 0 || to < from) {
      return -1;
    }
    switch (target) {
      case TINYINT:
        return CastTarget.TINYINT.code();
      case SMALLINT:
        return CastTarget.SMALLINT.code();
      case INTEGER:
        return CastTarget.INTEGER.code();
      case BIGINT:
        return CastTarget.BIGINT.code();
      case FLOAT:
      case REAL:
        return CastTarget.FLOAT.code();
      case DOUBLE:
        return CastTarget.DOUBLE.code();
      default:
        return -1;
    }
  }

  /**
   * A widening order over the numeric types (lower widens losslessly to higher); -1 if not numeric.
   */
  static int numericRank(SqlTypeName type) {
    switch (type) {
      case TINYINT:
        return 0;
      case SMALLINT:
        return 1;
      case INTEGER:
        return 2;
      case BIGINT:
        return 3;
      case FLOAT:
      case REAL:
        return 4;
      case DOUBLE:
        return 5;
      default:
        return -1;
    }
  }

  /** Whether {@code call} needs Flink's resolved decimal result precision and scale. */
  static boolean isDecimalArithmetic(RexCall call) {
    switch (call.getKind()) {
      case PLUS:
      case MINUS:
      case TIMES:
      case DIVIDE:
      case MOD:
        return call.getType().getSqlTypeName() == SqlTypeName.DECIMAL;
      default:
        return false;
    }
  }

  static final class IntegralPositionRange {
    final int lower;
    final int upper;

    IntegralPositionRange(int lower, int upper) {
      this.lower = lower;
      this.upper = upper;
    }

    boolean isNullOnly() {
      // An empty interval proves NULL-only; a null proof means unknown or potentially failing.
      return lower > upper;
    }
  }
}
