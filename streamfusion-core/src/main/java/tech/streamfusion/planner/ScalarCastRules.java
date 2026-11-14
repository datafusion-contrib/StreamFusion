package tech.streamfusion.planner;

import java.util.List;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable;
import tech.streamfusion.compat.FlinkCompat;
import tech.streamfusion.planner.common.Admission;
import tech.streamfusion.planner.common.CastTarget;
import tech.streamfusion.planner.common.ExpressionKind;

/** Cast families own their lowering, failure effects and value representation together. */
final class ScalarCastRules {
  enum Representation {
    SCALAR,
    ASCII_STRING,
    JAVA_STRING,
    BINARY,
    FIXED_BINARY,
    VARIABLE_BINARY
  }

  interface Context {
    Boolean legacyBehaviour();
    boolean requiresRowEvaluation(RexNode operand);
    boolean supportsBooleanText(RexNode operand);
    boolean operand(RexNode operand);
    boolean host(RexCall call);
    void node(ExpressionKind kind, int payload, int children);
    void integerLiteral(int value);
    void booleanLiteral(boolean value);
  }

  @FunctionalInterface
  private interface Lowering {
    boolean emit(Context context);
  }

  static final class Plan {
    private final Representation representation;
    private final boolean canFail;
    private final Lowering lowering;

    private Plan(Representation representation, boolean canFail, Lowering lowering) {
      this.representation = representation;
      this.canFail = canFail;
      this.lowering = lowering;
    }

    Representation representation() { return representation; }
    boolean canFail() { return canFail; }
    boolean emit(Context context) { return lowering.emit(context); }
  }

  private static final class Input {
    final RexCall call;
    final RexNode operand;
    final SqlTypeName source;
    final SqlTypeName target;
    final boolean tryCast;

    Input(RexCall call) {
      this.call = call;
      operand = call.getOperands().get(0);
      source = operand.getType().getSqlTypeName();
      target = call.getType().getSqlTypeName();
      tryCast = call.getOperator() == FlinkSqlOperatorTable.TRY_CAST;
    }
  }

  private abstract static class Rule {
    abstract boolean matches(Input input);
    abstract Admission<Plan> prepare(Input input, Context context);
    boolean canFail(Input input, Boolean legacy) { return false; }
    Representation representation(Input input, Boolean legacy) { return Representation.SCALAR; }

    Admission<Plan> accepted(Input input, Context context, Lowering lowering) {
      return Admission.accepted(new Plan(representation(input, context.legacyBehaviour()),
          canFail(input, context.legacyBehaviour()), lowering));
    }
  }

  private static final class IntegerParse extends Rule {
    @Override
    boolean matches(Input input) {
      return SqlTypeFamily.CHARACTER.contains(input.operand.getType()) && integerTarget(input.target) != null;
    }

    @Override
    boolean canFail(Input input, Boolean legacy) {
      return input.call.getKind() == SqlKind.CAST && !Boolean.TRUE.equals(legacy);
    }

    @Override
    Admission<Plan> prepare(Input input, Context context) {
      Boolean legacy = context.legacyBehaviour();
      if (!input.tryCast && legacy == null)
        return Admission.declined("STRING to integer requires the configured cast behavior");
      int flags = integerTarget(input.target).code() | (input.tryCast || Boolean.TRUE.equals(legacy) ? 4 : 0);
      return accepted(input, context, out -> {
        out.node(ExpressionKind.STRING_TO_INTEGER, flags, 1);
        return out.operand(input.operand);
      });
    }
  }

  private static final class IntegerFormat extends Rule {
    @Override
    boolean matches(Input input) {
      return integerTarget(input.source) != null && SqlTypeFamily.CHARACTER.contains(input.call.getType());
    }

    @Override
    Representation representation(Input input, Boolean legacy) { return Representation.ASCII_STRING; }

    @Override
    Admission<Plan> prepare(Input input, Context context) {
      Boolean legacy = context.legacyBehaviour();
      if (legacy == null)
        return Admission.declined("Integer to STRING requires the configured cast behavior");
      int length = legacy ? Integer.MAX_VALUE : input.call.getType().getPrecision();
      int width = !legacy && input.target == SqlTypeName.CHAR ? -length : length;
      return accepted(input, context, out -> {
        out.node(ExpressionKind.INTEGER_TO_STRING, width, 1);
        return out.operand(input.operand);
      });
    }
  }

  private static final class BooleanParse extends Rule {
    @Override
    boolean matches(Input input) {
      return SqlTypeFamily.CHARACTER.contains(input.operand.getType()) && input.target == SqlTypeName.BOOLEAN;
    }

    @Override
    boolean canFail(Input input, Boolean legacy) {
      return input.call.getKind() == SqlKind.CAST && !Boolean.TRUE.equals(legacy);
    }

    @Override
    Admission<Plan> prepare(Input input, Context context) {
      Boolean legacy = context.legacyBehaviour();
      if (input.tryCast && context.requiresRowEvaluation(input.operand))
        return accepted(input, context, out -> out.host(input.call));
      if (!input.tryCast && legacy == null)
        return Admission.declined("STRING to BOOLEAN requires the configured cast behavior");
      int operation = input.tryCast || Boolean.TRUE.equals(legacy) ? 160 : 159;
      return accepted(input, context, out -> {
        out.node(ExpressionKind.CALL, operation, 1);
        return out.operand(input.operand);
      });
    }
  }

  private static final class BooleanFormat extends Rule {
    @Override
    boolean matches(Input input) {
      return input.source == SqlTypeName.BOOLEAN && SqlTypeFamily.CHARACTER.contains(input.call.getType());
    }

    @Override
    Representation representation(Input input, Boolean legacy) { return Representation.ASCII_STRING; }

    @Override
    Admission<Plan> prepare(Input input, Context context) {
      Boolean legacy = context.legacyBehaviour();
      if (legacy == null || !context.supportsBooleanText(input.operand)) return Admission.noMatch();
      return accepted(input, context, out -> emitBooleanText(input.call, legacy, out));
    }
  }

  private static final class BinaryTarget extends Rule {
    @Override
    boolean matches(Input input) {
      return SqlTypeFamily.BINARY.contains(input.call.getType())
          && (SqlTypeFamily.CHARACTER.contains(input.operand.getType())
              || SqlTypeFamily.BINARY.contains(input.operand.getType()));
    }

    @Override
    Representation representation(Input input, Boolean legacy) {
      if (input.target != SqlTypeName.BINARY) return Representation.BINARY;
      return Boolean.TRUE.equals(legacy) ? Representation.VARIABLE_BINARY : Representation.FIXED_BINARY;
    }

    @Override
    Admission<Plan> prepare(Input input, Context context) {
      Boolean legacy = context.legacyBehaviour();
      if (legacy == null || context.requiresRowEvaluation(input.operand))
        return accepted(input, context, out -> out.host(input.call));
      int length = legacy ? 0 : input.call.getType().getPrecision();
      boolean padVariable = !legacy && input.target == SqlTypeName.VARBINARY
          && SqlTypeFamily.BINARY.contains(input.operand.getType())
          && FlinkCompat.binaryCastPadsVariable(input.operand.getType().getPrecision(), length);
      return accepted(input, context, out -> {
        if (padVariable) out.node(ExpressionKind.BINARY_CAST, 0, 1);
        out.node(ExpressionKind.BINARY_CAST,
            input.target == SqlTypeName.BINARY || padVariable ? -length : length, 1);
        return out.operand(input.operand);
      });
    }
  }

  private static final class BinaryToCharacter extends Rule {
    @Override
    boolean matches(Input input) {
      return SqlTypeFamily.BINARY.contains(input.operand.getType())
          && SqlTypeFamily.CHARACTER.contains(input.call.getType());
    }

    @Override
    Representation representation(Input input, Boolean legacy) { return Representation.JAVA_STRING; }

    @Override
    Admission<Plan> prepare(Input input, Context context) {
      Boolean legacy = context.legacyBehaviour();
      boolean unbounded = input.target == SqlTypeName.VARCHAR
          && input.call.getType().getPrecision() == Integer.MAX_VALUE;
      if (!unbounded && legacy == null) return Admission.noMatch();
      boolean bounded = !unbounded && Boolean.FALSE.equals(legacy);
      return accepted(input, context, out -> {
        out.node(ExpressionKind.CALL, 237, bounded ? 2 : 1);
        if (!out.operand(input.operand)) return false;
        if (bounded) out.integerLiteral(input.target == SqlTypeName.CHAR
            ? -input.call.getType().getPrecision() : input.call.getType().getPrecision());
        return true;
      });
    }
  }

  private static final IntegerFormat INTEGER_FORMAT = new IntegerFormat();
  private static final BinaryToCharacter BINARY_TO_CHARACTER = new BinaryToCharacter();
  private static final List<Rule> RULES = List.of(
      new IntegerParse(), INTEGER_FORMAT, new BooleanParse(), new BooleanFormat(),
      new BinaryTarget(), BINARY_TO_CHARACTER);

  private ScalarCastRules() {}

  private static Input input(RexCall call) {
    return call.getOperands().size() == 1
        && (call.getKind() == SqlKind.CAST || call.getOperator() == FlinkSqlOperatorTable.TRY_CAST)
        ? new Input(call) : null;
  }

  static Admission<Plan> prepare(RexCall call, Context context) {
    Input input = input(call);
    if (input != null)
      for (Rule rule : RULES) if (rule.matches(input)) return rule.prepare(input, context);
    return Admission.noMatch();
  }

  static boolean mayFailOnRow(RexCall call, Boolean legacy) {
    Input input = input(call);
    if (input != null)
      for (Rule rule : RULES) if (rule.matches(input)) return rule.canFail(input, legacy);
    return false;
  }

  static RexNode integerTextOperand(RexCall call) {
    Input input = input(call);
    return input != null && INTEGER_FORMAT.matches(input)
        && input.target == SqlTypeName.VARCHAR && input.call.getType().getPrecision() == Integer.MAX_VALUE
        && (call.getOperator() == FlinkSqlOperatorTable.CAST || call.getOperator() == FlinkSqlOperatorTable.TRY_CAST)
        ? input.operand : null;
  }

  static boolean modernBoundedBinaryCharacterCast(RexCall call, Boolean legacy) {
    Input input = input(call);
    return input != null && BINARY_TO_CHARACTER.matches(input) && Boolean.FALSE.equals(legacy)
        && (input.target == SqlTypeName.CHAR || input.call.getType().getPrecision() != Integer.MAX_VALUE);
  }

  static boolean emitBooleanText(RexCall call, boolean lowercase, Context out) {
    out.node(ExpressionKind.CALL, 244, 3);
    if (!out.operand(call.getOperands().get(0))) return false;
    out.booleanLiteral(lowercase);
    out.integerLiteral(call.getType().getSqlTypeName() == SqlTypeName.CHAR
        ? -call.getType().getPrecision() : call.getType().getPrecision());
    return true;
  }

  private static CastTarget integerTarget(SqlTypeName type) {
    switch (type) {
      case TINYINT: return CastTarget.TINYINT;
      case SMALLINT: return CastTarget.SMALLINT;
      case INTEGER: return CastTarget.INTEGER;
      case BIGINT: return CastTarget.BIGINT;
      default: return null;
    }
  }
}
