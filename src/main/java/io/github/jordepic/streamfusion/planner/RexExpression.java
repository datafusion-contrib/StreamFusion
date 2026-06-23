package io.github.jordepic.streamfusion.planner;

import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;

/**
 * Encodes a {@link RexNode} into the compact pre-order form the native engine decodes (see {@link
 * io.github.jordepic.streamfusion.Native#createFilterExpression}): parallel {@code kinds}/{@code
 * payload}/{@code childCounts} arrays plus typed literal pools. The encoding is the JVM counterpart
 * of the native expression builder, and admits only the operations the native side evaluates with
 * verified Flink parity; an unsupported node makes the whole encode fail (returning null), so the
 * containing operator falls back to Flink.
 */
final class RexExpression {

  // Node kinds, mirrored on the native side.
  private static final int KIND_INPUT_REF = 0;
  private static final int KIND_LIT_LONG = 1;
  private static final int KIND_LIT_DOUBLE = 2;
  private static final int KIND_LIT_STRING = 3;
  private static final int KIND_LIT_BOOL = 4;
  private static final int KIND_LIT_NULL = 5;
  private static final int KIND_CALL = 6;
  // Narrow integer literals keep their declared width (the value still rides in the long pool) so
  // native arithmetic evaluates in the same type as the host rather than a widened one.
  private static final int KIND_LIT_INT = 7;
  private static final int KIND_LIT_SMALL = 8;
  private static final int KIND_LIT_TINY = 9;
  // A cast node: payload is the target type code, with one child (the casted expression).
  private static final int KIND_CAST = 11;

  // Cast target type codes, mirrored on the native side.
  private static final int CAST_TINYINT = 0;
  private static final int CAST_SMALLINT = 1;
  private static final int CAST_INTEGER = 2;
  private static final int CAST_BIGINT = 3;
  private static final int CAST_FLOAT = 4;
  private static final int CAST_DOUBLE = 5;

  private final List<Integer> kinds = new ArrayList<>();
  private final List<Integer> payload = new ArrayList<>();
  private final List<Integer> childCounts = new ArrayList<>();
  private final List<Long> longs = new ArrayList<>();
  private final List<Double> doubles = new ArrayList<>();
  private final List<String> strings = new ArrayList<>();
  private final List<Integer> projectionRoots = new ArrayList<>();
  private int conditionRoot = -1;
  private String[] outputNames = new String[0];
  // Why the encode declined, set at the first (innermost) un-admitted node; null if it succeeded.
  private String reason;

  private RexExpression() {}

  /** Records the first decline reason and returns false, so callers can {@code return reject(...)}. */
  private boolean reject(String why) {
    if (reason == null) {
      reason = why;
    }
    return false;
  }

  /** The encoded expression, or null if {@code node} contains an unsupported operation. */
  static RexExpression encode(RexNode node) {
    RexExpression encoder = new RexExpression();
    return encoder.emit(node) ? encoder : null;
  }

  /**
   * Encodes a whole {@link Calc} — its optional condition followed by every projection expression —
   * into one shared set of pools, recording each tree's root node. Returns null if any node is an
   * operation the native engine does not admit, so the Calc falls back to the host.
   */
  static RexExpression encodeCalc(Calc calc) {
    RexExpression encoder = new RexExpression();
    return encoder.tryEncodeCalc(calc) ? encoder : null;
  }

  /**
   * Why {@code calc} cannot be encoded for the native engine (the first un-admitted op/operand), or
   * null if it can — for surfacing fallback reasons (ticket 29).
   */
  static String reasonForCalc(Calc calc) {
    RexExpression encoder = new RexExpression();
    return encoder.tryEncodeCalc(calc) ? null : encoder.reasonOrDefault();
  }

  private boolean tryEncodeCalc(Calc calc) {
    RexProgram program = calc.getProgram();
    if (program.getCondition() != null) {
      RexNode condition =
          RexUtil.expandSearch(
              calc.getCluster().getRexBuilder(),
              null,
              program.expandLocalRef(program.getCondition()));
      conditionRoot = kinds.size();
      if (!emit(condition)) {
        return false;
      }
    }
    for (RexLocalRef ref : program.getProjectList()) {
      projectionRoots.add(kinds.size());
      if (!emit(program.expandLocalRef(ref))) {
        return false;
      }
    }
    outputNames = calc.getRowType().getFieldNames().toArray(new String[0]);
    return true;
  }

  private String reasonOrDefault() {
    return reason != null ? reason : "unsupported Calc expression";
  }

  /** The pre-order node index of each projection tree's root. */
  int[] projectionRoots() {
    return toIntArray(projectionRoots);
  }

  /** The condition tree's root node index, or -1 if the Calc has no condition. */
  int conditionRoot() {
    return conditionRoot;
  }

  /** The Calc's output column names, in order. */
  String[] outputNames() {
    return outputNames;
  }

  int[] kinds() {
    return toIntArray(kinds);
  }

  int[] payload() {
    return toIntArray(payload);
  }

  int[] childCounts() {
    return toIntArray(childCounts);
  }

  long[] longs() {
    long[] out = new long[longs.size()];
    for (int i = 0; i < out.length; i++) {
      out[i] = longs.get(i);
    }
    return out;
  }

  double[] doubles() {
    double[] out = new double[doubles.size()];
    for (int i = 0; i < out.length; i++) {
      out[i] = doubles.get(i);
    }
    return out;
  }

  String[] strings() {
    return strings.toArray(new String[0]);
  }

  /** Appends {@code node} in pre-order; returns false (abandoning the encode) on an unsupported node. */
  private boolean emit(RexNode node) {
    if (node instanceof RexInputRef) {
      add(KIND_INPUT_REF, ((RexInputRef) node).getIndex(), 0);
      return true;
    }
    if (node instanceof RexLiteral) {
      return emitLiteral((RexLiteral) node);
    }
    if (node instanceof RexCall) {
      return emitCall((RexCall) node);
    }
    return reject("unsupported expression node: " + node);
  }

  private boolean emitLiteral(RexLiteral literal) {
    // An untyped NULL (e.g. a NULLIF/CASE `THEN NULL` branch); the surrounding expression's coercion
    // gives it a type, as it does on the host.
    if (literal.isNull()) {
      add(KIND_LIT_NULL, -1, 0);
      return true;
    }
    SqlTypeName type = literal.getType().getSqlTypeName();
    switch (type) {
      case TINYINT:
      case SMALLINT:
      case INTEGER:
      case BIGINT:
        {
          Long value = literal.getValueAs(Long.class);
          if (value == null) {
            return false;
          }
          add(integerLiteralKind(type), longs.size(), 0);
          longs.add(value);
          return true;
        }
      case FLOAT:
      case REAL:
      case DOUBLE:
      case DECIMAL:
        {
          Double value = literal.getValueAs(Double.class);
          if (value == null) {
            return false;
          }
          add(KIND_LIT_DOUBLE, doubles.size(), 0);
          doubles.add(value);
          return true;
        }
      case CHAR:
      case VARCHAR:
        {
          String value = literal.getValueAs(String.class);
          if (value == null) {
            return false;
          }
          add(KIND_LIT_STRING, strings.size(), 0);
          strings.add(value);
          return true;
        }
      case BOOLEAN:
        {
          Boolean value = literal.getValueAs(Boolean.class);
          if (value == null) {
            return false;
          }
          add(KIND_LIT_BOOL, longs.size(), 0);
          longs.add(value ? 1L : 0L);
          return true;
        }
      default:
        return reject("unsupported literal type: " + type);
    }
  }

  private boolean emitCall(RexCall call) {
    if (call.getKind() == SqlKind.CAST) {
      return emitCast(call);
    }
    if ("COALESCE".equalsIgnoreCase(call.getOperator().getName())) {
      return emitCoalesceAsCase(call.getOperands());
    }
    if ("TRIM".equalsIgnoreCase(call.getOperator().getName())) {
      return emitTrim(call);
    }
    if ("SUBSTRING".equalsIgnoreCase(call.getOperator().getName())) {
      return emitSubstring(call.getOperands());
    }
    int fnOp = functionOpCode(call.getOperator().getName());
    if (fnOp >= 0) {
      // The admitted scalar functions are all unary over a single string argument.
      if (call.getOperands().size() != 1) {
        return reject("unsupported arity for " + call.getOperator().getName());
      }
      add(KIND_CALL, fnOp, 1);
      return emit(call.getOperands().get(0));
    }
    int op = opCode(call.getKind());
    if (op < 0) {
      return reject("unsupported function/operator: " + call.getOperator().getName());
    }
    List<RexNode> operands = call.getOperands();
    switch (call.getKind()) {
      case NOT:
      case IS_NULL:
      case IS_NOT_NULL:
        if (operands.size() != 1) {
          return false;
        }
        add(KIND_CALL, op, 1);
        return emit(operands.get(0));
      case AND:
      case OR:
        // Calcite leaves AND/OR n-ary; the native binary op needs a left-deep nesting, which a
        // pre-order stream encodes as (n-1) call headers followed by the operands in order.
        if (operands.size() < 2) {
          return false;
        }
        for (int i = 0; i < operands.size() - 1; i++) {
          add(KIND_CALL, op, 2);
        }
        for (RexNode operand : operands) {
          if (!emit(operand)) {
            return false;
          }
        }
        return true;
      case CASE:
        // Searched CASE: operands are [when1, then1, …, else] — kept n-ary, the native side pairs
        // them back into when/then branches with a trailing else.
        if (operands.isEmpty()) {
          return false;
        }
        add(KIND_CALL, op, operands.size());
        for (RexNode operand : operands) {
          if (!emit(operand)) {
            return false;
          }
        }
        return true;
      default:
        // The remaining admitted ops (arithmetic and comparisons) are strictly binary.
        if (operands.size() != 2) {
          return false;
        }
        add(KIND_CALL, op, 2);
        return emit(operands.get(0)) && emit(operands.get(1));
    }
  }

  /**
   * Lowers {@code COALESCE(a, b, …, z)} to the searched CASE the host defines it as —
   * {@code CASE WHEN a IS NOT NULL THEN a WHEN b IS NOT NULL THEN b … ELSE z} — so it rides the
   * admitted CASE path with identical (first-non-null) semantics. Calcite does not pre-expand
   * COALESCE here, so we expand it ourselves rather than admit a separate op.
   */
  private boolean emitCoalesceAsCase(List<RexNode> operands) {
    int n = operands.size();
    if (n < 2) {
      return false;
    }
    // CASE operands are [when1, then1, …, else]; each leading arg becomes an IS NOT NULL guard and
    // the same arg as its result, with the final arg the else.
    add(KIND_CALL, opCode(SqlKind.CASE), 2 * (n - 1) + 1);
    for (int i = 0; i < n - 1; i++) {
      add(KIND_CALL, opCode(SqlKind.IS_NOT_NULL), 1);
      if (!emit(operands.get(i))) {
        return false;
      }
      if (!emit(operands.get(i))) {
        return false;
      }
    }
    return emit(operands.get(n - 1));
  }

  /**
   * Emits a cast, but only a widening numeric one (integer to a wider integer, integer to
   * float/double, float to double, or an identity cast). Those are lossless and evaluate identically
   * on both sides; narrowing, float-to-integer, and string casts differ in overflow/rounding/parsing
   * semantics, so they are not admitted and the expression falls back.
   */
  private boolean emitCast(RexCall call) {
    if (call.getOperands().size() != 1) {
      return reject("unsupported CAST arity");
    }
    SqlTypeName source = call.getOperands().get(0).getType().getSqlTypeName();
    SqlTypeName targetType = call.getType().getSqlTypeName();
    int target = wideningTargetCode(source, targetType);
    if (target < 0) {
      return reject("unsupported CAST " + source + "→" + targetType + " (only widening numeric)");
    }
    add(KIND_CAST, target, 1);
    return emit(call.getOperands().get(0));
  }

  /** The target type code for a widening numeric cast {@code source → target}, or -1 if not safe. */
  private static int wideningTargetCode(SqlTypeName source, SqlTypeName target) {
    int from = numericRank(source);
    int to = numericRank(target);
    if (from < 0 || to < 0 || to < from) {
      return -1;
    }
    switch (target) {
      case TINYINT:
        return CAST_TINYINT;
      case SMALLINT:
        return CAST_SMALLINT;
      case INTEGER:
        return CAST_INTEGER;
      case BIGINT:
        return CAST_BIGINT;
      case FLOAT:
      case REAL:
        return CAST_FLOAT;
      case DOUBLE:
        return CAST_DOUBLE;
      default:
        return -1;
    }
  }

  /** A widening order over the numeric types (lower widens losslessly to higher); -1 if not numeric. */
  private static int numericRank(SqlTypeName type) {
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

  /** The literal kind for an exact-integer SQL type, preserving its declared width. */
  private static int integerLiteralKind(SqlTypeName type) {
    switch (type) {
      case TINYINT:
        return KIND_LIT_TINY;
      case SMALLINT:
        return KIND_LIT_SMALL;
      case INTEGER:
        return KIND_LIT_INT;
      default:
        return KIND_LIT_LONG;
    }
  }

  /**
   * Emits {@code SUBSTRING(s FROM pos [FOR len])} (op 55, 2 or 3 operands → native substr/substring).
   * Admitted only when {@code pos} is an integer literal ≥ 1 and {@code len} (if present) ≥ 0: Flink
   * and DataFusion diverge when the start is below 1 (Flink clamps it to 1, DataFusion counts the
   * out-of-range prefix against the length), and a runtime position can't be checked — so a non-
   * literal or out-of-range bound falls back rather than risk a wrong answer.
   */
  private boolean emitSubstring(List<RexNode> args) {
    if (args.size() != 2 && args.size() != 3) {
      return reject("unsupported SUBSTRING arity");
    }
    if (!isIntLiteralAtLeast(args.get(1), 1)) {
      return reject("SUBSTRING requires a literal start position ≥ 1");
    }
    if (args.size() == 3 && !isIntLiteralAtLeast(args.get(2), 0)) {
      return reject("SUBSTRING requires a literal length ≥ 0");
    }
    add(KIND_CALL, 55, args.size());
    for (RexNode arg : args) {
      if (!emit(arg)) {
        return false;
      }
    }
    return true;
  }

  /** Whether {@code node} is an integer literal whose value is at least {@code min}. */
  private static boolean isIntLiteralAtLeast(RexNode node, long min) {
    if (!(node instanceof RexLiteral)) {
      return false;
    }
    Long value = ((RexLiteral) node).getValueAs(Long.class);
    return value != null && value >= min;
  }

  /**
   * Emits {@code TRIM(BOTH ' ' FROM s)} — the default whitespace both-sides trim — as a unary call
   * (op 54) mapped to DataFusion's {@code btrim}. Calcite gives TRIM three operands: a BOTH/LEADING/
   * TRAILING flag, the trim characters, and the source string. Only the default (flag {@code BOTH},
   * a single-space trim set) is admitted; LEADING/TRAILING or custom trim chars fall back.
   */
  private boolean emitTrim(RexCall call) {
    List<RexNode> operands = call.getOperands();
    if (operands.size() != 3
        || !(operands.get(0) instanceof RexLiteral)
        || !(operands.get(1) instanceof RexLiteral)) {
      return reject("unsupported TRIM form");
    }
    String flag = String.valueOf(((RexLiteral) operands.get(0)).getValue());
    String trimChars = ((RexLiteral) operands.get(1)).getValueAs(String.class);
    if (!"BOTH".equals(flag) || !" ".equals(trimChars)) {
      return reject("TRIM supports only the default BOTH whitespace trim");
    }
    add(KIND_CALL, 54, 1);
    return emit(operands.get(2));
  }

  /**
   * The op code for an admitted scalar function (matched by name, since Flink delivers them as
   * {@code OTHER_FUNCTION} calls), or -1. ASCII-equivalent to the host; the Unicode edges (case
   * folding, code-point vs UTF-16 length) are recorded in divergences/07.
   */
  private static int functionOpCode(String name) {
    switch (name.toUpperCase(java.util.Locale.ROOT)) {
      case "UPPER":
        return 50;
      case "LOWER":
        return 51;
      case "CHAR_LENGTH":
      case "CHARACTER_LENGTH":
        return 52;
      default:
        return -1;
    }
  }

  private static int opCode(SqlKind kind) {
    switch (kind) {
      case PLUS:
        return 0;
      case MINUS:
        return 1;
      case TIMES:
        return 2;
      case DIVIDE:
        return 3;
      case MOD:
        return 4;
      case GREATER_THAN:
        return 10;
      case GREATER_THAN_OR_EQUAL:
        return 11;
      case LESS_THAN:
        return 12;
      case LESS_THAN_OR_EQUAL:
        return 13;
      case EQUALS:
        return 14;
      case NOT_EQUALS:
        return 15;
      case AND:
        return 20;
      case OR:
        return 21;
      case NOT:
        return 22;
      case IS_NULL:
        return 30;
      case IS_NOT_NULL:
        return 31;
      case CASE:
        return 40;
      default:
        return -1;
    }
  }

  private void add(int kind, int payloadValue, int childCount) {
    kinds.add(kind);
    payload.add(payloadValue);
    childCounts.add(childCount);
  }

  private static int[] toIntArray(List<Integer> values) {
    int[] out = new int[values.size()];
    for (int i = 0; i < out.length; i++) {
      out[i] = values.get(i);
    }
    return out;
  }
}
