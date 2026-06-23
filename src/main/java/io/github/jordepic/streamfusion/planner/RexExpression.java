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

  private RexExpression() {}

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
    RexProgram program = calc.getProgram();
    RexExpression encoder = new RexExpression();
    if (program.getCondition() != null) {
      RexNode condition =
          RexUtil.expandSearch(
              calc.getCluster().getRexBuilder(),
              null,
              program.expandLocalRef(program.getCondition()));
      encoder.conditionRoot = encoder.kinds.size();
      if (!encoder.emit(condition)) {
        return null;
      }
    }
    for (RexLocalRef ref : program.getProjectList()) {
      encoder.projectionRoots.add(encoder.kinds.size());
      if (!encoder.emit(program.expandLocalRef(ref))) {
        return null;
      }
    }
    encoder.outputNames = calc.getRowType().getFieldNames().toArray(new String[0]);
    return encoder;
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
    return false;
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
        return false;
    }
  }

  private boolean emitCall(RexCall call) {
    if (call.getKind() == SqlKind.CAST) {
      return emitCast(call);
    }
    if ("COALESCE".equalsIgnoreCase(call.getOperator().getName())) {
      return emitCoalesceAsCase(call.getOperands());
    }
    int op = opCode(call.getKind());
    if (op < 0) {
      return false;
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
      return false;
    }
    int target = wideningTargetCode(call.getOperands().get(0).getType().getSqlTypeName(),
        call.getType().getSqlTypeName());
    if (target < 0) {
      return false;
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

  /** The native op code for a call kind, or -1 if the operation is not admitted. */
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
