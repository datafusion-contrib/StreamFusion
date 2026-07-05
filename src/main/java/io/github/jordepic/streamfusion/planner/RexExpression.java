package io.github.jordepic.streamfusion.planner;

import io.github.jordepic.streamfusion.operator.EncodedPredicate;
import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeFamily;
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
  // PROCTIME(): a nullary call materializing the current processing time as a TIMESTAMP_LTZ(3)
  // column. Admitting it keeps the planner's `PROCTIME() AS …` projection columnar, which is what
  // unblocks proctime-ordered operators (dedup, OVER) — those use the column only as an arrival-order
  // key and project it away, so its (non-deterministic) value is never observed in the output.
  private static final int KIND_PROCTIME = 12;
  // Field access: extract a named field from a ROW/struct-typed child. payload is the string-pool
  // index of the field name, with one child (the struct-typed expression). Nested access (a.b.c)
  // nests these, the child being itself a field access. Mirrors DataFusion's get_field.
  private static final int KIND_FIELD_ACCESS = 13;
  // Approximate decimal cast: payload packs the target DECIMAL precision/scale (precision*100 + scale),
  // one child. Wraps a (double-computed) arithmetic result, casting it to the declared DECIMAL so the
  // output column type matches — only under the approximate-decimal flag (not byte-exact to Flink).
  private static final int KIND_CAST_DECIMAL = 14;
  // A day-time INTERVAL literal: payload is the long-pool index of its value in milliseconds. The
  // native side builds an Arrow IntervalDayTime, so `timestamp - interval` evaluates to a timestamp.
  private static final int KIND_LIT_INTERVAL = 15;
  // An exact DECIMAL literal: payload indexes the string pool, whose entry is "unscaled|precision|scale"
  // (the unscaled integer as a string, as it can exceed i64). The native side builds a Decimal128 scalar,
  // so decimal arithmetic stays exact instead of routing through double.
  private static final int KIND_LIT_DECIMAL = 16;
  // A JVM UDF call: payload indexes the long pool at [udf id, return-type code]; children are the
  // argument expressions. The native JvmUdf node upcalls NativeUdf.invokeUdf to run the actual Flink
  // ScalarFunction.eval over each batch (columnar), so a non-builtin UDF stays inside the native island.
  private static final int KIND_UDF = 17;
  // A narrowing cast to an integer type: payload is the target integer type code (CAST_TINYINT..
  // CAST_BIGINT), one child. Built natively as a wrapping/saturating kernel matching Flink's primitive
  // Java cast, since arrow's own cast errors on overflow rather than wrapping.
  private static final int KIND_CAST_NARROW = 18;

  // ARRAY[i] / MAP[key] subscript (Calcite's ITEM), two children: the collection, then the literal
  // index/key. The native side dispatches on the collection type to DataFusion's array_element /
  // map get_field, whose null-on-miss semantics match Flink's subscript. Only literal subscripts
  // are admitted — see emitItem.
  private static final int KIND_ITEM = 19;

  // Decimal `/` and `%`: payload packs the declared result's precision*100 + scale; two children (the
  // operands). A fused native kernel reproduces Flink's 38-significant-digit quotient + rescale.
  private static final int KIND_DECIMAL_DIVIDE = 20;
  private static final int KIND_DECIMAL_MOD = 21;

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
  // Unary functions whose native (Rust) result can differ from the host's JVM result — locale case
  // folding and non-correctly-rounded transcendental math — keyed to their native op code. Admitted
  // only under the allowIncompatible flag (see NativeConfig); otherwise they fall back.
  private static final java.util.Map<String, Integer> INCOMPATIBLE_UNARY =
      java.util.Map.ofEntries(
          java.util.Map.entry("UPPER", 50),
          java.util.Map.entry("LOWER", 51),
          java.util.Map.entry("EXP", 72),
          java.util.Map.entry("LN", 73),
          java.util.Map.entry("SIN", 74),
          java.util.Map.entry("COS", 75),
          java.util.Map.entry("TAN", 76),
          java.util.Map.entry("ASIN", 77),
          java.util.Map.entry("ACOS", 78),
          java.util.Map.entry("ATAN", 79),
          java.util.Map.entry("LOG10", 80));

  // UDFs referenced by KIND_UDF nodes, and the longs-pool slots holding their local indices. Rather
  // than registering into NativeUdf's (planner-JVM) registry at encode time and baking a global id —
  // which a task manager's empty registry wouldn't have — we carry a serializable descriptor per UDF
  // and bake its local index; the operator registers them at open() and patches the slots.
  private final List<io.github.jordepic.streamfusion.operator.NativeUdf.Descriptor> udfs =
      new ArrayList<>();
  private final List<Integer> udfIdSlots = new ArrayList<>();

  private final List<Integer> projectionRoots = new ArrayList<>();
  private int conditionRoot = -1;
  private String[] outputNames = new String[0];
  // Why the encode declined, set at the first (innermost) un-admitted node; null if it succeeded.
  private String reason;
  // The session time zone (table.local-time-zone) — needed to format/extract a TIMESTAMP_LTZ, whose
  // calendar fields depend on it. Set only on the Calc path (where DATE_FORMAT/EXTRACT live); null
  // elsewhere, so an LTZ date/extract in a bare-RexNode context (e.g. a join residual) safely falls back.
  private String sessionZoneId;
  // Whether table.exec.legacy-cast-behaviour is enabled — known only when encoding a Calc (the config
  // rides the node). null (a bare predicate encode) declines the host-exact casts, conservatively.
  private Boolean legacyCastBehaviour;

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
   * Packages an encoded single-expression predicate (its root at node 0) for a native join operator,
   * or {@link EncodedPredicate#NONE} when there is no predicate.
   */
  static EncodedPredicate toEncodedPredicate(RexExpression predicate) {
    if (predicate == null) {
      return EncodedPredicate.NONE;
    }
    return new EncodedPredicate(
        predicate.kinds(),
        predicate.payload(),
        predicate.childCounts(),
        predicate.longs(),
        predicate.doubles(),
        predicate.strings(),
        predicate.udfBinding());
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
    org.apache.flink.table.api.TableConfig tableConfig =
        org.apache.flink.table.planner.utils.ShortcutUtils.unwrapTableConfig(calc);
    sessionZoneId = tableConfig.getLocalTimeZone().getId();
    legacyCastBehaviour =
        tableConfig
            .get(
                org.apache.flink.table.api.config.ExecutionConfigOptions
                    .TABLE_EXEC_LEGACY_CAST_BEHAVIOUR)
            .isEnabled();
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
      // Expand a Sarg/SEARCH the same as in the condition — a `FILTER (WHERE x >= a AND x < b)`
      // lowers its range into a SEARCH inside a projected expression, not just the condition.
      RexNode projection =
          RexUtil.expandSearch(
              calc.getCluster().getRexBuilder(), null, program.expandLocalRef(ref));
      if (!emit(projection)) {
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

  /**
   * Remaps every top-level input-column reference through {@code map} (old column index → new), in
   * place. Used when the entry transpose prunes the input: the Arrow batch the native operator sees
   * holds only the read columns, compacted, so each {@code INPUT_REF} points at its new position.
   * Nested field access is encoded by name and the pruned fields keep their names, so it is unaffected.
   */
  RexExpression remapInputs(int[] map) {
    for (int i = 0; i < kinds.size(); i++) {
      if (kinds.get(i) == KIND_INPUT_REF) {
        payload.set(i, map[payload.get(i)]);
      }
    }
    return this;
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

  /**
   * Records a UDF to register at operator open() and returns its local index (baked into the {@code
   * longs} pool in place of a plan-time global id); also notes the next {@code longs} slot as the one
   * holding that index, so the operator's {@link
   * io.github.jordepic.streamfusion.operator.NativeUdf.Binding} can patch it to the runtime id.
   */
  private int addUdf(io.github.jordepic.streamfusion.operator.NativeUdf.Descriptor descriptor) {
    int localIndex = udfs.size();
    udfs.add(descriptor);
    udfIdSlots.add(longs.size());
    return localIndex;
  }

  /** The UDF registrations this encoded expression needs, to carry into the operator. */
  io.github.jordepic.streamfusion.operator.NativeUdf.Binding udfBinding() {
    return new io.github.jordepic.streamfusion.operator.NativeUdf.Binding(
        udfs.toArray(new io.github.jordepic.streamfusion.operator.NativeUdf.Descriptor[0]),
        toIntArray(udfIdSlots));
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
    if (node instanceof RexFieldAccess) {
      RexFieldAccess access = (RexFieldAccess) node;
      add(KIND_FIELD_ACCESS, strings.size(), 1);
      strings.add(access.getField().getName());
      return emit(access.getReferenceExpr());
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
    // A day-time INTERVAL literal (SECOND/MINUTE/HOUR/DAY) — Calcite stores its value in milliseconds.
    // Admitted so datetime arithmetic like `ts - INTERVAL '10' SECOND` (Nexmark q7) is expressible; a
    // year-month interval (value in months) falls back.
    if (type.getFamily() == SqlTypeFamily.INTERVAL_DAY_TIME) {
      Long millis = literal.getValueAs(Long.class);
      if (millis == null) {
        return reject("null interval literal");
      }
      add(KIND_LIT_INTERVAL, longs.size(), 0);
      longs.add(millis);
      return true;
    }
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
      case DECIMAL:
        {
          java.math.BigDecimal value = literal.getValueAs(java.math.BigDecimal.class);
          if (value == null) {
            return false;
          }
          // Emit as an exact Decimal128: one string-pool entry "unscaled|precision|scale", where
          // unscaled is the integer value at the declared scale (a string, as it can exceed i64).
          // Decimal arithmetic then stays in Decimal128 rather than routing through double.
          int precision = literal.getType().getPrecision();
          int scale = literal.getType().getScale();
          java.math.BigInteger unscaled =
              value.setScale(scale, java.math.RoundingMode.UNNECESSARY).unscaledValue();
          add(KIND_LIT_DECIMAL, strings.size(), 0);
          strings.add(unscaled + "|" + precision + "|" + scale);
          return true;
        }
      case FLOAT:
      case REAL:
      case DOUBLE:
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
    // Reinterpret only re-tags a value's type (e.g. stripping a time-attribute/ROWTIME marker), never
    // changing the value — an identity projection of its first operand.
    if (call.getKind() == SqlKind.REINTERPRET) {
      return emit(call.getOperands().get(0));
    }
    if (call.getKind() == SqlKind.EXTRACT) {
      return emitExtract(call);
    }
    // Decimal-typed arithmetic, all exact. Add/subtract/multiply: the operands reach the native side
    // as Decimal128 (columns already are; literals emit as exact Decimal128), and Arrow's Decimal128
    // add/sub/multiply match Flink's — the products carry the full scale (sum of input scales for ×,
    // aligned max scale for ±), and the wrapping cast to the declared DECIMAL(p, s) rounds HALF_UP,
    // the same rounding Flink uses. Division/modulo need Flink's own two rounding steps (the
    // 38-significant-digit quotient, then the rescale to the declared type), which Arrow's division
    // cannot reproduce, so they run through a dedicated fused kernel.
    if (isDecimalArithmetic(call)) {
      int precision = call.getType().getPrecision();
      int scale = call.getType().getScale();
      if (call.getKind() == SqlKind.DIVIDE || call.getKind() == SqlKind.MOD) {
        add(
            call.getKind() == SqlKind.DIVIDE ? KIND_DECIMAL_DIVIDE : KIND_DECIMAL_MOD,
            precision * 100 + scale,
            2);
        for (RexNode operand : call.getOperands()) {
          if (!emit(operand)) {
            return false;
          }
        }
        return true;
      }
      add(KIND_CAST_DECIMAL, precision * 100 + scale, 1);
      // fall through: the arithmetic op is emitted next as this cast's single child.
    }
    // PROCTIME() / PROCTIME_MATERIALIZE(): a nullary current-processing-time column.
    if (call.getOperator().getName().toUpperCase(java.util.Locale.ROOT).contains("PROCTIME")) {
      add(KIND_PROCTIME, 0, 0);
      return true;
    }
    if ("ITEM".equals(call.getOperator().getName())) {
      return emitItem(call);
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
    if ("REPLACE".equalsIgnoreCase(call.getOperator().getName())) {
      List<RexNode> args = call.getOperands();
      if (args.size() != 3) {
        return reject("REPLACE requires 3 arguments");
      }
      add(KIND_CALL, 58, 3);
      for (RexNode arg : args) {
        if (!emit(arg)) {
          return false;
        }
      }
      return true;
    }
    if ("POSITION".equalsIgnoreCase(call.getOperator().getName())) {
      // POSITION(sub IN s) — operands [sub, s]; the native side calls strpos(s, sub).
      List<RexNode> args = call.getOperands();
      if (args.size() != 2) {
        return reject("POSITION requires 2 arguments (no FROM start)");
      }
      add(KIND_CALL, 57, 2);
      return emit(args.get(0)) && emit(args.get(1));
    }
    if ("SPLIT_INDEX".equalsIgnoreCase(call.getOperator().getName())) {
      return emitSplitIndex(call.getOperands());
    }
    if ("REGEXP_EXTRACT".equalsIgnoreCase(call.getOperator().getName())) {
      return emitRegexpExtract(call.getOperands());
    }
    if ("DATE_FORMAT".equalsIgnoreCase(call.getOperator().getName())) {
      return emitDateFormat(call.getOperands());
    }
    if ("TO_TIMESTAMP_LTZ".equalsIgnoreCase(call.getOperator().getName())) {
      return emitToTimestampLtz(call.getOperands());
    }
    if ("ABS".equalsIgnoreCase(call.getOperator().getName())) {
      return emitFloatUnary(call, 62);
    }
    if ("FLOOR".equalsIgnoreCase(call.getOperator().getName())) {
      return emitFloatUnary(call, 63);
    }
    if ("CEIL".equalsIgnoreCase(call.getOperator().getName())
        || "CEILING".equalsIgnoreCase(call.getOperator().getName())) {
      return emitFloatUnary(call, 64);
    }
    if ("SIGN".equalsIgnoreCase(call.getOperator().getName())) {
      return emitFloatUnary(call, 65);
    }
    if ("REPEAT".equalsIgnoreCase(call.getOperator().getName())) {
      // REPEAT(s, n): repeat s n times — operands [s, n], same order as DataFusion repeat.
      List<RexNode> args = call.getOperands();
      if (args.size() != 2) {
        return reject("REPEAT requires 2 arguments");
      }
      add(KIND_CALL, 66, 2);
      return emit(args.get(0)) && emit(args.get(1));
    }
    if ("LEFT".equalsIgnoreCase(call.getOperator().getName())) {
      return emitBoundedSubstr(call, 69);
    }
    if ("RIGHT".equalsIgnoreCase(call.getOperator().getName())) {
      return emitBoundedSubstr(call, 70);
    }
    if ("LPAD".equalsIgnoreCase(call.getOperator().getName())) {
      return emitPad(call, 82);
    }
    if ("RPAD".equalsIgnoreCase(call.getOperator().getName())) {
      return emitPad(call, 83);
    }
    // Functions whose native result can differ from the host — locale case folding (UPPER/LOWER) and
    // last-ULP transcendental math. They fall back unless the allowIncompatible flag opts them in.
    Integer incompatUnaryOp =
        INCOMPATIBLE_UNARY.get(call.getOperator().getName().toUpperCase(java.util.Locale.ROOT));
    if (incompatUnaryOp != null) {
      return emitIncompatibleUnary(call, incompatUnaryOp);
    }
    if ("POWER".equalsIgnoreCase(call.getOperator().getName())
        || "POW".equalsIgnoreCase(call.getOperator().getName())) {
      return emitIncompatiblePower(call);
    }
    if ("ROUND".equalsIgnoreCase(call.getOperator().getName())) {
      return emitIncompatibleRound(call);
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
    if (call.getOperator()
        instanceof org.apache.flink.table.planner.functions.bridging.BridgingSqlFunction) {
      return emitUdf(call);
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
      case IS_TRUE:
      case IS_NOT_TRUE:
      case IS_FALSE:
      case IS_NOT_FALSE:
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
    RelDataType sourceType = call.getOperands().get(0).getType();
    RelDataType resultType = call.getType();
    // A cast that leaves the value unchanged — same base type and precision/scale, differing only in
    // nullability or a time-attribute marker (Flink's `CAST(... ):TIMESTAMP_LTZ *ROWTIME*` that marks
    // the event-time column) — is an identity projection: emit the operand so the column passes through.
    if (sourceType.getSqlTypeName() == resultType.getSqlTypeName()
        && sourceType.getPrecision() == resultType.getPrecision()
        && sourceType.getScale() == resultType.getScale()) {
      return emit(call.getOperands().get(0));
    }
    SqlTypeName source = sourceType.getSqlTypeName();
    SqlTypeName targetType = resultType.getSqlTypeName();
    // A non-narrowing cast to VARCHAR from a CHAR or VARCHAR source (target length ≥ source). Flink
    // stores both as unpadded StringData and neither pads nor truncates a widening string cast, so the
    // value is unchanged — emit the operand as a passthrough. This covers a bounded computed string
    // coerced to an unbounded STRING sink column (Nexmark q14/q21's CASE result) and a CHAR literal
    // unified up to VARCHAR (the ELSE branch of COALESCE(s, 'x') / CASE). Narrowing (target < source)
    // truncates and is not admitted; casting *to* CHAR(n) pads, so it is not admitted either.
    if ((source == SqlTypeName.VARCHAR || source == SqlTypeName.CHAR)
        && targetType == SqlTypeName.VARCHAR
        && resultType.getPrecision() >= sourceType.getPrecision()) {
      return emit(call.getOperands().get(0));
    }
    // A cast to DECIMAL from an exact source (another DECIMAL, e.g. coercing q1's `0.908 * price` to
    // the sink's DECIMAL(23,3), or an integer) is byte-exact natively: Arrow rescales Decimal128 with
    // HALF_UP rounding, the same mode Flink uses. A float/double or string source falls through to
    // the host-exact cast upcall below.
    if (targetType == SqlTypeName.DECIMAL) {
      boolean exactSource =
          source == SqlTypeName.DECIMAL || numericRank(source) >= 0 && numericRank(source) <= 3;
      if (exactSource) {
        add(KIND_CAST_DECIMAL, resultType.getPrecision() * 100 + resultType.getScale(), 1);
        return emit(call.getOperands().get(0));
      }
    }
    int target = wideningTargetCode(source, targetType);
    if (target >= 0) {
      add(KIND_CAST, target, 1);
      return emit(call.getOperands().get(0));
    }
    // A narrowing cast to an integer target (a wider integer, or a float/double, narrowed to an
    // integer type). Flink emits the primitive Java cast: an integer source truncates to the low bits
    // (two's-complement wraparound), a float/double source rounds toward zero and saturates to the
    // target range with NaN→0. Rust's `as` reproduces both exactly, so a dedicated native wrapping
    // kernel matches the host — where arrow's own cast would instead error on overflow. See
    // divergences/07.
    int narrowTarget = narrowingIntTargetCode(source, targetType);
    if (narrowTarget >= 0) {
      add(KIND_CAST_NARROW, narrowTarget, 1);
      return emit(call.getOperands().get(0));
    }
    // The casts whose formatting/parsing the native engine cannot reproduce byte-for-byte — a number
    // (incl. decimal) to/from a string, narrowing a string / padding to CHAR(n), and the inexact
    // float/double→DECIMAL — run Flink's own CastExecutor through the columnar JVM upcall, so
    // trailing zeros, scientific-notation thresholds, trim semantics, and failure behavior are the
    // host's own (see HostCastFunction).
    if (hostCastSupported(sourceType, resultType)) {
      return emitHostCast(call, sourceType, resultType);
    }
    return reject("unsupported CAST " + source + "→" + targetType);
  }

  /** The number↔string / string-length / float→decimal casts routed through the host's cast rules. */
  private static boolean hostCastSupported(RelDataType sourceType, RelDataType resultType) {
    SqlTypeName source = sourceType.getSqlTypeName();
    SqlTypeName target = resultType.getSqlTypeName();
    boolean sourceString = source == SqlTypeName.VARCHAR || source == SqlTypeName.CHAR;
    boolean targetString = target == SqlTypeName.VARCHAR || target == SqlTypeName.CHAR;
    boolean sourceNumeric = numericRank(source) >= 0 || source == SqlTypeName.DECIMAL;
    boolean targetNumeric = numericRank(target) >= 0 || target == SqlTypeName.DECIMAL;
    boolean sourceFloat =
        source == SqlTypeName.FLOAT || source == SqlTypeName.REAL || source == SqlTypeName.DOUBLE;
    return (sourceNumeric && targetString)
        || (sourceString && targetNumeric)
        || (sourceString && targetString)
        || (sourceFloat && target == SqlTypeName.DECIMAL);
  }

  /** The UDF marshalling code for a host-cast operand/result, or -1 for a type the upcall can't carry. */
  private static int hostCastTypeCode(RelDataType type) {
    if (type.getSqlTypeName() == SqlTypeName.DECIMAL) {
      return io.github.jordepic.streamfusion.operator.NativeUdf.decimalType(
          type.getPrecision(), type.getScale());
    }
    return udfTypeCode(type.getSqlTypeName());
  }

  /** Emits a host-exact cast as a JVM-upcall node running Flink's own {@code CastExecutor}. */
  private boolean emitHostCast(RexCall call, RelDataType sourceType, RelDataType resultType) {
    if (legacyCastBehaviour == null || legacyCastBehaviour) {
      // The executor is built with the default cast behavior; legacy mode's null-on-failure (or an
      // unknown setting, on a bare predicate encode) would diverge from it.
      return reject(
          "CAST "
              + sourceType.getSqlTypeName()
              + "→"
              + resultType.getSqlTypeName()
              + ": host-exact cast runs only under the default (non-legacy) cast behavior");
    }
    int sourceCode = hostCastTypeCode(sourceType);
    int targetCode = hostCastTypeCode(resultType);
    if (sourceCode < 0 || targetCode < 0) {
      return reject("unsupported host-cast operand type " + sourceType.getSqlTypeName());
    }
    HostCastFunction function =
        new HostCastFunction(
            org.apache.flink.table.planner.calcite.FlinkTypeFactory.toLogicalType(sourceType),
            org.apache.flink.table.planner.calcite.FlinkTypeFactory.toLogicalType(resultType));
    java.lang.reflect.Method eval;
    try {
      eval = HostCastFunction.class.getMethod("eval", Object.class);
    } catch (ReflectiveOperationException e) {
      return reject("host cast unavailable: " + e.getMessage());
    }
    int localIndex =
        addUdf(
            io.github.jordepic.streamfusion.operator.NativeUdf.Descriptor.forFunction(
                function, eval, new int[] {sourceCode}, targetCode));
    add(KIND_UDF, longs.size(), 1);
    longs.add((long) localIndex);
    longs.add((long) targetCode);
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

  /**
   * The target integer code for a narrowing cast to an integer type — an integer→narrower-integer or a
   * float/double→integer cast — or -1 if {@code target} is not an integer type or the cast is not
   * narrowing (widening is handled by {@link #wideningTargetCode}, an identity/same-rank cast earlier).
   * Admitted because Flink's primitive Java cast wraps (integer source) / saturates with NaN→0 (float
   * source), which the native wrapping kernel reproduces.
   */
  private static int narrowingIntTargetCode(SqlTypeName source, SqlTypeName target) {
    int from = numericRank(source);
    if (from < 0) {
      return -1;
    }
    switch (target) {
      case TINYINT:
        return from > 0 ? CAST_TINYINT : -1;
      case SMALLINT:
        return from > 1 ? CAST_SMALLINT : -1;
      case INTEGER:
        return from > 2 ? CAST_INTEGER : -1;
      case BIGINT:
        return from > 3 ? CAST_BIGINT : -1;
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
  /**
   * Emits the SQL subscript {@code array[i]} / {@code map[key]} (Calcite's ITEM). Admitted only with
   * a literal subscript: DataFusion counts a runtime-negative array index from the end where Flink
   * returns NULL, and the native map lookup binds the key at compile time — so a non-literal
   * subscript falls back rather than risk a wrong answer. An array index literal below 1 also falls
   * back, leaving the host to raise its plan-time validation error. NULL-on-miss semantics (null
   * collection, index past the end, absent key) match Flink on the native side.
   */
  private boolean emitItem(RexCall call) {
    RexNode collection = call.getOperands().get(0);
    RexNode subscript = call.getOperands().get(1);
    if (!(subscript instanceof RexLiteral) || ((RexLiteral) subscript).isNull()) {
      return reject("non-literal ARRAY index / MAP key");
    }
    SqlTypeName collectionType = collection.getType().getSqlTypeName();
    if (collectionType == SqlTypeName.ARRAY) {
      if (!isIntLiteralAtLeast(subscript, 1)) {
        return reject("ARRAY index below 1");
      }
    } else if (collectionType != SqlTypeName.MAP) {
      return reject("ITEM over " + collectionType + " (only ARRAY and MAP)");
    }
    add(KIND_ITEM, 0, 2);
    return emit(collection) && emit(subscript);
  }

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

  /**
   * Emits {@code LEFT}/{@code RIGHT}(s, n) (op {@code op}) admitted only when {@code n} is an integer
   * literal ≥ 0: Flink returns the empty string for a negative count while DataFusion drops that many
   * characters from the other end, so a negative or runtime count falls back.
   */
  private boolean emitBoundedSubstr(RexCall call, int op) {
    List<RexNode> args = call.getOperands();
    if (args.size() != 2) {
      return reject(call.getOperator().getName() + " requires 2 arguments");
    }
    if (!isIntLiteralAtLeast(args.get(1), 0)) {
      return reject(call.getOperator().getName() + " requires a literal count ≥ 0");
    }
    add(KIND_CALL, op, 2);
    return emit(args.get(0)) && emit(args.get(1));
  }

  /**
   * Emits {@code LPAD}/{@code RPAD}(s, len [, pad]) (op {@code op}) with the length a literal ≥ 0 and
   * the pad string (if present) a literal — matching DataFusion Comet's scalar-pad constraint and
   * avoiding the negative/runtime-length edges, the same gating as LEFT/RIGHT/SUBSTRING.
   */
  private boolean emitPad(RexCall call, int op) {
    List<RexNode> args = call.getOperands();
    if (args.size() != 2 && args.size() != 3) {
      return reject(call.getOperator().getName() + " requires 2 or 3 arguments");
    }
    if (!isIntLiteralAtLeast(args.get(1), 0)) {
      return reject(call.getOperator().getName() + " requires a literal length ≥ 0");
    }
    if (args.size() == 3 && !(args.get(2) instanceof RexLiteral)) {
      return reject(call.getOperator().getName() + " pad string must be a literal");
    }
    add(KIND_CALL, op, args.size());
    for (RexNode arg : args) {
      if (!emit(arg)) {
        return false;
      }
    }
    return true;
  }

  /**
   * Emits {@code SPLIT_INDEX(str, sep, index)} (op 85). Admitted only when {@code sep} is a non-empty
   * string literal: with a whole, non-empty separator the native split reproduces Flink's
   * {@code splitByWholeSeparatorPreserveAllTokens} exactly. An empty or runtime separator (or the
   * char-code overload) falls back.
   */
  private boolean emitSplitIndex(List<RexNode> args) {
    if (args.size() != 3) {
      return reject("SPLIT_INDEX requires 3 arguments");
    }
    RexNode separator = args.get(1);
    if (!(separator instanceof RexLiteral)) {
      return reject("SPLIT_INDEX requires a literal separator");
    }
    String value = ((RexLiteral) separator).getValueAs(String.class);
    if (value == null || value.isEmpty()) {
      return reject("SPLIT_INDEX requires a non-empty separator");
    }
    add(KIND_CALL, 85, 3);
    for (RexNode arg : args) {
      if (!emit(arg)) {
        return false;
      }
    }
    return true;
  }

  /**
   * Emits {@code REGEXP_EXTRACT(str, pattern[, groupIndex])}. By default it routes through the host's own
   * {@code SqlFunctionUtils.regexpExtract} as a JVM-upcall node (see {@link #emitRegexpExtractJvm}), so the
   * regex is evaluated by {@code java.util.regex} exactly as Flink would — byte-identical, and the rest of
   * the expression still runs natively. Under the {@code allowIncompatible} flag it instead uses the pure
   * native Rust {@code regex} path (op 88, faster, no JVM crossing), which agrees with Java on common
   * syntax but can diverge on advanced features (backreferences, lookaround, some Unicode/class edges).
   */
  private boolean emitRegexpExtract(List<RexNode> args) {
    if (NativeConfig.allowsIncompatible("REGEXP_EXTRACT")) {
      return emitRegexpExtractNative(args);
    }
    return emitRegexpExtractJvm(args);
  }

  /**
   * The pure-native REGEXP_EXTRACT (op 88), opt-in behind the allowIncompatible flag. The pattern must be
   * a string literal (so the native side compiles it once per batch) and the group index a literal ≥ 0.
   * The two-argument form (no explicit index) falls back.
   */
  private boolean emitRegexpExtractNative(List<RexNode> args) {
    if (args.size() != 3) {
      return reject("REGEXP_EXTRACT requires 3 arguments (str, pattern, groupIndex)");
    }
    if (!(args.get(1) instanceof RexLiteral)) {
      return reject("REGEXP_EXTRACT requires a literal pattern");
    }
    if (!isIntLiteralAtLeast(args.get(2), 0)) {
      return reject("REGEXP_EXTRACT requires a literal group index ≥ 0");
    }
    add(KIND_CALL, 88, 3);
    for (RexNode arg : args) {
      if (!emit(arg)) {
        return false;
      }
    }
    return true;
  }

  /**
   * Emits {@code REGEXP_EXTRACT} as a JVM-upcall node (op {@link #KIND_UDF}) backed by
   * {@code NativeBuiltinFunctions.regexpExtract} — Flink's own {@code SqlFunctionUtils.regexpExtract}
   * logic byte-for-byte, minus its per-call {@code Pattern.compile}. The native engine packs the
   * argument columns and upcalls per batch, so the match runs under {@code java.util.regex} and is
   * byte-identical to the host for every pattern (the two- and three-argument forms both apply). The
   * string arguments must be string-typed and the group index INTEGER or BIGINT; anything else falls
   * back.
   */
  private boolean emitRegexpExtractJvm(List<RexNode> args) {
    if (args.size() != 2 && args.size() != 3) {
      return reject("REGEXP_EXTRACT requires 2 or 3 arguments (str, pattern[, groupIndex])");
    }
    for (int i = 0; i < 2; i++) {
      if (udfTypeCode(args.get(i).getType().getSqlTypeName())
          != io.github.jordepic.streamfusion.operator.NativeUdf.TYPE_STRING) {
        return reject("REGEXP_EXTRACT requires string str/pattern arguments");
      }
    }
    Class<?>[] signature;
    int[] argCodes;
    if (args.size() == 3) {
      int indexCode = udfTypeCode(args.get(2).getType().getSqlTypeName());
      Class<?> indexClass;
      if (indexCode == io.github.jordepic.streamfusion.operator.NativeUdf.TYPE_INT) {
        indexClass = int.class;
      } else if (indexCode == io.github.jordepic.streamfusion.operator.NativeUdf.TYPE_LONG) {
        indexClass = long.class;
      } else {
        return reject("REGEXP_EXTRACT group index must be INTEGER or BIGINT");
      }
      signature = new Class<?>[] {String.class, String.class, indexClass};
      argCodes =
          new int[] {
            io.github.jordepic.streamfusion.operator.NativeUdf.TYPE_STRING,
            io.github.jordepic.streamfusion.operator.NativeUdf.TYPE_STRING,
            indexCode
          };
    } else {
      signature = new Class<?>[] {String.class, String.class};
      argCodes =
          new int[] {
            io.github.jordepic.streamfusion.operator.NativeUdf.TYPE_STRING,
            io.github.jordepic.streamfusion.operator.NativeUdf.TYPE_STRING
          };
    }
    java.lang.reflect.Method regexpExtract;
    try {
      regexpExtract =
          io.github.jordepic.streamfusion.operator.NativeBuiltinFunctions.class.getMethod(
              "regexpExtract", signature);
    } catch (ReflectiveOperationException e) {
      return reject("REGEXP_EXTRACT host implementation unavailable: " + e.getMessage());
    }
    int returnCode = io.github.jordepic.streamfusion.operator.NativeUdf.TYPE_STRING;
    int localIndex =
        addUdf(
            io.github.jordepic.streamfusion.operator.NativeUdf.Descriptor.forBuiltin(
                regexpExtract, argCodes, returnCode));
    add(KIND_UDF, longs.size(), args.size());
    longs.add((long) localIndex);
    longs.add((long) returnCode);
    for (RexNode arg : args) {
      if (!emit(arg)) {
        return false;
      }
    }
    return true;
  }

  /**
   * Emits a Flink user {@link org.apache.flink.table.functions.ScalarFunction} call as a JVM-upcall node
   * (op {@link #KIND_UDF}). The function is registered in {@link
   * io.github.jordepic.streamfusion.operator.NativeUdf} and invoked per batch by the native {@code
   * JvmUdf} expression, which runs the actual {@code eval} over the Arrow argument columns (columnar) —
   * so the result is byte-identical to Flink. Admitted only when the definition is a {@code
   * ScalarFunction}, every argument and the result map to a supported marshalling type, and exactly one
   * {@code eval} overload of the right arity exists (an ambiguous overload is not guessed); otherwise the
   * call falls back.
   */
  private boolean emitUdf(RexCall call) {
    org.apache.flink.table.functions.FunctionDefinition def =
        ((org.apache.flink.table.planner.functions.bridging.BridgingSqlFunction) call.getOperator())
            .getDefinition();
    if (!(def instanceof org.apache.flink.table.functions.ScalarFunction)) {
      return reject("unsupported function/operator: " + call.getOperator().getName());
    }
    org.apache.flink.table.functions.ScalarFunction scalar =
        (org.apache.flink.table.functions.ScalarFunction) def;
    int returnCode = udfTypeCode(call.getType().getSqlTypeName());
    if (returnCode < 0) {
      return reject("UDF return type not native: " + call.getType().getSqlTypeName());
    }
    List<RexNode> args = call.getOperands();
    int[] argCodes = new int[args.size()];
    for (int i = 0; i < args.size(); i++) {
      argCodes[i] = udfTypeCode(args.get(i).getType().getSqlTypeName());
      if (argCodes[i] < 0) {
        return reject("UDF argument type not native: " + args.get(i).getType().getSqlTypeName());
      }
    }
    java.lang.reflect.Method eval = resolveEval(scalar, args.size());
    if (eval == null) {
      return reject(
          "UDF " + scalar.getClass().getName() + " has no single eval of arity " + args.size());
    }
    int localIndex =
        addUdf(
            io.github.jordepic.streamfusion.operator.NativeUdf.Descriptor.forFunction(
                scalar, eval, argCodes, returnCode));
    add(KIND_UDF, longs.size(), args.size());
    longs.add((long) localIndex);
    longs.add((long) returnCode);
    for (RexNode arg : args) {
      if (!emit(arg)) {
        return false;
      }
    }
    return true;
  }

  /** The native UDF marshalling type code for a SQL type (see NativeUdf.TYPE_*), or -1 if unsupported. */
  private static int udfTypeCode(SqlTypeName type) {
    switch (type) {
      case VARCHAR:
      case CHAR:
        return 0; // TYPE_STRING
      case BIGINT:
        return 1; // TYPE_LONG
      case INTEGER:
        return 2; // TYPE_INT
      case DOUBLE:
        return 3; // TYPE_DOUBLE
      case BOOLEAN:
        return 4; // TYPE_BOOLEAN
      case FLOAT:
      case REAL:
        return 5; // TYPE_FLOAT
      case SMALLINT:
        return 6; // TYPE_SHORT
      case TINYINT:
        return 7; // TYPE_BYTE
      default:
        return -1;
    }
  }

  /**
   * Emits {@code EXTRACT(unit FROM ts)} (op 89), the lowering of {@code YEAR}/{@code MONTH}/{@code
   * HOUR}/… (Nexmark q14's {@code HOUR(dateTime)}). Admitted only over a plain {@code TIMESTAMP} (a
   * local-zoned timestamp's fields depend on the session zone, like {@code DATE_FORMAT}) for the
   * unambiguous integer fields whose value is identical in Flink and chrono; anything else — a fractional
   * result (e.g. {@code SECOND} carrying millis, whose result type is decimal) or a
   * convention-divergent unit ({@code DOW}/{@code WEEK}/{@code QUARTER}) — falls back.
   */
  private boolean emitExtract(RexCall call) {
    List<RexNode> operands = call.getOperands();
    if (operands.size() != 2 || !(operands.get(0) instanceof RexLiteral)) {
      return reject("unsupported EXTRACT form");
    }
    RexNode source = operands.get(1);
    SqlTypeName sourceType = source.getType().getSqlTypeName();
    if (sourceType != SqlTypeName.TIMESTAMP
        && sourceType != SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE) {
      return reject("EXTRACT: only a TIMESTAMP or TIMESTAMP_LTZ argument is supported");
    }
    SqlTypeName returnType = call.getType().getSqlTypeName();
    if (returnType != SqlTypeName.BIGINT && returnType != SqlTypeName.INTEGER) {
      return reject("EXTRACT: only integer-valued fields supported");
    }
    Object unit = ((RexLiteral) operands.get(0)).getValue();
    String field = extractField(unit == null ? null : unit.toString());
    if (field == null) {
      return reject("EXTRACT: unsupported time unit " + unit);
    }
    if (sourceType == SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE) {
      return emitExtractLtz(source, unit, field, returnType);
    }
    add(KIND_CALL, 89, 2);
    if (!emit(source)) {
      return false;
    }
    add(KIND_LIT_STRING, strings.size(), 0);
    strings.add(field);
    return true;
  }

  /**
   * {@code EXTRACT} over a {@code TIMESTAMP_LTZ}, whose fields depend on the session time zone. By default
   * routes through Flink's own {@code DateTimeUtils.extractFromTimestamp(unit, ts, zone)} via the JVM
   * upcall (byte-identical); behind {@code allowIncompatible} emits the native {@code chrono-tz} path
   * (op 91: the timestamp, the chrono field name, the zone id).
   */
  private boolean emitExtractLtz(RexNode source, Object unit, String field, SqlTypeName returnType) {
    if (sessionZoneId == null) {
      return reject("EXTRACT over TIMESTAMP_LTZ: session time zone unavailable");
    }
    int returnCode =
        returnType == SqlTypeName.INTEGER
            ? io.github.jordepic.streamfusion.operator.NativeUdf.TYPE_INT
            : io.github.jordepic.streamfusion.operator.NativeUdf.TYPE_LONG;
    if (NativeConfig.allowsIncompatible("EXTRACT")) {
      if (!nativeZoneSupported(sessionZoneId)) {
        return reject("EXTRACT: native path needs an IANA or fixed-offset session time zone");
      }
      add(KIND_CALL, 91, 3);
      if (!emit(source)) {
        return false;
      }
      add(KIND_LIT_STRING, strings.size(), 0);
      strings.add(field);
      add(KIND_LIT_STRING, strings.size(), 0);
      strings.add(sessionZoneId);
      return true;
    }
    return emitLtzUpcall(
        source,
        new io.github.jordepic.streamfusion.operator.LtzDateTimeFunctions.Extract(
            unit.toString(), sessionZoneId),
        returnCode);
  }

  /** The chrono field name for a Calcite {@code TimeUnitRange}, or null if not a supported integer field. */
  private static String extractField(String unit) {
    if (unit == null) {
      return null;
    }
    switch (unit) {
      case "YEAR":
        return "year";
      case "MONTH":
        return "month";
      case "DAY":
        return "day";
      case "HOUR":
        return "hour";
      case "MINUTE":
        return "minute";
      case "SECOND":
        return "second";
      default:
        return null;
    }
  }

  /** The single public {@code eval} of the given arity, or null if none or more than one matches. */
  private static java.lang.reflect.Method resolveEval(Object function, int arity) {
    java.lang.reflect.Method match = null;
    for (java.lang.reflect.Method method : function.getClass().getMethods()) {
      if (method.getName().equals("eval") && method.getParameterCount() == arity) {
        if (match != null) {
          return null; // ambiguous — do not guess which overload the host would pick
        }
        match = method;
      }
    }
    return match;
  }

  /**
   * Emits {@code TO_TIMESTAMP_LTZ(epoch, precision)} (op 87). Admitted only for the millisecond form
   * ({@code precision} literal 3) over an integer epoch — the only shape the native side reads (epoch
   * millis → the nanosecond/no-tz timestamp ArrowConversion pins every timestamp column to). Any other
   * precision (seconds, micros) or a non-literal precision falls back.
   */
  private boolean emitToTimestampLtz(List<RexNode> args) {
    if (args.size() != 2) {
      return reject("TO_TIMESTAMP_LTZ requires 2 arguments");
    }
    if (!(args.get(1) instanceof RexLiteral)) {
      return reject("TO_TIMESTAMP_LTZ requires a literal precision");
    }
    Object precision = ((RexLiteral) args.get(1)).getValueAs(Integer.class);
    if (!Integer.valueOf(3).equals(precision)) {
      return reject("TO_TIMESTAMP_LTZ: only millisecond precision (3) is supported");
    }
    add(KIND_CALL, 87, 1);
    return emit(args.get(0));
  }

  /**
   * Emits {@code DATE_FORMAT(timestamp, format)} (op 86). Admitted only for a plain {@code TIMESTAMP}
   * (not local-zoned, whose formatting would depend on the session zone) with a literal format whose
   * Java pattern translates to a byte-identical chrono pattern (see {@link #toChronoFormat}); the
   * translated pattern is passed to the native side. Anything else falls back.
   */
  private boolean emitDateFormat(List<RexNode> args) {
    if (args.size() != 2) {
      return reject("DATE_FORMAT requires 2 arguments");
    }
    RexNode timestamp = args.get(0);
    SqlTypeName tsType = timestamp.getType().getSqlTypeName();
    if (tsType != SqlTypeName.TIMESTAMP && tsType != SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE) {
      return reject("DATE_FORMAT: only a TIMESTAMP or TIMESTAMP_LTZ argument is supported");
    }
    if (!(args.get(1) instanceof RexLiteral)) {
      return reject("DATE_FORMAT: format must be a literal");
    }
    String javaPattern = ((RexLiteral) args.get(1)).getValueAs(String.class);
    if (tsType == SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE) {
      return emitDateFormatLtz(timestamp, javaPattern);
    }
    String chrono = toChronoFormat(javaPattern);
    if (chrono == null) {
      return reject("DATE_FORMAT: unsupported format pattern");
    }
    add(KIND_CALL, 86, 2);
    if (!emit(timestamp)) {
      return false;
    }
    add(KIND_LIT_STRING, strings.size(), 0);
    strings.add(chrono);
    return true;
  }

  /**
   * {@code DATE_FORMAT} over a {@code TIMESTAMP_LTZ}, whose formatting depends on the session time zone.
   * By default it routes through Flink's own {@code DateTimeUtils.formatTimestamp(ts, pattern, zone)} via
   * the columnar JVM upcall — byte-identical. Behind {@code allowIncompatible} it emits the pure-native
   * {@code chrono-tz} path (op 90: the timestamp, the chrono pattern, the zone id), which can diverge from
   * the JVM at tz-database edges (see the divergences note).
   */
  private boolean emitDateFormatLtz(RexNode timestamp, String javaPattern) {
    if (sessionZoneId == null) {
      return reject("DATE_FORMAT over TIMESTAMP_LTZ: session time zone unavailable");
    }
    if (NativeConfig.allowsIncompatible("DATE_FORMAT")) {
      String chrono = toChronoFormat(javaPattern);
      if (chrono == null) {
        return reject("DATE_FORMAT: unsupported format pattern");
      }
      if (!nativeZoneSupported(sessionZoneId)) {
        return reject("DATE_FORMAT: native path needs an IANA or fixed-offset session time zone");
      }
      add(KIND_CALL, 90, 3);
      if (!emit(timestamp)) {
        return false;
      }
      add(KIND_LIT_STRING, strings.size(), 0);
      strings.add(chrono);
      add(KIND_LIT_STRING, strings.size(), 0);
      strings.add(sessionZoneId);
      return true;
    }
    return emitLtzUpcall(
        timestamp,
        new io.github.jordepic.streamfusion.operator.LtzDateTimeFunctions.DateFormat(
            javaPattern, sessionZoneId),
        io.github.jordepic.streamfusion.operator.NativeUdf.TYPE_STRING);
  }

  /**
   * Emits an arity-1 JVM upcall over a single {@code TIMESTAMP_LTZ} column (marshalled as epoch millis),
   * backed by the given serializable {@link org.apache.flink.table.functions.ScalarFunction} that calls
   * Flink's own zone-aware datetime code — the LTZ parity path, mirroring {@link #emitRegexpExtractJvm}.
   */
  private boolean emitLtzUpcall(
      RexNode timestamp,
      org.apache.flink.table.functions.ScalarFunction function,
      int returnCode) {
    java.lang.reflect.Method eval;
    try {
      eval = function.getClass().getMethod("eval", Long.class);
    } catch (ReflectiveOperationException e) {
      return reject("LTZ datetime host implementation unavailable: " + e.getMessage());
    }
    int localIndex =
        addUdf(
            io.github.jordepic.streamfusion.operator.NativeUdf.Descriptor.forFunction(
                function,
                eval,
                new int[] {io.github.jordepic.streamfusion.operator.NativeUdf.TYPE_TIMESTAMP},
                returnCode));
    add(KIND_UDF, longs.size(), 1);
    longs.add((long) localIndex);
    longs.add((long) returnCode);
    return emit(timestamp);
  }

  /**
   * Whether the native {@code chrono-tz} path accepts this session zone: IANA names ({@code Region/City}),
   * {@code UTC}, and fixed offsets ({@code +HH:MM}). Legacy JVM forms ({@code GMT+1}, {@code PST}) that
   * {@code chrono-tz} rejects fall back, so we never diverge silently on an unparseable zone.
   */
  private static boolean nativeZoneSupported(String zoneId) {
    return "UTC".equals(zoneId)
        || zoneId.indexOf('/') >= 0
        || zoneId.matches("[+-]\\d{2}:\\d{2}");
  }

  /**
   * Translates a Java {@code DateTimeFormatter} pattern to the equivalent chrono strftime pattern, or
   * null if it uses any field the translation can't reproduce byte-for-byte. Only zero-padded numeric
   * fields (the unambiguous ones) and literal separators are admitted; text fields, fractional seconds,
   * am/pm, zones, and single-letter (non-padded) fields fall back.
   */
  private static String toChronoFormat(String pattern) {
    if (pattern == null || pattern.isEmpty()) {
      return null;
    }
    StringBuilder out = new StringBuilder();
    int i = 0;
    while (i < pattern.length()) {
      char c = pattern.charAt(i);
      if (Character.isLetter(c)) {
        int j = i;
        while (j < pattern.length() && pattern.charAt(j) == c) {
          j++;
        }
        String token;
        switch (pattern.substring(i, j)) {
          case "yyyy":
            token = "%Y";
            break;
          case "yy":
            token = "%y";
            break;
          case "MM":
            token = "%m";
            break;
          case "dd":
            token = "%d";
            break;
          case "HH":
            token = "%H";
            break;
          case "mm":
            token = "%M";
            break;
          case "ss":
            token = "%S";
            break;
          default:
            return null;
        }
        out.append(token);
        i = j;
      } else if (c == '%') {
        out.append("%%"); // a literal percent — chrono's escape
        i++;
      } else {
        out.append(c); // separator / literal passes through
        i++;
      }
    }
    return out.toString();
  }

  /** A unary incompatible function, native only under its allowIncompatible flag. */
  private boolean emitIncompatibleUnary(RexCall call, int op) {
    String name = call.getOperator().getName();
    if (!NativeConfig.allowsIncompatible(name)) {
      // UPPER/LOWER have an exact default: route to the host's own case folding via a JVM upcall, so
      // they run natively and byte-identically without the flag. The transcendental math ops (last-ULP
      // divergence) have no such cheap exact path, so they still fall back unless opted in.
      if (op == 50) {
        return emitStringCaseJvm(call, "upper");
      }
      if (op == 51) {
        return emitStringCaseJvm(call, "lower");
      }
      return reject(incompatibleReason(name));
    }
    if (call.getOperands().size() != 1) {
      return reject(name + " requires one argument");
    }
    add(KIND_CALL, op, 1);
    return emit(call.getOperands().get(0));
  }

  /**
   * Emits {@code UPPER}/{@code LOWER} as a JVM-upcall node (op {@link #KIND_UDF}) backed by {@link
   * io.github.jordepic.streamfusion.operator.NativeBuiltinFunctions}, which calls Flink's own {@code
   * BinaryStringData} case folding — byte-identical to the host, with the rest of the expression still
   * native. The argument must be string-typed.
   */
  private boolean emitStringCaseJvm(RexCall call, String method) {
    List<RexNode> args = call.getOperands();
    if (args.size() != 1) {
      return reject(method + " requires one argument");
    }
    if (udfTypeCode(args.get(0).getType().getSqlTypeName())
        != io.github.jordepic.streamfusion.operator.NativeUdf.TYPE_STRING) {
      return reject(method + " requires a string argument");
    }
    java.lang.reflect.Method impl;
    try {
      impl =
          io.github.jordepic.streamfusion.operator.NativeBuiltinFunctions.class.getMethod(
              method, org.apache.flink.table.data.binary.BinaryStringData.class);
    } catch (ReflectiveOperationException e) {
      return reject(method + " host implementation unavailable: " + e.getMessage());
    }
    int returnCode = io.github.jordepic.streamfusion.operator.NativeUdf.TYPE_STRING;
    int localIndex =
        addUdf(
            io.github.jordepic.streamfusion.operator.NativeUdf.Descriptor.forBuiltin(
                impl,
                new int[] {io.github.jordepic.streamfusion.operator.NativeUdf.TYPE_STRING},
                returnCode));
    add(KIND_UDF, longs.size(), 1);
    longs.add((long) localIndex);
    longs.add((long) returnCode);
    return emit(args.get(0));
  }

  /** {@code POWER(base, exp)} (also the lowering of {@code SQRT}), native only under the flag. */
  private boolean emitIncompatiblePower(RexCall call) {
    if (!NativeConfig.allowsIncompatible("POWER")) {
      return reject(incompatibleReason("POWER"));
    }
    List<RexNode> args = call.getOperands();
    if (args.size() != 2) {
      return reject("POWER requires 2 arguments");
    }
    add(KIND_CALL, 71, 2);
    return emit(args.get(0)) && emit(args.get(1));
  }

  /** {@code ROUND(x [, scale])} over float/double, native only under the flag. */
  private boolean emitIncompatibleRound(RexCall call) {
    if (!NativeConfig.allowsIncompatible("ROUND")) {
      return reject(incompatibleReason("ROUND"));
    }
    List<RexNode> args = call.getOperands();
    if (args.isEmpty() || args.size() > 2) {
      return reject("ROUND requires 1 or 2 arguments");
    }
    SqlTypeName type = args.get(0).getType().getSqlTypeName();
    if (type != SqlTypeName.FLOAT && type != SqlTypeName.REAL && type != SqlTypeName.DOUBLE) {
      return reject("ROUND: only float/double operands admitted");
    }
    if (args.size() == 2 && !(args.get(1) instanceof RexLiteral)) {
      return reject("ROUND: scale must be a literal");
    }
    add(KIND_CALL, 84, args.size());
    for (RexNode arg : args) {
      if (!emit(arg)) {
        return false;
      }
    }
    return true;
  }

  private static String incompatibleReason(String name) {
    return name
        + ": native result may differ from the host; enable with -Dstreamfusion.expression."
        + name.toUpperCase(java.util.Locale.ROOT)
        + ".allowIncompatible=true";
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
    // Compatible (always-native) unary functions only. Functions whose native result can diverge from
    // the host (UPPER/LOWER, transcendental math) are handled by the incompatible dispatch in
    // emitCall, gated behind the allowIncompatible flag — see INCOMPATIBLE_UNARY.
    switch (name.toUpperCase(java.util.Locale.ROOT)) {
      case "CHAR_LENGTH":
      case "CHARACTER_LENGTH":
        return 52;
      case "REVERSE":
        return 59;
      case "LTRIM":
        return 60;
      case "RTRIM":
        return 61;
      case "ASCII":
        return 67;
      case "CHR":
        return 81;
      default:
        return -1;
    }
  }

  /**
   * Emits a unary numeric function (op code {@code op}) admitted only over a float/double operand.
   * Integer cases are excluded: {@code ABS(INT_MIN)} overflows (Java wraps, DataFusion's checked
   * kernel errors), and {@code FLOOR}/{@code CEIL} on an integer is an identity Flink keeps in the
   * integer type — both divergence-prone, so they fall back.
   */
  private boolean emitFloatUnary(RexCall call, int op) {
    if (call.getOperands().size() != 1) {
      return reject(call.getOperator().getName() + ": requires one argument");
    }
    SqlTypeName type = call.getOperands().get(0).getType().getSqlTypeName();
    if (type != SqlTypeName.FLOAT && type != SqlTypeName.REAL && type != SqlTypeName.DOUBLE) {
      return reject(call.getOperator().getName() + ": only float/double operands admitted");
    }
    add(KIND_CALL, op, 1);
    return emit(call.getOperands().get(0));
  }

  /** Whether {@code call} is an arithmetic operation whose result is a DECIMAL (not yet native). */
  private static boolean isDecimalArithmetic(RexCall call) {
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
      case IS_TRUE:
        return 32;
      case IS_NOT_TRUE:
        return 33;
      case IS_FALSE:
        return 34;
      case IS_NOT_FALSE:
        return 35;
      case CASE:
        return 40;
      case LIKE:
        return 56;
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
