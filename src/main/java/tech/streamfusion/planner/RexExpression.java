package tech.streamfusion.planner;

import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Locale;
import java.util.Map;
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
import org.apache.calcite.sql.SqlJsonConstructorNullClause;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import tech.streamfusion.operator.EncodedPredicate;
import tech.streamfusion.operator.WatermarkExpression;

/**
 * Encodes a {@link RexNode} into the compact pre-order form the native engine decodes (see {@link
 * tech.streamfusion.Native#createFilterExpression}): parallel {@code kinds}/{@code payload}/{@code
 * childCounts} arrays plus typed literal pools. The encoding is the JVM counterpart of the native
 * expression builder, and admits only the operations the native side evaluates with verified Flink
 * parity; an unsupported node makes the whole encode fail (returning null), so the containing
 * operator falls back to Flink.
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
  // PROCTIME materializes a volatile execution-time TIMESTAMP_LTZ(3) column.
  private static final int KIND_PROCTIME = 12;
  // Field access: extract a named field from a ROW/struct-typed child. payload is the string-pool
  // index of the field name, with one child (the struct-typed expression). Nested access (a.b.c)
  // nests these, the child being itself a field access. Mirrors DataFusion's get_field.
  private static final int KIND_FIELD_ACCESS = 13;
  // Exact decimal cast: payload packs precision*100 + scale; one decimal or integer child.
  // Rounds HALF_UP before checking precision, returning NULL on overflow.
  private static final int KIND_CAST_DECIMAL = 14;
  // An exact DECIMAL literal: payload indexes the string pool, whose entry is
  // "unscaled|precision|scale"
  // (the unscaled integer as a string, as it can exceed i64). The native side builds a Decimal128
  // scalar,
  // so decimal arithmetic stays exact instead of routing through double.
  private static final int KIND_LIT_DECIMAL = 16;
  // A JVM UDF call: payload indexes the long pool at [udf id, return-type code]; children are the
  // argument expressions. The native JvmUdf node upcalls NativeUdf.invokeUdf to run the actual
  // Flink
  // ScalarFunction.eval over each batch (columnar), so a non-builtin UDF stays inside the native
  // island.
  private static final int KIND_UDF = 17;
  // A narrowing cast to an integer type: payload is the target integer type code (CAST_TINYINT..
  // CAST_BIGINT), one child. Built natively as a wrapping/saturating kernel matching Flink's
  // primitive
  // Java cast, since arrow's own cast errors on overflow rather than wrapping.
  private static final int KIND_CAST_NARROW = 18;

  // ARRAY[i] / MAP[key] subscript (Calcite's ITEM): collection and runtime index/key children.
  // Payload 1 marks a compact timestamp MAP key; Arrow's component layout erases this precision.
  private static final int KIND_ITEM = 19;

  // Decimal `/` and `%`: payload packs the declared result's precision*100 + scale; two children
  // (the
  // operands). A fused native kernel reproduces Flink's 38-significant-digit quotient + rescale.
  private static final int KIND_DECIMAL_DIVIDE = 20;
  private static final int KIND_DECIMAL_MOD = 21;

  // A single-precision literal, so a FLOAT expression is not widened to double by its constants.
  private static final int KIND_LIT_FLOAT = 22;
  private static final int KIND_LIT_BINARY = 23;
  private static final int KIND_LIT_TEMPORAL = 24;
  private static final int KIND_CLOCK = 25;
  private static final int KIND_LIT_TIMESTAMP = 29;
  private static final int KIND_DECIMAL_ROUND = 30;
  private static final int KIND_RANDOM = 32;
  private static final int KIND_STRING_TO_INTEGER = 33;
  private static final int KIND_INTEGER_TO_STRING = 34;
  private static final int KIND_DECIMAL_TRUNCATE = 35;
  private static final int KIND_FROM_UNIXTIME = 36;
  private static final int KIND_DECIMAL_FLOAT = 37;
  // Whole-row JVM evaluation; long pool [udf id, output Arrow schema string index].
  private static final int KIND_ROW_UDF = 38;
  // A typed NULL carries a one-field Arrow IPC schema in the string pool.
  private static final int KIND_LIT_TYPED_NULL = 31;
  // Fused exact arithmetic: payload is the declared result precision*100 + scale; two children.
  private static final int KIND_DECIMAL_ADD = 26;
  private static final int KIND_DECIMAL_SUBTRACT = 27;
  private static final int KIND_DECIMAL_MULTIPLY = 28;

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
  private static final Map<String, Integer> INCOMPATIBLE_UNARY =
      Map.ofEntries(
          Map.entry("UPPER", 50),
          Map.entry("LOWER", 51),
          Map.entry("EXP", 72),
          Map.entry("LN", 73),
          Map.entry("SIN", 74),
          Map.entry("COS", 75),
          Map.entry("TAN", 76),
          Map.entry("ASIN", 77),
          Map.entry("ACOS", 78),
          Map.entry("ATAN", 79),
          Map.entry("LOG10", 80));

  // UDFs referenced by KIND_UDF nodes, and the longs-pool slots holding their local indices. Rather
  // than registering into NativeUdf's (planner-JVM) registry at encode time and baking a global id
  // —
  // which a task manager's empty registry wouldn't have — we carry a serializable descriptor per
  // UDF
  // and bake its local index; the operator registers them at open() and patches the slots.
  private final List<tech.streamfusion.operator.NativeUdf.Descriptor> udfs = new ArrayList<>();
  private final List<Integer> udfIdSlots = new ArrayList<>();

  private final List<Integer> projectionRoots = new ArrayList<>();
  private int conditionRoot = -1;
  private String[] outputNames = new String[0];
  // Why the encode declined, set at the first (innermost) un-admitted node; null if it succeeded.
  private String reason;
  // The session time zone (table.local-time-zone) — needed to format/extract a TIMESTAMP_LTZ, whose
  // calendar fields depend on it. Predicate encoders receive their owning relational node too.
  private String sessionZoneId;
  // A bare predicate without a relational context conservatively declines host-exact numeric casts.
  private Boolean legacyCastBehaviour;
  private boolean watermarkAvailable;
  private org.apache.flink.configuration.ReadableConfig temporalConfig =
      new org.apache.flink.configuration.Configuration();
  // Root of the projection currently being encoded; null for conditions and bare predicates.
  private RexNode projectionRoot;
  private int binaryUdfCalls;
  private boolean rowFusion;
  private final java.util.Set<String> statefulUdfEvaluations = new java.util.HashSet<>();
  private ClassLoader expressionClassLoader = RexExpression.class.getClassLoader();

  private RexExpression() {}

  /**
   * Records the first decline reason and returns false, so callers can {@code return reject(...)}.
   */
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

  static RexExpression encode(RexNode node, org.apache.calcite.rel.RelNode context) {
    RexExpression encoder = new RexExpression();
    encoder.expressionClassLoader = org.apache.flink.table.planner.utils.ShortcutUtils.unwrapClassLoader(context);
    encoder.configure(
        org.apache.flink.table.planner.utils.ShortcutUtils.unwrapTableConfig(context));
    return encoder.emit(node) ? encoder : null;
  }

  /**
   * Packages an encoded single-expression predicate (its root at node 0) for a native join
   * operator, or {@link EncodedPredicate#NONE} when there is no predicate.
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
    CalcEncoding encoded = tryEncodeCalc(calc);
    return encoded.supported() ? encoded.encoder() : null;
  }

  static RexExpression encodeProjections(List<RexNode> projections, List<String> names) {
    RexExpression encoder = new RexExpression();
    for (RexNode projection : projections) {
      encoder.projectionRoot = projection;
      encoder.projectionRoots.add(encoder.kinds.size());
      if (!encoder.emit(projection)) {
        return null;
      }
    }
    encoder.outputNames = names.toArray(new String[0]);
    return encoder;
  }

  /**
   * A watermark projection uses millisecond-valued temporal kernels in the same expression tree as
   * Calc. Its intermediate results never become timestamp columns in the data pipeline.
   */
  static RexExpression encodeWatermark(RexNode expression, int rowtimeColumn, boolean epochMillis) {
    RexExpression encoder = new RexExpression();
    encoder.projectionRoots.add(0);
    encoder.outputNames = new String[] {"watermark_millis"};
    return encoder.emitWatermark(expression, rowtimeColumn, epochMillis) ? encoder : null;
  }

  private boolean emitWatermark(RexNode expression, int rowtimeColumn, boolean epochMillis) {
    if (isWatermarkRowtime(expression, rowtimeColumn, epochMillis)) {
      add(KIND_CALL, WatermarkExpression.TIMESTAMP_MILLIS, 1);
      add(KIND_INPUT_REF, rowtimeColumn, 0);
      return true;
    }
    if (!(expression instanceof RexCall)) {
      return reject("watermark requires a rowtime expression");
    }
    RexCall call = (RexCall) expression;
    if (call.getKind() != SqlKind.MINUS
        || call.getOperands().size() != 2
        || !(call.getOperands().get(1) instanceof RexLiteral)) {
      return reject("watermark admits subtraction of constant intervals");
    }
    RexLiteral interval = (RexLiteral) call.getOperands().get(1);
    SqlTypeFamily family = interval.getType().getSqlTypeName().getFamily();
    if (family != SqlTypeFamily.INTERVAL_DAY_TIME && family != SqlTypeFamily.INTERVAL_YEAR_MONTH) {
      return reject("watermark subtraction requires an interval");
    }
    BigDecimal value = interval.getValueAs(BigDecimal.class);
    if (value == null || value.signum() < 0) {
      return reject("watermark requires a nonnegative interval literal");
    }
    boolean months = family == SqlTypeFamily.INTERVAL_YEAR_MONTH;
    long amount;
    try {
      amount = months ? value.intValueExact() : value.longValueExact();
    } catch (ArithmeticException outOfRange) {
      return reject("watermark interval exceeds Flink's internal range");
    }
    add(
        KIND_CALL,
        months
            ? WatermarkExpression.SUBTRACT_MONTHS
            : WatermarkExpression.SUBTRACT_MILLIS,
        2);
    RexNode timestamp = call.getOperands().get(0);
    // Fuse a direct timestamp read with the shift; composed expressions keep their original order.
    if (isWatermarkRowtime(timestamp, rowtimeColumn, epochMillis)) {
      add(KIND_INPUT_REF, rowtimeColumn, 0);
    } else if (!emitWatermark(timestamp, rowtimeColumn, epochMillis)) {
      return false;
    }
    add(months ? KIND_LIT_INT : KIND_LIT_LONG, longs.size(), 0);
    longs.add(amount);
    return true;
  }

  private static boolean isWatermarkRowtime(RexNode node, int column, boolean epochMillis) {
    if (!epochMillis) {
      return node instanceof RexInputRef && ((RexInputRef) node).getIndex() == column;
    }
    if (!(node instanceof RexCall)) {
      return false;
    }
    RexCall call = (RexCall) node;
    if (!"TO_TIMESTAMP_LTZ".equals(call.getOperator().getName())
        || call.getOperands().size() != 2
        || !(call.getOperands().get(0) instanceof RexInputRef)
        || ((RexInputRef) call.getOperands().get(0)).getIndex() != column
        || !(call.getOperands().get(1) instanceof RexLiteral)) {
      return false;
    }
    BigDecimal precision = ((RexLiteral) call.getOperands().get(1)).getValueAs(BigDecimal.class);
    return precision != null && precision.compareTo(BigDecimal.valueOf(3)) == 0;
  }

  /**
   * Why {@code calc} cannot be encoded for the native engine (the first un-admitted op/operand), or
   * null if it can — for surfacing fallback reasons (ticket 29).
   */
  static String reasonForCalc(Calc calc) {
    CalcEncoding encoded = tryEncodeCalc(calc);
    return encoded.supported() ? null : encoded.encoder().reasonOrDefault();
  }

  private record CalcEncoding(RexExpression encoder, boolean supported) {}

  private static CalcEncoding tryEncodeCalc(Calc calc) {
    RexExpression encoder = forCalc(calc);
    boolean supported = encoder.emitCalc(calc);
    if (!supported && containsSqlJson(calc.getProgram())) {
      // A failed native attempt may have populated pools and UDF bindings. Generate the complete
      // Calc in a fresh encoder so short-circuiting and row evaluation order stay with Flink.
      encoder = forCalc(calc);
      supported = encoder.emitRowCalc(calc);
    }
    return new CalcEncoding(encoder, supported);
  }

  private static RexExpression forCalc(Calc calc) {
    RexExpression encoder = new RexExpression();
    encoder.expressionClassLoader =
        org.apache.flink.table.planner.utils.ShortcutUtils.unwrapClassLoader(calc);
    encoder.configure(org.apache.flink.table.planner.utils.ShortcutUtils.unwrapTableConfig(calc));
    encoder.watermarkAvailable = true;
    return encoder;
  }

  static boolean isRowCalc(int[] kinds) {
    return kinds.length > 0 && kinds[0] == KIND_ROW_UDF;
  }

  private void configure(org.apache.flink.table.api.TableConfig tableConfig) {
    sessionZoneId = tableConfig.getLocalTimeZone().getId();
    var expressionConfig = org.apache.flink.table.api.TableConfig.getDefault();
    expressionConfig.setRootConfiguration(tableConfig.getRootConfiguration());
    expressionConfig.addConfiguration(tableConfig.getConfiguration());
    expressionConfig.setLocalTimeZone(tableConfig.getLocalTimeZone());
    temporalConfig = expressionConfig;
    legacyCastBehaviour =
        tableConfig
            .get(
                org.apache.flink.table.api.config.ExecutionConfigOptions
                    .TABLE_EXEC_LEGACY_CAST_BEHAVIOUR)
            .isEnabled();
  }

  private boolean emitCalc(Calc calc) {
    RexProgram program = calc.getProgram();
    if (binaryUdfCallCount(program) > 1) return emitRowCalc(calc);
    projectionRoot = null;
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
      projectionRoot = projection;
      if (!emit(projection)) {
        return false;
      }
    }
    outputNames = calc.getRowType().getFieldNames().toArray(new String[0]);
    return true;
  }

  private static int binaryUdfCallCount(RexProgram program) {
    int[] count = {0};
    var visitor =
        new org.apache.calcite.rex.RexVisitorImpl<Void>(true) {
          @Override
          public Void visitCall(RexCall call) {
            if (call.getType().getSqlTypeName() == SqlTypeName.VARBINARY
                && call.getOperator()
                    instanceof
                    org.apache.flink.table.planner.functions.bridging.BridgingSqlFunction function
                && function.getDefinition()
                    instanceof org.apache.flink.table.functions.ScalarFunction) {
              count[0]++;
            }
            return super.visitCall(call);
          }
        };
    if (program.getCondition() != null)
      program.expandLocalRef(program.getCondition()).accept(visitor);
    for (RexLocalRef project : program.getProjectList())
      program.expandLocalRef(project).accept(visitor);
    return count[0];
  }

  static boolean containsSqlJson(RexProgram program) {
    if (program.getCondition() != null
        && containsSqlJson(program.expandLocalRef(program.getCondition()))) return true;
    return program.getProjectList().stream()
        .anyMatch(project -> containsSqlJson(program.expandLocalRef(project)));
  }

  private static boolean containsSqlJson(RexNode node) {
    if (node instanceof org.apache.calcite.rex.RexFieldAccess access) {
      return containsSqlJson(access.getReferenceExpr());
    }
    if (!(node instanceof RexCall call)) return false;
    return switch (call.getOperator().getName().toUpperCase(Locale.ROOT)) {
      case "JSON_VALUE",
          "JSON_EXISTS",
          "JSON_QUERY",
          "JSON_QUOTE",
          "JSON_UNQUOTE",
          "JSON_STRING",
          "JSON_OBJECT",
          "JSON_ARRAY",
          "JSON",
          "IS JSON VALUE",
          "IS NOT JSON VALUE",
          "IS JSON OBJECT",
          "IS NOT JSON OBJECT",
          "IS JSON ARRAY",
          "IS NOT JSON ARRAY",
          "IS JSON SCALAR",
          "IS NOT JSON SCALAR" ->
          true;
      default -> call.getOperands().stream().anyMatch(RexExpression::containsSqlJson);
    };
  }

  private static int rowCalcTypeCode(RelDataType type) {
    boolean nested =
        switch (type.getSqlTypeName()) {
          case ARRAY -> rowCalcTypeCode(type.getComponentType()) >= 0;
          case ROW ->
              type.getFieldList().stream().allMatch(field -> rowCalcTypeCode(field.getType()) >= 0);
          default -> false;
        };
    return nested ? tech.streamfusion.operator.NativeUdf.TYPE_INTERNAL : hostCastTypeCode(type);
  }

  private boolean emitRowCalc(Calc calc) {
    rowFusion = true;
    RexProgram program = calc.getProgram();
    List<RexNode> arguments = new ArrayList<>();
    List<org.apache.flink.table.types.logical.LogicalType> types = new ArrayList<>();
    List<Integer> codes = new ArrayList<>();
    Map<Integer, RexInputRef> inputs = new java.util.LinkedHashMap<>();
    var remap =
        new org.apache.calcite.rex.RexShuttle() {
          @Override
          public RexNode visitInputRef(RexInputRef input) {
            return inputs.computeIfAbsent(
                input.getIndex(),
                ignored -> {
                  int code = rowCalcTypeCode(input.getType());
                  if (code < 0)
                    throw new IllegalArgumentException(
                        "row-fused UDF input type is not supported: " + input.getType());
                  RexInputRef replacement = new RexInputRef(arguments.size(), input.getType());
                  arguments.add(input);
                  types.add(
                      org.apache.flink.table.planner.calcite.FlinkTypeFactory.toLogicalType(
                          input.getType()));
                  codes.add(code);
                  return replacement;
                });
          }
        };
    try {
      RexNode condition =
          program.getCondition() == null
              ? null
              : RexUtil.expandSearch(
                  calc.getCluster().getRexBuilder(),
                  null,
                  program.expandLocalRef(program.getCondition()));
      if (condition != null) {
        if (!validateGeneratedExpression(condition)) return false;
        condition = condition.accept(remap);
      }
      List<RexNode> projections = new ArrayList<>();
      for (RexLocalRef ref : program.getProjectList()) {
        RexNode projection =
            RexUtil.expandSearch(
                calc.getCluster().getRexBuilder(), null, program.expandLocalRef(ref));
        if (!validateGeneratedExpression(projection)) return false;
        if (rowCalcTypeCode(projection.getType()) < 0)
          return reject("row-fused UDF output type is not supported: " + projection.getType());
        projections.add(projection.accept(remap));
      }
      if (arguments.isEmpty()) {
        types.add(new org.apache.flink.table.types.logical.IntType());
        codes.add(tech.streamfusion.operator.NativeUdf.TYPE_INT);
      }
      var resultType =
          org.apache.flink.table.planner.calcite.FlinkTypeFactory.toLogicalRowType(
              calc.getRowType());
      var function =
          new FlinkExpressionFunction(
              projections,
              condition,
              types.toArray(org.apache.flink.table.types.logical.LogicalType[]::new),
              resultType,
              temporalConfig,
              expressionClassLoader);
      Method eval = FlinkExpressionFunction.class.getMethod("eval", Object[].class);
      int index =
          addUdf(
              tech.streamfusion.operator.NativeUdf.Descriptor.forFunction(
                  function,
                  eval,
                  codes.stream().mapToInt(Integer::intValue).toArray(),
                  tech.streamfusion.operator.NativeUdf.TYPE_ROW));
      projectionRoots.add(kinds.size());
      add(KIND_ROW_UDF, longs.size(), Math.max(1, arguments.size()));
      longs.add((long) index);
      longs.add((long) strings.size());
      var schema =
          tech.streamfusion.arrow.ArrowConversion.toArrowSchema(
              org.apache.flink.table.types.logical.RowType.of(resultType.copy(true)));
      strings.add(Base64.getEncoder().encodeToString(schema.serializeAsMessage()));
      if (arguments.isEmpty()) {
        add(KIND_LIT_INT, longs.size(), 0);
        longs.add(0L);
      }
      for (RexNode argument : arguments) if (!emit(argument)) return false;
      outputNames = calc.getRowType().getFieldNames().toArray(new String[0]);
      return true;
    } catch (Exception unsupported) {
      return reject("host Calc cannot be generated: " + unsupported.getMessage());
    }
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
   * Nested field access is encoded by name and the pruned fields keep their names, so it is
   * unaffected.
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
   * longs} pool in place of a plan-time global id); also notes the next {@code longs} slot as the
   * one holding that index, so the operator's {@link tech.streamfusion.operator.NativeUdf.Binding}
   * can patch it to the runtime id.
   */
  private int addUdf(tech.streamfusion.operator.NativeUdf.Descriptor descriptor) {
    int localIndex = udfs.size();
    udfs.add(descriptor);
    udfIdSlots.add(longs.size());
    return localIndex;
  }

  /** The UDF registrations this encoded expression needs, to carry into the operator. */
  tech.streamfusion.operator.NativeUdf.Binding udfBinding() {
    return new tech.streamfusion.operator.NativeUdf.Binding(
        udfs.toArray(new tech.streamfusion.operator.NativeUdf.Descriptor[0]),
        toIntArray(udfIdSlots));
  }

  /**
   * Appends {@code node} in pre-order; returns false (abandoning the encode) on an unsupported
   * node.
   */
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
    SqlTypeName type = literal.getType().getSqlTypeName();
    int temporalType = temporalTypeCode(literal.getType());
    if (temporalType == 11) {
      org.apache.flink.table.data.TimestampData timestamp = literal.isNull() ? null
          : org.apache.flink.table.data.TimestampData.fromLocalDateTime(java.time.LocalDateTime.parse(
              literal.getValueAs(org.apache.calcite.util.TimestampString.class).toString().replace(' ', 'T')));
      add(KIND_LIT_TIMESTAMP, longs.size(), 0);
      longs.add(timestamp == null ? 0L : 1L);
      longs.add(timestamp == null ? 0L : timestamp.getMillisecond());
      longs.add(timestamp == null ? 0L : (long) timestamp.getNanoOfMillisecond());
      return true;
    }
    if (temporalType >= 0) {
      long value = 0;
      if (!literal.isNull()) {
        try {
          value =
              switch (temporalType) {
                case 9 ->
                    literal
                        .getValueAs(org.apache.calcite.util.DateString.class)
                        .getDaysSinceEpoch();
                case 10 ->
                    literal.getValueAs(org.apache.calcite.util.TimeString.class).getMillisOfDay();
                case 12, 13 -> literal.getValueAs(Long.class);
                default ->
                    throw new IllegalArgumentException("unsupported temporal literal " + type);
              };
        } catch (ArithmeticException e) {
          return reject("temporal literal exceeds its integer range");
        }
      }
      add(KIND_LIT_TEMPORAL, longs.size(), 0);
      longs.add((long) temporalType);
      longs.add(literal.isNull() ? 0L : 1L);
      longs.add(value);
      return true;
    }
    if (literal.isNull()) {
      if (type == SqlTypeName.CHAR
          || type == SqlTypeName.VARCHAR
          || type == SqlTypeName.VARBINARY) {
        if (type == SqlTypeName.VARBINARY) {
          add(KIND_LIT_BINARY, longs.size(), 0);
          longs.add(-1L);
        } else {
          add(KIND_LIT_STRING, strings.size(), 0);
          strings.add(null);
        }
        return true;
      }
      if (type == SqlTypeName.NULL) {
        add(KIND_LIT_NULL, -1, 0);
        return true;
      }
      try {
        var logicalType =
            org.apache.flink.table.planner.calcite.FlinkTypeFactory.toLogicalType(
                literal.getType());
        var schema =
            tech.streamfusion.arrow.ArrowConversion.toArrowSchema(
                org.apache.flink.table.types.logical.RowType.of(logicalType));
        add(KIND_LIT_TYPED_NULL, strings.size(), 0);
        strings.add(Base64.getEncoder().encodeToString(schema.serializeAsMessage()));
      } catch (org.apache.flink.table.api.TableException
          | UnsupportedOperationException unsupportedType) {
        return reject("unsupported NULL literal type: " + literal.getType());
      }
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
          BigDecimal value = literal.getValueAs(BigDecimal.class);
          if (value == null) {
            return false;
          }
          // Emit as an exact Decimal128: one string-pool entry "unscaled|precision|scale", where
          // unscaled is the integer value at the declared scale (a string, as it can exceed i64).
          // Decimal arithmetic then stays in Decimal128 rather than routing through double.
          int precision = literal.getType().getPrecision();
          int scale = literal.getType().getScale();
          BigDecimal rounded = value.setScale(scale, RoundingMode.HALF_UP);
          if (rounded.precision() > precision) {
            add(KIND_CAST_DECIMAL, precision * 100 + scale, 1);
            add(KIND_LIT_NULL, -1, 0);
            return true;
          }
          BigInteger unscaled = rounded.unscaledValue();
          add(KIND_LIT_DECIMAL, strings.size(), 0);
          strings.add(unscaled + "|" + precision + "|" + scale);
          return true;
        }
      case FLOAT:
      case REAL:
        {
          Double value = literal.getValueAs(Double.class);
          if (value == null) {
            return false;
          }
          // The host evaluates FLOAT arithmetic in single precision. A double literal would widen
          // the
          // whole expression, changing both the result type the plan promised and its last bits.
          add(KIND_LIT_FLOAT, doubles.size(), 0);
          doubles.add(value);
          return true;
        }
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
      case VARBINARY:
        {
          org.apache.calcite.avatica.util.ByteString value =
              literal.getValueAs(org.apache.calcite.avatica.util.ByteString.class);
          if (value == null) {
            return false;
          }
          byte[] bytes = value.getBytes();
          add(KIND_LIT_BINARY, longs.size(), 0);
          longs.add((long) bytes.length);
          for (byte item : bytes) {
            longs.add((long) (item & 0xff));
          }
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
    if (call.getOperands().stream()
        .anyMatch(
            operand ->
                containsDecimalUdf(operand)
                    || JsonStringIdentity.containsSensitiveString(operand))) {
      return emitHostExpression(call, true);
    }
    if (call.getOperator()
            instanceof org.apache.flink.table.planner.functions.bridging.BridgingSqlFunction function
        && function.getDefinition() instanceof org.apache.flink.table.functions.ScalarFunction) {
      return emitUdf(call);
    }
    if (switch (call.getKind()) {
      case LESS_THAN, LESS_THAN_OR_EQUAL, GREATER_THAN, GREATER_THAN_OR_EQUAL ->
          call.getOperands().stream()
              .anyMatch(operand -> SqlTypeFamily.CHARACTER.contains(operand.getType()));
      default -> false;
    }) {
      return reject("Flink string ordering depends on its Java or serialized representation");
    }
    if ((call.getKind() == SqlKind.EQUALS || call.getKind() == SqlKind.NOT_EQUALS)
        && hasImplicitStringNumericCast(call)) {
      return reject("Flink rejects implicit VARCHAR/numeric equality during code generation");
    }
    if (isTimestampIdentityCast(call)) {
      return emit(call.getOperands().get(0));
    }
    if (call.getOperator() == org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable.RAND
        || call.getOperator()
            == org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable.RAND_INTEGER) {
      return emitRandom(call);
    }
    long roundingWidth = nativeRoundingWidth(call);
    if (roundingWidth > 0) {
      add(KIND_CALL, 154, 3);
      if (!emit(call.getOperands().get(0))) {
        return false;
      }
      add(KIND_LIT_LONG, longs.size(), 0);
      longs.add(roundingWidth);
      add(KIND_LIT_BOOL, longs.size(), 0);
      longs.add("FLOOR".equals(call.getOperator().getName()) ? 0L : 1L);
      return true;
    }
    String unixTimeFormat = nativeUnixTimeFormat(call);
    if (unixTimeFormat != null) {
      add(KIND_FROM_UNIXTIME, strings.size(), 1);
      strings.add(unixTimeFormat);
      return emit(call.getOperands().get(0));
    }
    if (needsTemporalFunction(call) || needsExactPower(call)) {
      return emitHostExpression(call, false);
    }
    if (call.getKind() == SqlKind.MINUS_PREFIX) {
      return emitFloatUnary(call, 5);
    }
    if (call.getKind() == SqlKind.CAST
        || call.getOperator()
            == org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable.TRY_CAST) {
      return emitCast(call);
    }
    // Reinterpret only re-tags a value's type (e.g. stripping a time-attribute/ROWTIME marker),
    // never
    // changing the value — an identity projection of its first operand.
    if (call.getKind() == SqlKind.REINTERPRET) {
      return emit(call.getOperands().get(0));
    }
    if (call.getKind() == SqlKind.EXTRACT) {
      return emitExtract(call);
    }
    // Transmit Flink's resolved result type to each fused decimal kernel. Generic arithmetic
    // followed by a cast can overflow before rescaling, or infer a different intermediate scale.
    if (isDecimalArithmetic(call)) {
      int precision = call.getType().getPrecision();
      int scale = call.getType().getScale();
      int kind =
          switch (call.getKind()) {
            case PLUS -> KIND_DECIMAL_ADD;
            case MINUS -> KIND_DECIMAL_SUBTRACT;
            case TIMES -> KIND_DECIMAL_MULTIPLY;
            case DIVIDE -> KIND_DECIMAL_DIVIDE;
            case MOD -> KIND_DECIMAL_MOD;
            default -> throw new IllegalStateException("not decimal arithmetic: " + call.getKind());
          };
      add(kind, precision * 100 + scale, 2);
      for (RexNode operand : call.getOperands()) {
        if (!emit(operand)) {
          return false;
        }
      }
      return true;
    }
    String functionName = call.getOperator().getName().toUpperCase(Locale.ROOT);
    int clockField =
        switch (functionName) {
          case "CURRENT_TIMESTAMP", "CURRENT_ROW_TIMESTAMP", "NOW" -> 0;
          case "LOCALTIMESTAMP" -> 1;
          case "CURRENT_DATE" -> 2;
          case "CURRENT_TIME", "LOCALTIME" -> 3;
          case "CURRENT_WATERMARK" -> 5;
          case "UNIX_TIMESTAMP" -> call.getOperands().isEmpty() ? 4 : -1;
          default -> -1;
        };
    if (clockField >= 0) {
      if (clockField == 5 && !watermarkAvailable) {
        return reject("CURRENT_WATERMARK requires a Calc runtime context");
      }
      String zone =
          sessionZoneId == null ? "UTC" : java.time.ZoneId.of(sessionZoneId).normalized().getId();
      if ("Z".equals(zone)) {
        zone = "UTC";
      }
      if (clockField >= 1 && clockField <= 3 && !nativeZoneSupported(zone)) {
        return reject("unsupported clock time zone " + zone);
      }
      add(KIND_CLOCK, strings.size(), 0);
      strings.add(clockField + "|" + zone);
      return true;
    }
    // PROCTIME() / PROCTIME_MATERIALIZE(): a nullary current-processing-time column.
    if ("PROCTIME".equals(functionName) || "PROCTIME_MATERIALIZE".equals(functionName)) {
      add(KIND_PROCTIME, 0, 0);
      return true;
    }
    if ("ITEM".equals(functionName)) {
      return emitItem(call);
    }
    if ("COALESCE".equals(functionName)) {
      if (!RexUtil.isDeterministic(call) || containsScalarUdf(call)) {
        return emitHostExpression(call, true);
      }
      return emitCoalesceAsCase(call.getOperands());
    }
    if (call.getOperator()
        == org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable.IF) {
      if (call.getOperands().size() != 3) return reject("IF requires three operands");
      boolean supportedType = switch (call.getType().getSqlTypeName()) {
        case TINYINT, SMALLINT, INTEGER, BIGINT, FLOAT, REAL, DOUBLE, DECIMAL,
            CHAR, VARCHAR, DATE, TIME, TIMESTAMP, BINARY, VARBINARY -> true;
        default -> false;
      };
      if (!supportedType) return reject("IF requires a verified scalar result type");
      if (!RexUtil.isDeterministic(call)
          || containsScalarUdf(call)
          || call.getOperands().subList(1, 3).stream()
              .anyMatch(
                  operand ->
                      !org.apache.calcite.sql.type.SqlTypeUtil.equalSansNullability(
                          operand.getType(), call.getType()))) {
        // Flink inserts branch casts in codegen and can hoist volatile/UDF evaluation.
        return emitHostExpression(call, true);
      }
      add(KIND_CALL, opCode(SqlKind.CASE), 3);
      for (RexNode operand : call.getOperands()) {
        if (!emit(operand)) return false;
      }
      return true;
    }
    if (call.getOperator()
            instanceof org.apache.flink.table.planner.functions.bridging.BridgingSqlFunction function
        && function.getDefinition()
            == org.apache.flink.table.functions.BuiltInFunctionDefinitions.IF_NULL) {
      return emitBuiltinCall(call, 158);
    }
    if (call.getOperator()
        == org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable.HASH_CODE) {
      if (call.getOperands().size() != 1) return reject("HASH_CODE requires one operand");
      return switch (call.getOperands().get(0).getType().getSqlTypeName()) {
        case BOOLEAN, TINYINT, SMALLINT, INTEGER, BIGINT, FLOAT, REAL, DOUBLE,
            DECIMAL, CHAR, VARCHAR, DATE, TIME, TIMESTAMP -> emitBuiltinCall(call, 162);
        default -> reject("HASH_CODE requires a verified scalar type");
      };
    }
    if ("ENCODE".equals(functionName)) {
      return emitCharsetFunction(call, 120, SqlTypeFamily.CHARACTER);
    }
    if ("DECODE".equals(functionName)) {
      return emitCharsetFunction(call, 121, SqlTypeFamily.BINARY);
    }
    if ("TO_DATE".equals(functionName)) {
      return emitCharacterFunction(call, 131, 1, 1);
    }
    if ("JSON_QUOTE".equals(functionName)) {
      return emitCharacterFunction(call, 122, 1, 1);
    }
    if ("JSON_UNQUOTE".equals(functionName)) {
      return emitCharacterFunction(call, 123, 1, 1);
    }
    if (("JSON_STRING".equals(functionName) || "JSON_OBJECT".equals(functionName))
        && !tech.streamfusion.compat.JsonRuntimeCompat.PRESERVES_DECIMAL_SCALE
        && call.getOperands().stream()
            .anyMatch(operand -> operand.getType().getSqlTypeName() == SqlTypeName.DECIMAL)) {
      return reject(
          "JSON decimal rendering requires the selected Flink line's generated evaluator");
    }
    if ("JSON_STRING".equals(functionName)) {
      if (call.getOperands().size() != 1 || !isJsonScalarValue(call.getOperands().get(0))) {
        return reject("JSON_STRING requires a character, boolean, integer, or decimal scalar");
      }
      return emitBuiltinCall(call, 152);
    }
    if ("JSON_OBJECT".equals(functionName)) {
      return emitJsonObject(call);
    }
    if ("JSON_VALUE".equals(functionName)) {
      return emitJsonValue(call);
    }
    if ("JSON_EXISTS".equals(functionName)) {
      return emitJsonExists(call);
    }
    int jsonPredicate = switch (functionName) {
      case "IS JSON VALUE", "IS NOT JSON VALUE" -> 144;
      case "IS JSON OBJECT", "IS NOT JSON OBJECT" -> 145;
      case "IS JSON ARRAY", "IS NOT JSON ARRAY" -> 146;
      case "IS JSON SCALAR", "IS NOT JSON SCALAR" -> 147;
      default -> -1;
    };
    if (jsonPredicate >= 0) {
      return emitIsJson(call, jsonPredicate, functionName.startsWith("IS NOT"));
    }
    if ("SPLIT".equals(functionName)) {
      List<RexNode> args = call.getOperands();
      if (args.size() != 2
          || !isCharacter(args.get(0))
          || !isCharacter(args.get(1))
          || !(args.get(1) instanceof RexLiteral)) {
        return reject("SPLIT requires a literal non-empty character separator");
      }
      String separator = ((RexLiteral) args.get(1)).getValueAs(String.class);
      if (separator == null || separator.isEmpty()) {
        return reject("SPLIT requires a literal non-empty character separator");
      }
      return emitBuiltinCall(call, 124);
    }
    if ("STARTSWITH".equals(functionName)) {
      return emitCharacterFunction(call, 100, 2, 2);
    }
    if ("ENDSWITH".equals(functionName)) {
      return emitCharacterFunction(call, 101, 2, 2);
    }
    if (call.getOperator()
        == org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable.INSTR) {
      return emitInstr(call);
    }
    if ("LOCATE".equals(functionName)) {
      return emitStringSearch(call, 102, true);
    }
    if ("BIN".equals(functionName)) {
      return emitEncoding(call, 104, true, false);
    }
    if ("HEX".equals(functionName)) {
      return emitEncoding(call, 105, true, true);
    }
    if ("TO_BASE64".equals(functionName)) {
      if (call.getOperands().size() == 1
          && call.getOperands().get(0).getType().getSqlTypeName().getFamily()
              == SqlTypeFamily.BINARY) {
        return emitBuiltinCall(call, 151);
      }
      return emitEncoding(call, 107, false, true);
    }
    if ("UNHEX".equals(functionName)) {
      return emitEncoding(call, 108, false, true);
    }
    if ("GREATEST".equals(functionName)) {
      return emitExtremum(call, 109);
    }
    if ("LEAST".equals(functionName)) {
      return emitExtremum(call, 110);
    }
    if ("INITCAP".equals(functionName)) {
      return emitCharacterFunction(call, 111, 1, 1);
    }
    if ("TRANSLATE".equals(functionName)
        || "TRANSLATE3".equals(functionName)) {
      return emitCharacterFunction(call, 112, 3, 3);
    }
    if ("BTRIM".equals(functionName)) {
      return emitTrimSet(call, 113, 1);
    }
    if ("LTRIM".equals(functionName) && call.getOperands().size() == 2) {
      return emitTrimSet(call, 139, 2);
    }
    if ("RTRIM".equals(functionName) && call.getOperands().size() == 2) {
      return emitTrimSet(call, 140, 2);
    }
    if ("ELT".equals(functionName)) {
      return emitElt(call);
    }
    if ("URL_ENCODE".equals(functionName)) {
      return emitCharacterFunction(call, 115, 1, 1);
    }
    if ("URL_DECODE".equals(functionName)) {
      return emitCharacterFunction(call, Runtime.version().feature() >= 25 ? 118 : 117, 1, 1);
    }
    if ("OVERLAY".equals(functionName)) {
      return emitOverlay(call);
    }
    if ("CONCAT".equals(functionName) || "||".equals(functionName)) {
      return emitStringCall(call, 93, 1, Integer.MAX_VALUE);
    }
    if ("CONCAT_WS".equals(functionName)) {
      return emitStringCall(call, 94, 1, Integer.MAX_VALUE);
    }
    int hashOp = hashOpCode(functionName);
    if (hashOp >= 0) {
      return emitStringCall(call, hashOp, 1, 1);
    }
    if ("SHA2".equals(functionName)) {
      return emitSha2(call);
    }
    if ("TRIM".equals(functionName)) {
      return emitTrim(call);
    }
    if ("SUBSTRING".equals(functionName)
        || "SUBSTR".equals(functionName)) {
      return emitStringWithIntegers(call, 125, 2, 3);
    }
    if ("REPLACE".equals(functionName)) {
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
    if ("POSITION".equals(functionName)) {
      // POSITION(sub IN s) — operands [sub, s]; the native side calls strpos(s, sub).
      List<RexNode> args = call.getOperands();
      if (args.size() != 2) {
        return reject("POSITION requires 2 arguments (no FROM start)");
      }
      add(KIND_CALL, 57, 2);
      return emit(args.get(0)) && emit(args.get(1));
    }
    if ("SPLIT_INDEX".equals(functionName)) {
      return emitSplitIndex(call.getOperands());
    }
    if ("REGEXP_EXTRACT".equals(functionName)) {
      return emitRegexpExtract(call.getOperands());
    }
    if ("DATE_FORMAT".equals(functionName)) {
      return emitDateFormat(call.getOperands());
    }
    if ("ABS".equals(functionName)) {
      return emitFloatUnary(call, 62);
    }
    if ("FLOOR".equals(functionName)) {
      return emitFloatUnary(call, 63);
    }
    if ("CEIL".equals(functionName) || "CEILING".equals(functionName)) {
      return emitFloatUnary(call, 64);
    }
    if ("SIGN".equals(functionName)) {
      return emitFloatUnary(call, 65);
    }
    if ("REPEAT".equals(functionName)) {
      // REPEAT(s, n): repeat s n times — operands [s, n], same order as DataFusion repeat.
      List<RexNode> args = call.getOperands();
      if (args.size() != 2) {
        return reject("REPEAT requires 2 arguments");
      }
      add(KIND_CALL, 66, 2);
      return emit(args.get(0)) && emit(args.get(1));
    }
    if ("LEFT".equals(functionName)) {
      return emitStringWithIntegers(call, 126, 2, 2);
    }
    if ("RIGHT".equals(functionName)) {
      return emitStringWithIntegers(call, 127, 2, 2);
    }
    if ("LPAD".equals(functionName)) {
      return emitPad(call, 128);
    }
    if ("RPAD".equals(functionName)) {
      return emitPad(call, 129);
    }
    // Functions whose native result can differ from the host — locale case folding (UPPER/LOWER)
    // and
    // last-ULP transcendental math. They fall back unless the allowIncompatible flag opts them in.
    Integer incompatUnaryOp =
        INCOMPATIBLE_UNARY.get(functionName);
    if (incompatUnaryOp != null) {
      return emitIncompatibleUnary(call, incompatUnaryOp);
    }
    if ("POWER".equals(functionName)
        || "POW".equals(functionName)) {
      return emitIncompatiblePower(call);
    }
    if (call.getOperator()
            == org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable.ROUND
        || call.getOperator()
            == org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable.TRUNCATE) {
      if (!call.getOperands().isEmpty()
          && call.getOperands().get(0).getType().getSqlTypeName() == SqlTypeName.DECIMAL) {
        return emitDecimalRound(call);
      }
      return call.getOperator()
              == org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable.ROUND
          ? emitIncompatibleRound(call)
          : reject("TRUNCATE requires a DECIMAL operand");
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
        if (operands.stream().anyMatch(this::requiresRowShortCircuit)) {
          return reject("Fallible expressions under AND/OR require Flink's row short-circuiting");
        }
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
        int narrowResult = arithmeticNarrowResult(call);
        if (narrowResult >= 0) {
          add(KIND_CAST_NARROW, narrowResult, 1);
        }
        add(KIND_CALL, op, 2);
        return emit(operands.get(0)) && emit(operands.get(1));
    }
  }

  private static boolean hasImplicitStringNumericCast(RexCall comparison) {
    boolean character = false;
    boolean numeric = false;
    for (RexNode operand : comparison.getOperands()) {
      character |= operand.getType().getSqlTypeName().getFamily() == SqlTypeFamily.CHARACTER;
      numeric |= operand.getType().getSqlTypeName().getFamily() == SqlTypeFamily.NUMERIC;
      if (operand instanceof RexCall && operand.getKind() == SqlKind.CAST) {
        RexNode source = ((RexCall) operand).getOperands().get(0);
        boolean sourceString =
            source.getType().getSqlTypeName().getFamily() == SqlTypeFamily.CHARACTER;
        boolean targetNumeric =
            operand.getType().getSqlTypeName().getFamily() == SqlTypeFamily.NUMERIC;
        if (sourceString && targetNumeric) {
          return true;
        }
      }
    }
    return character && numeric;
  }

  private boolean emitInstr(RexCall call) {
    List<RexNode> args = call.getOperands();
    if (args.size() == 2) {
      return emitStringSearch(call, 102, false);
    }
    if (args.size() < 3
        || args.size() > 4
        || !isCharacter(args.get(0))
        || !isCharacter(args.get(1))
        || args.subList(2, args.size()).stream().anyMatch(arg -> !isInt32(arg))) {
      return reject("INSTR requires two character strings and optional INT start/occurrence");
    }
    return emitBuiltinCall(call, 161);
  }

  private boolean emitStringSearch(RexCall call, int op, boolean locate) {
    List<RexNode> args = call.getOperands();
    String name = call.getOperator().getName();
    if (args.size() != 2 && !(locate && args.size() == 3)) {
      return reject(name + (locate ? " requires 2 or 3 arguments" : " requires 2 arguments"));
    }
    for (int i = 0; i < 2; i++) {
      SqlTypeName type = args.get(i).getType().getSqlTypeName();
      if (type.getFamily() != SqlTypeFamily.CHARACTER && type != SqlTypeName.NULL) {
        return reject(name + ": only character strings admitted");
      }
    }
    if (args.size() == 3) {
      SqlTypeName startType = args.get(2).getType().getSqlTypeName();
      if (startType != SqlTypeName.TINYINT
          && startType != SqlTypeName.SMALLINT
          && startType != SqlTypeName.INTEGER
          && startType != SqlTypeName.NULL) {
        return reject("LOCATE start must be TINYINT, SMALLINT, or INTEGER");
      }
      op = 103;
    }
    add(KIND_CALL, op, args.size());
    // LOCATE takes (needle, string[, start]); the native functions take the string first.
    return emit(args.get(locate ? 1 : 0))
        && emit(args.get(locate ? 0 : 1))
        && (args.size() == 2 || emit(args.get(2)));
  }

  private boolean emitJsonObject(RexCall call) {
    List<RexNode> args = call.getOperands();
    if (args.isEmpty() || args.size() % 2 != 1 || !(args.get(0) instanceof RexLiteral)) {
      return reject("JSON_OBJECT requires a NULL policy and key/value pairs");
    }
    Object policyValue = ((RexLiteral) args.get(0)).getValue();
    if (!(policyValue instanceof SqlJsonConstructorNullClause nullClause)) {
      return reject("JSON_OBJECT: unsupported NULL policy");
    }
    String policy =
        switch (nullClause) {
          case NULL_ON_NULL -> "NULL";
          case ABSENT_ON_NULL -> "ABSENT";
        };
    for (int index = 1; index < args.size(); index += 2) {
      if (!(args.get(index) instanceof RexLiteral key) || !isCharacter(key) || key.isNull()) {
        return reject("JSON_OBJECT requires non-null literal character keys");
      }
      String name = key.getValueAs(String.class);
      if (name == null || name.codePoints().anyMatch(c -> c >= 0xd800 && c <= 0xdfff)) {
        return reject("JSON_OBJECT requires keys with well-formed Unicode");
      }
      if (!isJsonScalarValue(args.get(index + 1))) {
        return reject("JSON_OBJECT requires character, boolean, integer, or decimal scalar values");
      }
    }
    add(KIND_CALL, 153, args.size());
    add(KIND_LIT_STRING, strings.size(), 0);
    strings.add(policy);
    for (RexNode arg : args.subList(1, args.size())) {
      if (!emit(arg)) {
        return false;
      }
    }
    return true;
  }

  private static boolean isJsonScalarValue(RexNode value) {
    // Flink treats direct JSON constructors as raw JSON, despite their character return type.
    if (value instanceof RexCall call
        && List.of("JSON_OBJECT", "JSON_ARRAY", "JSON")
            .contains(call.getOperator().getName().toUpperCase(Locale.ROOT))) {
      return false;
    }
    return switch (value.getType().getSqlTypeName()) {
      case CHAR, VARCHAR, BOOLEAN, TINYINT, SMALLINT, INTEGER, BIGINT, DECIMAL -> true;
      default -> false;
    };
  }

  private boolean emitCharsetFunction(RexCall call, int op, SqlTypeFamily inputFamily) {
    List<RexNode> args = call.getOperands();
    if (args.size() != 2
        || args.get(0).getType().getSqlTypeName().getFamily() != inputFamily
        || !(args.get(1) instanceof RexLiteral)) {
      return reject(call.getOperator().getName() + " requires a literal charset");
    }
    String name = ((RexLiteral) args.get(1)).getValueAs(String.class);
    if (name == null) {
      return reject(call.getOperator().getName() + ": NULL charset");
    }
    String charset;
    try {
      charset = java.nio.charset.Charset.forName(name).name();
    } catch (IllegalArgumentException e) {
      return reject(call.getOperator().getName() + ": unknown charset");
    }
    if (!List.of("UTF-8", "US-ASCII", "ISO-8859-1", "UTF-16", "UTF-16BE", "UTF-16LE")
        .contains(charset)) {
      return reject(call.getOperator().getName() + ": unverified charset " + charset);
    }
    add(KIND_CALL, op, 2);
    if (!emit(args.get(0))) {
      return false;
    }
    add(KIND_LIT_STRING, strings.size(), 0);
    strings.add(charset);
    return true;
  }

  private boolean emitExtremum(RexCall call, int op) {
    List<RexNode> args = call.getOperands();
    if (args.size() < 2) {
      return reject(call.getOperator().getName() + " requires at least two arguments");
    }
    RelDataType result = call.getType();
    SqlTypeName type = result.getSqlTypeName();
    boolean integer = SqlTypeFamily.INTEGER.getTypeNames().contains(type);
    boolean character = type.getFamily() == SqlTypeFamily.CHARACTER;
    if (character && args.stream().anyMatch(arg -> !isAsciiLiteralResult(arg))) {
      return reject(call.getOperator().getName() + " requires ASCII-provable string operands");
    }
    if (!integer && !character && type != SqlTypeName.BOOLEAN && type != SqlTypeName.DECIMAL) {
      return reject(
          call.getOperator().getName()
              + " supports integers, strings, boolean, and matching decimals");
    }
    for (RexNode arg : args) {
      RelDataType input = arg.getType();
      SqlTypeName inputType = input.getSqlTypeName();
      boolean admitted =
          integer
              ? SqlTypeFamily.INTEGER.getTypeNames().contains(inputType)
              : character
                  ? inputType.getFamily() == SqlTypeFamily.CHARACTER
                  : inputType == type
                      && (type != SqlTypeName.DECIMAL
                          || (input.getPrecision() == result.getPrecision()
                              && input.getScale() == result.getScale()));
      if (!admitted) {
        return reject(call.getOperator().getName() + " operand types require unverified coercion");
      }
    }
    return emitBuiltinCall(call, op);
  }

  private static boolean isAsciiLiteralResult(RexNode node) {
    if (node instanceof RexLiteral literal) {
      String value = literal.getValueAs(String.class);
      return value == null || value.chars().allMatch(ch -> ch < 128);
    }
    if (node instanceof RexCall call && call.getKind() == SqlKind.CASE) {
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

  private boolean emitTrimSet(RexCall call, int op, int min) {
    if (call.getOperands().size() == 2 && !(call.getOperands().get(1) instanceof RexLiteral)) {
      return reject(call.getOperator().getName() + " requires a literal trim set");
    }
    return emitCharacterFunction(call, op, min, 2);
  }

  private boolean emitCharacterFunction(RexCall call, int op, int min, int max) {
    if (call.getOperands().size() < min
        || call.getOperands().size() > max
        || call.getOperands().stream().anyMatch(arg -> !isCharacter(arg))) {
      return reject(
          call.getOperator().getName() + " requires " + min + ".." + max + " character arguments");
    }
    return emitBuiltinCall(call, op);
  }

  private boolean emitRandom(RexCall call) {
    boolean integer = call.getOperator()
        == org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable.RAND_INTEGER;
    List<RexNode> args = call.getOperands();
    int required = integer ? 1 : 0;
    if (args.size() < required || args.size() > required + 1
        || args.stream().anyMatch(arg -> arg.getType().getSqlTypeName() != SqlTypeName.INTEGER)) {
      return reject("RAND/RAND_INTEGER require Flink's INT seed and bound overloads");
    }
    boolean seeded = args.size() > required;
    int flags = (integer ? 1 : 0) | (seeded ? 2 : 0)
        | (seeded && args.get(0) instanceof RexLiteral ? 4 : 0);
    add(KIND_RANDOM, flags, args.size());
    for (RexNode arg : args) {
      if (!emit(arg)) {
        return false;
      }
    }
    return true;
  }

  private boolean requiresRowShortCircuit(RexNode node) {
    if (!(node instanceof RexCall call)) {
      return false;
    }
    if (call.getOperator()
            == org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable.RAND_INTEGER
        && !isIntLiteralAtLeast(call.getOperands().get(call.getOperands().size() - 1), 1)) {
      return true;
    }
    if (call.getType().getSqlTypeName().getFamily() == SqlTypeFamily.NUMERIC
        && call.getType().getSqlTypeName() != SqlTypeName.FLOAT
        && call.getType().getSqlTypeName() != SqlTypeName.REAL
        && call.getType().getSqlTypeName() != SqlTypeName.DOUBLE
        && (call.getKind() == SqlKind.MOD || call.getKind() == SqlKind.DIVIDE)
        && !hasNonzeroIntegerDivisor(call)) {
      return true;
    }
    String name = call.getOperator().getName().toUpperCase(Locale.ROOT);
    List<RexNode> args = call.getOperands();
    if (call.getKind() == SqlKind.CAST
        && (call.getType().getSqlTypeName() == SqlTypeName.BOOLEAN
            || SqlTypeFamily.INTEGER.getTypeNames().contains(call.getType().getSqlTypeName()))
        && args.size() == 1
        && isCharacter(args.get(0))
        && !Boolean.TRUE.equals(legacyCastBehaviour)) {
      return true;
    }
    if (call.getOperator()
            == org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable.INSTR
        && args.size() >= 3
        && (!isIntLiteralAtLeast(args.get(2), Integer.MIN_VALUE + 1)
            || args.size() == 4 && !isIntLiteralAtLeast(args.get(3), 1))) {
      return true;
    }
    if ((call.getOperator()
                == org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable.ROUND
            || call.getOperator()
                == org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable.TRUNCATE)
        && args.size() == 2
        && args.get(0).getType().getSqlTypeName() == SqlTypeName.DECIMAL
        && args.get(1) instanceof RexLiteral round
        && !round.isNull()
        && !isIntLiteralAtLeast(round, -38)) {
      return true;
    }
    if ("JSON_VALUE".equals(name)) {
      if (call.getType().getSqlTypeName() != SqlTypeName.VARCHAR) {
        return true;
      }
      for (int i = 2; i < args.size(); ) {
        String policy = jsonSymbol(args.get(i));
        if ("ERROR".equals(policy)) {
          return true;
        }
        i += "DEFAULT".equals(policy) ? 3 : 2;
      }
    }
    if ("JSON_EXISTS".equals(name)
        && args.size() == 3
        && "ERROR".equals(jsonSymbol(args.get(2)))) {
      return true;
    }
    return args.stream().anyMatch(this::requiresRowShortCircuit);
  }

  private static boolean hasNonzeroIntegerDivisor(RexCall call) {
    return SqlTypeFamily.INTEGER.getTypeNames().contains(call.getType().getSqlTypeName())
        && call.getOperands().size() == 2
        && call.getOperands().stream().allMatch(
            arg -> SqlTypeFamily.INTEGER.getTypeNames().contains(arg.getType().getSqlTypeName()))
        && call.getOperands().get(1) instanceof RexLiteral divisor
        && !divisor.isNull()
        && divisor.getValueAs(BigDecimal.class).signum() != 0;
  }

  private boolean jsonRuntimeAvailable() {
    try {
      if (tech.streamfusion.operator.NativeJsonRuntime.available()) {
        return true;
      }
    } catch (LinkageError incompatibleJackson) {
      // Loading the runtime class itself may fail before available() can run.
    }
    return reject("SQL/JSON requires Jackson 2.18.2 with a shared thread-local token buffer");
  }

  private boolean emitJsonValue(RexCall call) {
    if (!jsonRuntimeAvailable()) {
      return false;
    }
    if (JsonPathSpec.unicodeVersion() == null) {
      return reject("JSON_VALUE requires verified JDK 17, 21, 24 or 25 token rules");
    }
    List<RexNode> args = call.getOperands();
    SqlTypeName returnType = call.getType().getSqlTypeName();
    int op =
        switch (returnType) {
          case VARCHAR -> 141;
          case BOOLEAN -> 148;
          case INTEGER -> 149;
          case DOUBLE -> 150;
          default -> -1;
        };
    if (op < 0) {
      return reject("JSON_VALUE supports RETURNING VARCHAR, BOOLEAN, INTEGER or DOUBLE");
    }
    String path = jsonPath(args);
    if (path == null) {
      return reject("JSON_VALUE requires a literal definite member/index path");
    }
    String empty = "NULL";
    String error = "NULL";
    String emptyDefault = null;
    String errorDefault = null;
    for (int i = 2; i < args.size(); ) {
      String behavior = jsonSymbol(args.get(i++));
      String defaultValue = null;
      if ("DEFAULT".equals(behavior)) {
        if (i >= args.size()
            || !(args.get(i++) instanceof RexLiteral literal)
            || (defaultValue = jsonDefault(literal, returnType)) == null) {
          return reject(
              "JSON_VALUE DEFAULT requires a non-null literal matching RETURNING; DOUBLE defaults"
                  + " are not admitted");
        }
      } else if (!"NULL".equals(behavior) && !"ERROR".equals(behavior)) {
        return reject("JSON_VALUE has an unsupported behavior");
      }
      if (i >= args.size()) {
        return reject("JSON_VALUE requires ON EMPTY or ON ERROR after a behavior");
      }
      String mode = jsonSymbol(args.get(i++));
      if ("EMPTY".equals(mode)) {
        empty = behavior;
        emptyDefault = defaultValue;
      } else if ("ERROR".equals(mode)) {
        error = behavior;
        errorDefault = defaultValue;
      } else {
        return reject("JSON_VALUE has an unsupported behavior target");
      }
    }
    if ("ERROR".equals(empty) || "ERROR".equals(error)) {
      return reject("JSON_VALUE ERROR policies require Flink's generated exception handling");
    }
    if (returnType == SqlTypeName.BOOLEAN
        && ("NULL".equals(empty) || "NULL".equals(error))
        && call != projectionRoot) {
      return reject(
          "JSON_VALUE BOOLEAN with NULL policies requires a direct projection; Flink unboxes null"
              + " in boolean contexts");
    }
    add(KIND_CALL, op, 7);
    if (!emit(args.get(0))) {
      return false;
    }
    for (String value : new String[] {path, empty, emptyDefault, error, errorDefault}) {
      emitString(value);
    }
    emitString(JsonPathSpec.unicodeVersion());
    return true;
  }

  private static String jsonDefault(RexLiteral literal, SqlTypeName returnType) {
    if (literal.isNull()) {
      return null;
    }
    SqlTypeName literalType = literal.getType().getSqlTypeName();
    if (returnType == SqlTypeName.VARCHAR && isCharacter(literal)) {
      return literal.getValueAs(String.class);
    }
    if (returnType == SqlTypeName.BOOLEAN && literalType == SqlTypeName.BOOLEAN) {
      return literal.getValueAs(Boolean.class).toString();
    }
    if (returnType == SqlTypeName.INTEGER && literalType == SqlTypeName.INTEGER) {
      return Integer.toString(literal.getValueAs(BigDecimal.class).intValueExact());
    }
    // DOUBLE defaults are generated as Double/DecimalData, not the BigDecimal object
    // that Flink's RETURNING conversion expects. Preserve that behavior on Flink.
    return null;
  }

  private boolean emitIsJson(RexCall call, int op, boolean negate) {
    if (!jsonRuntimeAvailable()) {
      return false;
    }
    if (JsonPathSpec.unicodeVersion() == null) {
      return reject("IS JSON requires verified JDK 17, 21, 24 or 25 token rules");
    }
    if (call.getOperands().size() != 1 || !isCharacter(call.getOperands().get(0))) {
      return reject("IS JSON requires one character argument");
    }
    if (negate) {
      add(KIND_CALL, opCode(SqlKind.NOT), 1);
    }
    add(KIND_CALL, op, 2);
    if (!emit(call.getOperands().get(0))) {
      return false;
    }
    emitString(JsonPathSpec.unicodeVersion());
    return true;
  }

  private boolean emitJsonExists(RexCall call) {
    if (!jsonRuntimeAvailable()) {
      return false;
    }
    if (JsonPathSpec.unicodeVersion() == null) {
      return reject("JSON_EXISTS requires verified JDK 17, 21, 24 or 25 token rules");
    }
    List<RexNode> args = call.getOperands();
    String path = jsonPath(args);
    if (path == null || args.size() > 3) {
      return reject("JSON_EXISTS requires a literal definite member/index path");
    }
    String error = args.size() == 2 ? "FALSE" : jsonSymbol(args.get(2));
    if (error == null || !List.of("TRUE", "FALSE", "UNKNOWN", "ERROR").contains(error)) {
      return reject("JSON_EXISTS has an unsupported ON ERROR behavior");
    }
    if ("UNKNOWN".equals(error) && call != projectionRoot) {
      return reject(
          "JSON_EXISTS UNKNOWN ON ERROR requires a direct projection; Flink unboxes null in boolean"
              + " contexts");
    }
    if ("ERROR".equals(error)) {
      return emitJsonExistsError(args);
    }
    add(KIND_CALL, 142, 4);
    if (!emit(args.get(0))) {
      return false;
    }
    emitString(path);
    emitString(error);
    emitString(JsonPathSpec.unicodeVersion());
    return true;
  }

  private boolean emitJsonExistsError(List<RexNode> args) {
    Method implementation;
    try {
      implementation =
          tech.streamfusion.operator.NativeBuiltinFunctions.class.getMethod(
              "jsonExistsError", String.class, String.class);
    } catch (ReflectiveOperationException unavailable) {
      return reject("JSON_EXISTS host error handling unavailable: " + unavailable.getMessage());
    }
    int string = tech.streamfusion.operator.NativeUdf.TYPE_STRING;
    int result = tech.streamfusion.operator.NativeUdf.TYPE_BOOLEAN;
    int localIndex =
        addUdf(
            tech.streamfusion.operator.NativeUdf.Descriptor.forBuiltin(
                implementation, new int[] {string, string}, result));
    add(KIND_UDF, longs.size(), 2);
    longs.add((long) localIndex);
    longs.add((long) result);
    return emit(args.get(0)) && emit(args.get(1));
  }

  private static String jsonPath(List<RexNode> args) {
    if (args.size() < 2
        || !isCharacter(args.get(0))
        || !(args.get(1) instanceof RexLiteral literal)
        || !isCharacter(literal)
        || literal.isNull()) {
      return null;
    }
    return JsonPathSpec.normalize(literal.getValueAs(String.class));
  }

  private static String jsonSymbol(RexNode node) {
    return node instanceof RexLiteral literal
            && literal.getType().getSqlTypeName() == SqlTypeName.SYMBOL
            && literal.getValue() instanceof Enum<?> symbol
        ? symbol.name()
        : null;
  }

  private void emitString(String value) {
    add(KIND_LIT_STRING, strings.size(), 0);
    strings.add(value);
  }

  private static boolean isCharacter(RexNode arg) {
    return arg.getType().getSqlTypeName().getFamily() == SqlTypeFamily.CHARACTER;
  }

  private boolean emitBuiltinCall(RexCall call, int op) {
    add(KIND_CALL, op, call.getOperands().size());
    for (RexNode arg : call.getOperands()) {
      if (!emit(arg)) {
        return false;
      }
    }
    return true;
  }

  private boolean emitElt(RexCall call) {
    List<RexNode> args = call.getOperands();
    if (args.size() < 2
        || args.get(0).getType().getSqlTypeName() != SqlTypeName.INTEGER
        || args.subList(1, args.size()).stream().anyMatch(arg -> !isCharacter(arg))) {
      // Flink 2.2.1 casts its Number index to java.lang.Integer after checking the bounds.
      return reject("ELT requires an INTEGER index and character values");
    }
    return emitBuiltinCall(call, 114);
  }

  private boolean emitOverlay(RexCall call) {
    List<RexNode> args = call.getOperands();
    if ((args.size() != 3 && args.size() != 4)
        || !isCharacter(args.get(0))
        || !isCharacter(args.get(1))
        || args.subList(2, args.size()).stream()
            .anyMatch(
                arg ->
                    !SqlTypeFamily.INTEGER
                        .getTypeNames()
                        .contains(arg.getType().getSqlTypeName()))) {
      return reject("OVERLAY requires two character strings and integer positions");
    }
    add(KIND_CALL, 116, args.size());
    for (int i = 0; i < args.size(); i++) {
      if (i >= 2) {
        add(KIND_CAST, CAST_BIGINT, 1);
      }
      if (!emit(args.get(i))) {
        return false;
      }
    }
    return true;
  }

  private boolean emitEncoding(RexCall call, int op, boolean integers, boolean strings) {
    String name = call.getOperator().getName();
    if (call.getOperands().size() != 1) {
      return reject(name + " requires 1 argument");
    }
    RexNode arg = call.getOperands().get(0);
    SqlTypeName type = arg.getType().getSqlTypeName();
    boolean integer =
        type == SqlTypeName.TINYINT
            || type == SqlTypeName.SMALLINT
            || type == SqlTypeName.INTEGER
            || type == SqlTypeName.BIGINT;
    boolean string = type.getFamily() == SqlTypeFamily.CHARACTER;
    if (!(integers && integer) && !(strings && string) && type != SqlTypeName.NULL) {
      return reject(name + ": unsupported input type " + type);
    }
    if (op == 105 && !integer) {
      op = 106;
    }
    add(KIND_CALL, op, 1);
    return emit(arg);
  }

  private boolean emitStringCall(RexCall call, int op, int minArgs, int maxArgs) {
    List<RexNode> args = call.getOperands();
    if (args.size() < minArgs || args.size() > maxArgs) {
      return reject("unsupported arity for " + call.getOperator().getName());
    }
    for (RexNode arg : args) {
      if (arg.getType().getSqlTypeName().getFamily() != SqlTypeFamily.CHARACTER
          && arg.getType().getSqlTypeName() != SqlTypeName.NULL) {
        return reject(call.getOperator().getName() + ": only character strings admitted");
      }
    }
    add(KIND_CALL, op, args.size());
    for (RexNode arg : args) {
      if (!emit(arg)) {
        return false;
      }
    }
    return true;
  }

  private static int hashOpCode(String name) {
    switch (name) {
      case "SHA1":
        return 143;
      case "MD5":
        return 95;
      case "SHA224":
        return 96;
      case "SHA256":
        return 97;
      case "SHA384":
        return 98;
      case "SHA512":
        return 99;
      default:
        return -1;
    }
  }

  private boolean emitSha2(RexCall call) {
    List<RexNode> args = call.getOperands();
    if (args.size() != 2) {
      return reject("SHA2: only the two-argument UTF-8 form is admitted");
    }
    RexNode bitLength = args.get(1);
    if (!(bitLength instanceof RexLiteral) || ((RexLiteral) bitLength).isNull()) {
      return reject("SHA2 requires a literal bit length of 224, 256, 384, or 512");
    }
    int bits;
    try {
      bits = ((RexLiteral) bitLength).getValueAs(BigDecimal.class).intValueExact();
    } catch (ArithmeticException e) {
      return reject("SHA2 requires a literal bit length of 224, 256, 384, or 512");
    }
    int op = hashOpCode("SHA" + bits);
    if (op < 0) {
      return reject("SHA2 requires a literal bit length of 224, 256, 384, or 512");
    }
    if (args.get(0).getType().getSqlTypeName().getFamily() != SqlTypeFamily.CHARACTER
        && args.get(0).getType().getSqlTypeName() != SqlTypeName.NULL) {
      return reject("SHA2: only character strings admitted");
    }
    add(KIND_CALL, op, 1);
    return emit(args.get(0));
  }

  private static int arithmeticNarrowResult(RexCall call) {
    switch (call.getKind()) {
      case PLUS:
      case MINUS:
      case TIMES:
      case DIVIDE:
      case MOD:
        break;
      default:
        return -1;
    }
    switch (call.getType().getSqlTypeName()) {
      case TINYINT:
        return CAST_TINYINT;
      case SMALLINT:
        return CAST_SMALLINT;
      case INTEGER:
        return CAST_INTEGER;
      default:
        return -1;
    }
  }

  /**
   * Lowers {@code COALESCE(a, b, …, z)} to the searched CASE the host defines it as — {@code CASE
   * WHEN a IS NOT NULL THEN a WHEN b IS NOT NULL THEN b … ELSE z} — so it rides the admitted CASE
   * path with identical (first-non-null) semantics. Calcite does not pre-expand COALESCE here, so
   * we expand it ourselves rather than admit a separate op.
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

  /** Emits only native or host-exact casts that preserve Flink's configured cast semantics. */
  private boolean emitCast(RexCall call) {
    if (call.getOperands().size() != 1) {
      return reject("unsupported CAST arity");
    }
    RelDataType sourceType = call.getOperands().get(0).getType();
    RelDataType resultType = call.getType();
    boolean tryCast =
        call.getOperator()
            == org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable.TRY_CAST;
    SqlTypeName source = sourceType.getSqlTypeName();
    SqlTypeName targetType = resultType.getSqlTypeName();
    int sourceInteger = numericRank(source);
    int targetInteger = numericRank(targetType);
    if ((source == SqlTypeName.VARCHAR || source == SqlTypeName.CHAR)
        && targetInteger >= 0
        && targetInteger <= 3) {
      if (!tryCast && legacyCastBehaviour == null) {
        return reject("STRING to integer requires the configured cast behavior");
      }
      add(
          KIND_STRING_TO_INTEGER,
          targetInteger | (tryCast || Boolean.TRUE.equals(legacyCastBehaviour) ? 4 : 0),
          1);
      return emit(call.getOperands().get(0));
    }
    if (sourceInteger >= 0
        && sourceInteger <= 3
        && (targetType == SqlTypeName.VARCHAR || targetType == SqlTypeName.CHAR)) {
      if (legacyCastBehaviour == null) {
        return reject("Integer to STRING requires the configured cast behavior");
      }
      int length = legacyCastBehaviour ? Integer.MAX_VALUE : resultType.getPrecision();
      boolean pad = !legacyCastBehaviour && targetType == SqlTypeName.CHAR;
      add(KIND_INTEGER_TO_STRING, pad ? -length : length, 1);
      return emit(call.getOperands().get(0));
    }
    if (tryCast
        && targetType == SqlTypeName.DECIMAL
        && (source == SqlTypeName.VARCHAR || source == SqlTypeName.CHAR)) {
      return emitHostExpression(call, true);
    }
    if (tryCast && !(source == SqlTypeName.DECIMAL && targetType == SqlTypeName.DECIMAL)) {
      return reject("unsupported TRY_CAST " + source + "→" + targetType);
    }
    // A cast that leaves the value unchanged — same base type and precision/scale, differing only
    // in
    // nullability or a time-attribute marker (Flink's `CAST(... ):TIMESTAMP_LTZ *ROWTIME*` that
    // marks
    // the event-time column) — is an identity projection: emit the operand so the column passes
    // through.
    if (org.apache.calcite.sql.type.SqlTypeUtil.equalSansNullability(sourceType, resultType)) {
      return emit(call.getOperands().get(0));
    }
    RelDataType decimalSource =
        source == SqlTypeName.ARRAY ? sourceType.getComponentType() : sourceType;
    RelDataType floatingTarget =
        targetType == SqlTypeName.ARRAY ? resultType.getComponentType() : resultType;
    if ((source == SqlTypeName.ARRAY) == (targetType == SqlTypeName.ARRAY)
        && decimalSource.getSqlTypeName() == SqlTypeName.DECIMAL
        && (floatingTarget.getSqlTypeName() == SqlTypeName.FLOAT
            || floatingTarget.getSqlTypeName() == SqlTypeName.REAL
            || floatingTarget.getSqlTypeName() == SqlTypeName.DOUBLE)) {
      add(
          KIND_DECIMAL_FLOAT,
          floatingTarget.getSqlTypeName() == SqlTypeName.DOUBLE ? CAST_DOUBLE : CAST_FLOAT,
          1);
      return emit(call.getOperands().get(0));
    }
    if ((source == SqlTypeName.VARCHAR || source == SqlTypeName.CHAR)
        && targetType == SqlTypeName.BOOLEAN) {
      if (legacyCastBehaviour == null) {
        return reject("STRING to BOOLEAN requires the configured cast behavior");
      }
      return emitBuiltinCall(call, legacyCastBehaviour ? 160 : 159);
    }
    // A non-narrowing cast to VARCHAR from a CHAR or VARCHAR source (target length ≥ source). Flink
    // stores both as unpadded StringData and neither pads nor truncates a widening string cast, so
    // the
    // value is unchanged — emit the operand as a passthrough. This covers a bounded computed string
    // coerced to an unbounded STRING sink column (Nexmark q14/q21's CASE result) and a CHAR literal
    // unified up to VARCHAR (the ELSE branch of COALESCE(s, 'x') / CASE). Narrowing (target <
    // source)
    // truncates and is not admitted; casting *to* CHAR(n) pads, so it is not admitted either.
    if ((source == SqlTypeName.VARCHAR || source == SqlTypeName.CHAR)
        && targetType == SqlTypeName.VARCHAR
        && resultType.getPrecision() >= sourceType.getPrecision()) {
      return emit(call.getOperands().get(0));
    }
    // Exact-source casts round HALF_UP and return NULL on overflow. Float/double and string
    // sources use the host-exact cast upcall below.
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
    // integer type). Java keeps the low bits for integer sources. Floating sources saturate to
    // INT/BIGINT with NaN→0, then discard high bits for byte/short targets. The native kernel
    // uses staged Rust casts for the same conversion; see divergences/07.
    int narrowTarget = narrowingIntTargetCode(source, targetType);
    if (narrowTarget >= 0) {
      add(KIND_CAST_NARROW, narrowTarget, 1);
      return emit(call.getOperands().get(0));
    }
    // The casts whose formatting/parsing the native engine cannot reproduce byte-for-byte — a
    // float/double/decimal to/from a string, narrowing a string / padding to CHAR(n), and the inexact
    // float/double→DECIMAL — run Flink's own CastExecutor through the columnar JVM upcall, so
    // trailing zeros, scientific-notation thresholds, trim semantics, and failure behavior are the
    // host's own (see HostCastFunction).
    if (hostCastSupported(sourceType, resultType)) {
      return emitHostCast(call, sourceType, resultType);
    }
    return reject("unsupported CAST " + source + "→" + targetType);
  }

  /**
   * The number↔string / string-length / float→decimal casts routed through the host's cast rules.
   */
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

  /**
   * The UDF marshalling code for a host-cast operand/result, or -1 for a type the upcall can't
   * carry.
   */
  private static int hostCastTypeCode(RelDataType type) {
    int temporal = temporalTypeCode(type);
    if (temporal >= 0) {
      return temporal;
    }
    if (type.getSqlTypeName() == SqlTypeName.DECIMAL) {
      return tech.streamfusion.operator.NativeUdf.decimalType(type.getPrecision(), type.getScale());
    }
    return udfTypeCode(type.getSqlTypeName());
  }

  private static int temporalTypeCode(RelDataType type) {
    SqlTypeName name = type.getSqlTypeName();
    if (name.getFamily() == SqlTypeFamily.INTERVAL_YEAR_MONTH) {
      return 12;
    }
    if (name.getFamily() == SqlTypeFamily.INTERVAL_DAY_TIME) {
      return 13;
    }
    return switch (name) {
      case DATE -> 9;
      case TIME -> 10;
      case TIMESTAMP, TIMESTAMP_WITH_LOCAL_TIME_ZONE -> 11;
      default -> -1;
    };
  }

  private String nativeUnixTimeFormat(RexCall call) {
    if (!"FROM_UNIXTIME".equalsIgnoreCase(call.getOperator().getName())
        || call.getOperands().isEmpty()
        || call.getOperands().size() > 2) return null;
    SqlTypeName input = call.getOperands().get(0).getType().getSqlTypeName();
    if (input != SqlTypeName.BIGINT
        && input != SqlTypeName.INTEGER
        && input != SqlTypeName.SMALLINT
        && input != SqlTypeName.TINYINT) return null;
    String pattern = "yyyy-MM-dd HH:mm:ss";
    if (call.getOperands().size() == 2) {
      if (!(call.getOperands().get(1) instanceof RexLiteral literal)) return null;
      pattern = literal.getValueAs(String.class);
    }
    return NativeUnixTimeFormat.encode(pattern, sessionZoneId);
  }

  private static boolean needsTemporalFunction(RexCall call) {
    String name = call.getOperator().getName().toUpperCase(Locale.ROOT);
    if (java.util.Set.of(
            "TO_TIMESTAMP",
            "TO_TIMESTAMP_LTZ",
            "TIMESTAMPADD",
            "TIMESTAMPDIFF",
            "CONVERT_TZ",
            "FROM_UNIXTIME")
        .contains(name)) {
      return true;
    }
    if ("TO_DATE".equals(name)) {
      return call.getOperands().size() == 2;
    }
    if ("UNIX_TIMESTAMP".equals(name)) {
      return !call.getOperands().isEmpty();
    }
    if ("DATE_FORMAT".equals(name)) {
      if (NativeConfig.allowsIncompatible("DATE_FORMAT")
          && call.getOperands().size() == 2
          && call.getOperands().get(1) instanceof RexLiteral format
          && call.getOperands().get(0).getType().getSqlTypeName()
              == SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE
          && toChronoFormat(format.getValueAs(String.class)) != null) {
        return false;
      }
      return call.getOperands().size() != 2
          || !(call.getOperands().get(1) instanceof RexLiteral format)
          || call.getOperands().get(0).getType().getSqlTypeName() != SqlTypeName.TIMESTAMP
          || call.getOperands().get(0) instanceof RexCall child && needsTemporalFunction(child)
          || toChronoFormat(format.getValueAs(String.class)) == null;
    }
    if (call.getKind() == SqlKind.EXTRACT) {
      String unit = String.valueOf(((RexLiteral) call.getOperands().get(0)).getValue());
      SqlTypeName source = call.getOperands().get(1).getType().getSqlTypeName();
      if (NativeConfig.allowsIncompatible("EXTRACT")
          && source == SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE
          && extractField(unit) != null) {
        return false;
      }
      if (call.getOperands().get(1) instanceof RexCall child && needsTemporalFunction(child)) {
        return true;
      }
      return !(java.util.Set.of("QUARTER", "WEEK", "DOY", "DOW").contains(unit)
          && (source == SqlTypeName.DATE || source == SqlTypeName.TIMESTAMP));
    }
    if (("FLOOR".equals(name) || "CEIL".equals(name) || "CEILING".equals(name))
        && call.getOperands().size() == 2) {
      return true;
    }
    boolean temporalOperand =
        call.getOperands().stream().anyMatch(operand -> temporalTypeCode(operand.getType()) >= 0);
    return switch (call.getKind()) {
      case CAST -> temporalOperand || temporalTypeCode(call.getType()) >= 0;
      case EQUALS,
              NOT_EQUALS,
              LESS_THAN,
              LESS_THAN_OR_EQUAL,
              GREATER_THAN,
              GREATER_THAN_OR_EQUAL,
              IS_DISTINCT_FROM,
              IS_NOT_DISTINCT_FROM ->
          temporalOperand;
      case PLUS, MINUS, TIMES, DIVIDE, MOD, MINUS_PREFIX -> temporalOperand;
      default -> "DATETIME_PLUS".equals(name) || "-".equals(name) && temporalOperand;
    };
  }

  private static long nativeRoundingWidth(RexCall call) {
    String name = call.getOperator().getName();
    if (!("FLOOR".equals(name) || "CEIL".equals(name) || "CEILING".equals(name))
        || call.getOperands().size() != 2
        || call.getOperands().get(0).getType().getSqlTypeName() != SqlTypeName.TIMESTAMP
        || !(call.getOperands().get(1) instanceof RexLiteral unit)) {
      return -1;
    }
    return switch (String.valueOf(unit.getValue())) {
      case "DAY" -> 86_400_000L;
      case "HOUR" -> 3_600_000L;
      case "MINUTE" -> 60_000L;
      case "SECOND" -> 1_000L;
      case "MILLISECOND" -> 1L;
      default -> -1L;
    };
  }

  private static boolean isTimestampIdentityCast(RexCall call) {
    if (call.getKind() != SqlKind.CAST || call.getOperands().size() != 1) {
      return false;
    }
    RelDataType source = call.getOperands().get(0).getType();
    return (source.getSqlTypeName() == SqlTypeName.TIMESTAMP
            || source.getSqlTypeName() == SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE)
        && source.getSqlTypeName() == call.getType().getSqlTypeName()
        && call.getType().getPrecision() >= source.getPrecision();
  }

  private static boolean containsDecimalUdf(RexNode node) {
    if (!(node instanceof RexCall call)) return false;
    if (call.getType().getSqlTypeName() == SqlTypeName.DECIMAL
        && call.getOperator()
            instanceof org.apache.flink.table.planner.functions.bridging.BridgingSqlFunction function
        && function.getDefinition() instanceof org.apache.flink.table.functions.ScalarFunction) {
      return true;
    }
    return call.getOperands().stream().anyMatch(RexExpression::containsDecimalUdf);
  }

  private static boolean containsScalarUdf(RexNode node) {
    if (!(node instanceof RexCall call)) return false;
    if (call.getOperator()
            instanceof org.apache.flink.table.planner.functions.bridging.BridgingSqlFunction function
        && function.getDefinition() instanceof org.apache.flink.table.functions.ScalarFunction) {
      return true;
    }
    return call.getOperands().stream().anyMatch(RexExpression::containsScalarUdf);
  }

  private boolean emitHostExpression(RexCall call, boolean fuseConsumers) {
    if (fuseConsumers && !validateGeneratedExpression(call)) return false;
    List<RexNode> arguments = new ArrayList<>();
    List<org.apache.flink.table.types.logical.LogicalType> types = new ArrayList<>();
    List<Integer> codes = new ArrayList<>();
    RexNode expression;
    try {
      expression = hostExpressionArguments(call, arguments, types, codes, fuseConsumers);
    } catch (IllegalArgumentException e) {
      return reject(e.getMessage());
    }
    int returnCode = hostCastTypeCode(call.getType());
    if (returnCode < 0) {
      return reject("unsupported generated-expression result type " + call.getType());
    }
    if (arguments.isEmpty()) {
      types.add(new org.apache.flink.table.types.logical.IntType());
      codes.add(tech.streamfusion.operator.NativeUdf.TYPE_INT);
    }
    FlinkExpressionFunction function;
    Method eval;
    try {
      function =
          new FlinkExpressionFunction(
              expression,
              types.toArray(org.apache.flink.table.types.logical.LogicalType[]::new),
              temporalConfig,
              expressionClassLoader);
      eval = FlinkExpressionFunction.class.getMethod("eval", Object[].class);
    } catch (Exception e) {
      return reject("host expression cannot be generated: " + e.getMessage());
    }
    for (org.apache.flink.table.functions.ScalarFunction dependency : function.functions()) {
      if (!claimUdfEvaluation(dependency)) return false;
    }
    int localIndex =
        addUdf(
            tech.streamfusion.operator.NativeUdf.Descriptor.forFunction(
                function, eval, codes.stream().mapToInt(Integer::intValue).toArray(), returnCode));
    add(KIND_UDF, longs.size(), Math.max(1, arguments.size()));
    longs.add((long) localIndex);
    longs.add((long) returnCode);
    if (arguments.isEmpty()) {
      add(KIND_LIT_INT, longs.size(), 0);
      longs.add(0L);
    }
    for (RexNode argument : arguments) {
      if (!emit(argument)) {
        return false;
      }
    }
    return true;
  }

  private RexNode hostExpressionArguments(
      RexNode node,
      List<RexNode> arguments,
      List<org.apache.flink.table.types.logical.LogicalType> types,
      List<Integer> codes,
      boolean fuseConsumers) {
    if (node instanceof RexLiteral) {
      return node;
    }
    if (node instanceof RexCall call
        && (fuseConsumers
            || needsTemporalFunction(call) && nativeUnixTimeFormat(call) == null
            || needsExactPower(call))) {
      List<RexNode> operands = new ArrayList<>();
      for (RexNode operand : call.getOperands()) {
        operands.add(hostExpressionArguments(operand, arguments, types, codes, fuseConsumers));
      }
      return call.clone(call.getType(), operands);
    }
    int code = hostCastTypeCode(node.getType());
    if (code < 0) {
      throw new IllegalArgumentException(
          "unsupported generated-expression argument type " + node.getType());
    }
    RexInputRef reference = new RexInputRef(arguments.size(), node.getType());
    arguments.add(node);
    types.add(
        org.apache.flink.table.planner.calcite.FlinkTypeFactory.toLogicalType(node.getType()));
    codes.add(code);
    return reference;
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
    Method eval;
    try {
      eval = HostCastFunction.class.getMethod("eval", Object.class);
    } catch (ReflectiveOperationException e) {
      return reject("host cast unavailable: " + e.getMessage());
    }
    int localIndex =
        addUdf(
            tech.streamfusion.operator.NativeUdf.Descriptor.forFunction(
                function, eval, new int[] {sourceCode}, targetCode));
    add(KIND_UDF, longs.size(), 1);
    longs.add((long) localIndex);
    longs.add((long) targetCode);
    return emit(call.getOperands().get(0));
  }

  /**
   * The target type code for a widening numeric cast {@code source → target}, or -1 if not safe.
   */
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
   * The target integer code for a narrowing cast to an integer type — an integer→narrower-integer
   * or a float/double→integer cast — or -1 if {@code target} is not an integer type or the cast is
   * not narrowing (widening is handled by {@link #wideningTargetCode}, an identity/same-rank cast
   * earlier). Admitted because Flink's primitive Java cast wraps (integer source) / saturates with
   * NaN→0 (float source), which the native wrapping kernel reproduces.
   */
  private static int narrowingIntTargetCode(SqlTypeName source, SqlTypeName target) {
    int from = source == SqlTypeName.DECIMAL ? 4 : numericRank(source);
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

  /**
   * A widening order over the numeric types (lower widens losslessly to higher); -1 if not numeric.
   */
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
   * Emits collection access with Flink's one-based array and first-matching map semantics. Invalid
   * array literals remain on the host so its plan-time validation error is preserved.
   */
  private boolean emitItem(RexCall call) {
    RexNode collection = call.getOperands().get(0);
    RexNode subscript = call.getOperands().get(1);
    SqlTypeName collectionType = collection.getType().getSqlTypeName();
    if (collectionType == SqlTypeName.ARRAY) {
      if (subscript.getType().getSqlTypeName() != SqlTypeName.INTEGER
          && subscript.getType().getSqlTypeName() != SqlTypeName.NULL) {
        return reject("ARRAY index must be INT");
      }
      if (subscript instanceof RexLiteral
          && !((RexLiteral) subscript).isNull()
          && !isIntLiteralAtLeast(subscript, 1)) {
        return reject("ARRAY index below 1");
      }
    } else if (collectionType == SqlTypeName.MAP
        && !(subscript instanceof RexLiteral)
        && !List.of(
                SqlTypeName.TINYINT,
                SqlTypeName.SMALLINT,
                SqlTypeName.INTEGER,
                SqlTypeName.BIGINT,
                SqlTypeName.DECIMAL,
                SqlTypeName.CHAR,
                SqlTypeName.VARCHAR,
                SqlTypeName.BOOLEAN,
                SqlTypeName.BINARY,
                SqlTypeName.VARBINARY,
                SqlTypeName.DATE,
                SqlTypeName.TIME,
                SqlTypeName.TIMESTAMP,
                SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE)
            .contains(collection.getType().getKeyType().getSqlTypeName())) {
      return reject("dynamic MAP key type " + collection.getType().getKeyType());
    } else if (collectionType != SqlTypeName.MAP) {
      return reject("ITEM over " + collectionType + " (only ARRAY and MAP)");
    }
    if (collectionType == SqlTypeName.MAP
        && !(subscript instanceof RexLiteral)
        && !org.apache.calcite.sql.type.SqlTypeUtil.equalSansNullability(
            collection.getType().getKeyType(), subscript.getType())
        && !(collection.getType().getKeyType().getFamily() == SqlTypeFamily.CHARACTER
            && subscript.getType().getFamily() == SqlTypeFamily.CHARACTER)) {
      return reject("dynamic MAP lookup requires matching key types");
    }
    int compactTimestampKey = 0;
    if (collectionType == SqlTypeName.MAP) {
      RelDataType keyType = collection.getType().getKeyType();
      SqlTypeName keyName = keyType.getSqlTypeName();
      boolean timestampKey =
          keyName == SqlTypeName.TIMESTAMP || keyName == SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE;
      if (keyType.isNullable()
          && (timestampKey && keyType.getPrecision() > 3
              || keyName == SqlTypeName.DECIMAL && keyType.getPrecision() > 18)) {
        return reject("nullable non-compact MAP key requires host lookup");
      }
      compactTimestampKey = timestampKey && keyType.getPrecision() <= 3 ? 1 : 0;
    }
    add(KIND_ITEM, compactTimestampKey, 2);
    return emit(collection) && emit(subscript);
  }

  private boolean emitStringWithIntegers(RexCall call, int op, int min, int max) {
    List<RexNode> args = call.getOperands();
    if (args.size() < min
        || args.size() > max
        || !isCharacter(args.get(0))
        || args.stream().skip(1).anyMatch(arg -> !isInt32(arg))) {
      return reject(call.getOperator().getName() + " requires a string and INT parameters");
    }
    return emitBuiltinCall(call, op);
  }

  private static boolean isInt32(RexNode arg) {
    return List.of(SqlTypeName.TINYINT, SqlTypeName.SMALLINT, SqlTypeName.INTEGER, SqlTypeName.NULL)
        .contains(arg.getType().getSqlTypeName());
  }

  private boolean emitPad(RexCall call, int op) {
    List<RexNode> args = call.getOperands();
    if (args.size() != 3
        || !isCharacter(args.get(0))
        || !isInt32(args.get(1))
        || !isCharacter(args.get(2))) {
      return reject(call.getOperator().getName() + " requires STRING, INT, STRING arguments");
    }
    return emitBuiltinCall(call, op);
  }

  private boolean emitSplitIndex(List<RexNode> args) {
    if (args.size() != 3
        || !isCharacter(args.get(0))
        || !isCharacter(args.get(1))
        || !isInt32(args.get(2))) {
      return reject("SPLIT_INDEX requires STRING, STRING, INT arguments");
    }
    add(KIND_CALL, 130, 3);
    for (RexNode arg : args) {
      if (!emit(arg)) {
        return false;
      }
    }
    return true;
  }

  /**
   * Emits {@code REGEXP_EXTRACT(str, pattern[, groupIndex])}. By default it routes through the
   * host's own {@code SqlFunctionUtils.regexpExtract} as a JVM-upcall node (see {@link
   * #emitRegexpExtractJvm}), so the regex is evaluated by {@code java.util.regex} exactly as Flink
   * would — byte-identical, and the rest of the expression still runs natively. Under the {@code
   * allowIncompatible} flag it instead uses the pure native Rust {@code regex} path (op 88, faster,
   * no JVM crossing), which agrees with Java on common syntax but can diverge on advanced features
   * (backreferences, lookaround, some Unicode/class edges).
   */
  private boolean emitRegexpExtract(List<RexNode> args) {
    if (NativeConfig.allowsIncompatible("REGEXP_EXTRACT")) {
      return emitRegexpExtractNative(args);
    }
    return emitRegexpExtractJvm(args);
  }

  /**
   * The pure-native REGEXP_EXTRACT (op 88), opt-in behind the allowIncompatible flag. The pattern
   * must be a string literal (so the native side compiles it once per batch) and the group index a
   * literal ≥ 0. The two-argument form (no explicit index) falls back.
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
   * Emits {@code REGEXP_EXTRACT} as a JVM-upcall node (op {@link #KIND_UDF}) backed by {@code
   * NativeBuiltinFunctions.regexpExtract} — Flink's own {@code SqlFunctionUtils.regexpExtract}
   * logic byte-for-byte, minus its per-call {@code Pattern.compile}. The native engine packs the
   * argument columns and upcalls per batch, so the match runs under {@code java.util.regex} and is
   * byte-identical to the host for every pattern (the two- and three-argument forms both apply).
   * The string arguments must be string-typed and the group index INTEGER or BIGINT; anything else
   * falls back.
   */
  private boolean emitRegexpExtractJvm(List<RexNode> args) {
    if (args.size() != 2 && args.size() != 3) {
      return reject("REGEXP_EXTRACT requires 2 or 3 arguments (str, pattern[, groupIndex])");
    }
    for (int i = 0; i < 2; i++) {
      if (udfTypeCode(args.get(i).getType().getSqlTypeName())
          != tech.streamfusion.operator.NativeUdf.TYPE_STRING) {
        return reject("REGEXP_EXTRACT requires string str/pattern arguments");
      }
    }
    Class<?>[] signature;
    int[] argCodes;
    if (args.size() == 3) {
      int indexCode = udfTypeCode(args.get(2).getType().getSqlTypeName());
      Class<?> indexClass;
      if (indexCode == tech.streamfusion.operator.NativeUdf.TYPE_INT) {
        indexClass = int.class;
      } else if (indexCode == tech.streamfusion.operator.NativeUdf.TYPE_LONG) {
        indexClass = long.class;
      } else {
        return reject("REGEXP_EXTRACT group index must be INTEGER or BIGINT");
      }
      signature = new Class<?>[] {String.class, String.class, indexClass};
      argCodes =
          new int[] {
            tech.streamfusion.operator.NativeUdf.TYPE_STRING,
            tech.streamfusion.operator.NativeUdf.TYPE_STRING,
            indexCode
          };
    } else {
      signature = new Class<?>[] {String.class, String.class};
      argCodes =
          new int[] {
            tech.streamfusion.operator.NativeUdf.TYPE_STRING,
            tech.streamfusion.operator.NativeUdf.TYPE_STRING
          };
    }
    Method regexpExtract;
    try {
      regexpExtract =
          tech.streamfusion.operator.NativeBuiltinFunctions.class.getMethod(
              "regexpExtract", signature);
    } catch (ReflectiveOperationException e) {
      return reject("REGEXP_EXTRACT host implementation unavailable: " + e.getMessage());
    }
    int returnCode = tech.streamfusion.operator.NativeUdf.TYPE_STRING;
    int localIndex =
        addUdf(
            tech.streamfusion.operator.NativeUdf.Descriptor.forBuiltin(
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

  private Method rejectUdfMethod(String reason) {
    reject(reason);
    return null;
  }

  private Method checkedUdfMethod(RexCall call) {
    org.apache.flink.table.functions.FunctionDefinition def =
        ((org.apache.flink.table.planner.functions.bridging.BridgingSqlFunction) call.getOperator())
            .getDefinition();
    if (!(def instanceof org.apache.flink.table.functions.ScalarFunction)) {
      return rejectUdfMethod("unsupported function/operator: " + call.getOperator().getName());
    }
    if (def instanceof org.apache.flink.table.functions.SpecializedFunction) {
      return rejectUdfMethod("UDF specialization requires Flink's code-generation context");
    }
    org.apache.flink.table.functions.ScalarFunction scalar =
        (org.apache.flink.table.functions.ScalarFunction) def;
    SqlTypeName resultType = call.getType().getSqlTypeName();
    if (!rowFusion && resultType == SqlTypeName.VARBINARY && ++binaryUdfCalls > 1) {
      return rejectUdfMethod("multiple binary UDF calls may share mutable result buffers");
    }
    int returnCode = udfTypeCode(call.getType());
    if (returnCode < 0) {
      return rejectUdfMethod("UDF return type not native: " + call.getType().getSqlTypeName());
    }
    List<RexNode> args = call.getOperands();
    for (RexNode argument : args) {
      if (udfTypeCode(argument.getType()) < 0) {
        return rejectUdfMethod(
            "UDF argument type not native: " + argument.getType().getSqlTypeName());
      }
    }
    Method eval = resolveEval(scalar, args);
    if (eval == null) {
      return rejectUdfMethod(
          "UDF " + scalar.getClass().getName() + " has no single eval of arity " + args.size());
    }
    if ((resultType == SqlTypeName.DECIMAL && eval.getReturnType() != BigDecimal.class)
        || (resultType == SqlTypeName.VARBINARY && eval.getReturnType() != byte[].class)) {
      return rejectUdfMethod("UDF result uses an unsupported Java conversion class");
    }
    return eval;
  }

  private boolean validateGeneratedExpression(RexNode node) {
    if (!(node instanceof RexCall call)) return true;
    if (call.getOperator()
            instanceof org.apache.flink.table.planner.functions.bridging.BridgingSqlFunction function
        && function.getDefinition() instanceof org.apache.flink.table.functions.ScalarFunction
        && checkedUdfMethod(call) == null) {
      return false;
    }
    return call.getOperands().stream().allMatch(this::validateGeneratedExpression);
  }

  /**
   * Emits a Flink user {@link org.apache.flink.table.functions.ScalarFunction} call as a JVM-upcall
   * node (op {@link #KIND_UDF}). The function is registered in {@link
   * tech.streamfusion.operator.NativeUdf} and invoked per batch by the native {@code JvmUdf}
   * expression, which runs the actual {@code eval} over the Arrow argument columns (columnar) — so
   * the result is byte-identical to Flink. Admitted only when the definition is a {@code
   * ScalarFunction}, every argument and the result map to a supported marshalling type, and exactly
   * one {@code eval} overload of the right arity exists (an ambiguous overload is not guessed);
   * otherwise the call falls back.
   */
  private boolean emitUdf(RexCall call) {
    Method eval = checkedUdfMethod(call);
    if (eval == null) return false;
    var scalar = (org.apache.flink.table.functions.ScalarFunction)
        ((org.apache.flink.table.planner.functions.bridging.BridgingSqlFunction) call.getOperator())
            .getDefinition();
    if (!claimUdfEvaluation(scalar)) return false;
    int returnCode = udfTypeCode(call.getType());
    List<RexNode> args = call.getOperands();
    int[] argCodes = args.stream().mapToInt(arg -> udfTypeCode(arg.getType())).toArray();
    int localIndex =
        addUdf(
            tech.streamfusion.operator.NativeUdf.Descriptor.forFunction(
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

  private boolean claimUdfEvaluation(org.apache.flink.table.functions.ScalarFunction function) {
    if (!function.isDeterministic() && !statefulUdfEvaluations.add(function.functionIdentifier())) {
      return reject("shared stateful scalar UDF requires Flink's per-row invocation order");
    }
    return true;
  }

  /**
   * The native UDF marshalling type code for a SQL type (see NativeUdf.TYPE_*), or -1 if
   * unsupported.
   */
  private static int udfTypeCode(RelDataType type) {
    if (type.getSqlTypeName() == SqlTypeName.DECIMAL) {
      return tech.streamfusion.operator.NativeUdf.decimalType(type.getPrecision(), type.getScale());
    }
    return udfTypeCode(type.getSqlTypeName());
  }

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
      case VARBINARY:
        return tech.streamfusion.operator.NativeUdf.TYPE_BINARY;
      default:
        return -1;
    }
  }

  /**
   * Emits the retained Rust extraction paths. Other units and types are handled by the generated
   * temporal evaluator before reaching this method.
   */
  private boolean emitExtract(RexCall call) {
    List<RexNode> operands = call.getOperands();
    if (operands.size() != 2 || !(operands.get(0) instanceof RexLiteral)) {
      return reject("unsupported EXTRACT form");
    }
    RexNode source = operands.get(1);
    SqlTypeName sourceType = source.getType().getSqlTypeName();
    String calendarUnit = String.valueOf(((RexLiteral) operands.get(0)).getValue());
    int calendarOp =
        switch (calendarUnit) {
          case "QUARTER" -> 133;
          case "WEEK" -> 134;
          case "DOY" -> 135;
          case "DOW" -> 136;
          default -> -1;
        };
    if (calendarOp >= 0
        && (sourceType == SqlTypeName.DATE || sourceType == SqlTypeName.TIMESTAMP)) {
      add(KIND_CALL, calendarOp, 1);
      return emit(source);
    }
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
   * {@code EXTRACT} over a {@code TIMESTAMP_LTZ}, whose fields depend on the session time zone. By
   * default routes through Flink's own {@code DateTimeUtils.extractFromTimestamp(unit, ts, zone)}
   * via the JVM upcall (byte-identical); behind {@code allowIncompatible} emits the native {@code
   * chrono-tz} path (op 91: the timestamp, the chrono field name, the zone id).
   */
  private boolean emitExtractLtz(
      RexNode source, Object unit, String field, SqlTypeName returnType) {
    if (sessionZoneId == null) {
      return reject("EXTRACT over TIMESTAMP_LTZ: session time zone unavailable");
    }
    int returnCode =
        returnType == SqlTypeName.INTEGER
            ? tech.streamfusion.operator.NativeUdf.TYPE_INT
            : tech.streamfusion.operator.NativeUdf.TYPE_LONG;
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
        new tech.streamfusion.operator.LtzDateTimeFunctions.Extract(unit.toString(), sessionZoneId),
        returnCode);
  }

  /**
   * The chrono field name for a Calcite {@code TimeUnitRange}, or null if not a supported integer
   * field.
   */
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

  /** The best public {@code eval} for the operands, or null when the Java overload is ambiguous. */
  private static Method resolveEval(Object function, List<RexNode> args) {
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
        int argumentScore = udfParameterScore(parameter, args.get(i).getType().getSqlTypeName());
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

  private static int udfParameterScore(Class<?> parameter, SqlTypeName type) {
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
    switch (type) {
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
      default:
        return -1;
    }
    if (parameter == argument) {
      return 2;
    }
    return parameter.isAssignableFrom(argument) ? 1 : -1;
  }

  /**
   * Emits {@code DATE_FORMAT(timestamp, format)} (op 86). Admitted only for a plain {@code
   * TIMESTAMP} (not local-zoned, whose formatting would depend on the session zone) with a literal
   * format whose Java pattern translates to a byte-identical chrono pattern (see {@link
   * #toChronoFormat}); the translated pattern is passed to the native side. Other patterns use the
   * generated temporal evaluator before reaching this method.
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
   * {@code DATE_FORMAT} over a {@code TIMESTAMP_LTZ}, whose formatting depends on the session time
   * zone. By default it routes through Flink's own {@code DateTimeUtils.formatTimestamp(ts,
   * pattern, zone)} via the columnar JVM upcall — byte-identical. Behind {@code allowIncompatible}
   * it emits the pure-native {@code chrono-tz} path (op 90: the timestamp, the chrono pattern, the
   * zone id), which can diverge from the JVM at tz-database edges (see the divergences note).
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
        new tech.streamfusion.operator.LtzDateTimeFunctions.DateFormat(javaPattern, sessionZoneId),
        tech.streamfusion.operator.NativeUdf.TYPE_STRING);
  }

  /**
   * Emits an arity-1 JVM upcall over a single {@code TIMESTAMP_LTZ} column (marshalled as epoch
   * millis), backed by the given serializable {@link
   * org.apache.flink.table.functions.ScalarFunction} that calls Flink's own zone-aware datetime
   * code — the LTZ parity path, mirroring {@link #emitRegexpExtractJvm}.
   */
  private boolean emitLtzUpcall(
      RexNode timestamp, org.apache.flink.table.functions.ScalarFunction function, int returnCode) {
    Method eval;
    try {
      eval = function.getClass().getMethod("eval", Long.class);
    } catch (ReflectiveOperationException e) {
      return reject("LTZ datetime host implementation unavailable: " + e.getMessage());
    }
    int localIndex =
        addUdf(
            tech.streamfusion.operator.NativeUdf.Descriptor.forFunction(
                function,
                eval,
                new int[] {tech.streamfusion.operator.NativeUdf.TYPE_TIMESTAMP},
                returnCode));
    add(KIND_UDF, longs.size(), 1);
    longs.add((long) localIndex);
    longs.add((long) returnCode);
    return emit(timestamp);
  }

  /**
   * Whether the native {@code chrono-tz} path accepts this session zone: IANA names ({@code
   * Region/City}), {@code UTC}, and fixed offsets ({@code +HH:MM}). Legacy JVM forms ({@code
   * GMT+1}, {@code PST}) that {@code chrono-tz} rejects fall back, so we never diverge silently on
   * an unparseable zone.
   */
  private static boolean nativeZoneSupported(String zoneId) {
    return "UTC".equals(zoneId) || zoneId.indexOf('/') >= 0 || zoneId.matches("[+-]\\d{2}:\\d{2}");
  }

  /**
   * Translates a Java {@code DateTimeFormatter} pattern to the equivalent chrono strftime pattern,
   * or null if it uses any field the translation can't reproduce byte-for-byte. Only zero-padded
   * numeric fields (the unambiguous ones) and literal separators are admitted; text fields,
   * fractional seconds, am/pm, zones, and single-letter (non-padded) fields fall back.
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
      // UPPER/LOWER have an exact default: route to the host's own case folding via a JVM upcall,
      // so
      // they run natively and byte-identically without the flag. The transcendental math ops
      // (last-ULP
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
   * tech.streamfusion.operator.NativeBuiltinFunctions}, which calls Flink's own {@code
   * BinaryStringData} case folding — byte-identical to the host, with the rest of the expression
   * still native. The argument must be string-typed.
   */
  private boolean emitStringCaseJvm(RexCall call, String method) {
    List<RexNode> args = call.getOperands();
    if (args.size() != 1) {
      return reject(method + " requires one argument");
    }
    if (udfTypeCode(args.get(0).getType().getSqlTypeName())
        != tech.streamfusion.operator.NativeUdf.TYPE_STRING) {
      return reject(method + " requires a string argument");
    }
    Method impl;
    try {
      impl =
          tech.streamfusion.operator.NativeBuiltinFunctions.class.getMethod(
              method, org.apache.flink.table.data.binary.BinaryStringData.class);
    } catch (ReflectiveOperationException e) {
      return reject(method + " host implementation unavailable: " + e.getMessage());
    }
    int returnCode = tech.streamfusion.operator.NativeUdf.TYPE_STRING;
    int localIndex =
        addUdf(
            tech.streamfusion.operator.NativeUdf.Descriptor.forBuiltin(
                impl, new int[] {tech.streamfusion.operator.NativeUdf.TYPE_STRING}, returnCode));
    add(KIND_UDF, longs.size(), 1);
    longs.add((long) localIndex);
    longs.add((long) returnCode);
    return emit(args.get(0));
  }

  private static boolean needsExactPower(RexCall call) {
    String name = call.getOperator().getName();
    return ("POWER".equalsIgnoreCase(name) || "POW".equalsIgnoreCase(name))
        && !NativeConfig.allowsIncompatible("POWER");
  }

  /** The opt-in Rust implementation; the default path uses Flink's generated Math.pow call. */
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

  private boolean emitDecimalRound(RexCall call) {
    boolean truncate =
        call.getOperator()
            == org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable.TRUNCATE;
    List<RexNode> args = call.getOperands();
    if (args.isEmpty()
        || args.size() > 2
        || args.size() == 2 && (!(args.get(1) instanceof RexLiteral) || !isInt32(args.get(1)))) {
      return reject("DECIMAL " + call.getOperator().getName() + " requires a literal INT scale");
    }
    Integer round =
        args.size() == 1
            ? Integer.valueOf(0)
            : ((RexLiteral) args.get(1)).getValueAs(Integer.class);
    RelDataType input = args.get(0).getType();
    RelDataType output = call.getType();
    if (round != null && round < -38) {
      if (truncate) {
        return emitHostExpression(call, true);
      }
      HostDecimalRoundFunction function =
          new HostDecimalRoundFunction(input.getPrecision(), input.getScale(), round);
      Method eval;
      try {
        eval = HostDecimalRoundFunction.class.getMethod("eval", BigDecimal.class);
      } catch (ReflectiveOperationException e) {
        return reject("decimal ROUND unavailable: " + e.getMessage());
      }
      int returnCode = hostCastTypeCode(output);
      int localIndex =
          addUdf(
              tech.streamfusion.operator.NativeUdf.Descriptor.forFunction(
                  function, eval, new int[] {hostCastTypeCode(input)}, returnCode));
      add(KIND_UDF, longs.size(), 1);
      longs.add((long) localIndex);
      longs.add((long) returnCode);
      return emit(args.get(0));
    }
    add(
        truncate ? KIND_DECIMAL_TRUNCATE : KIND_DECIMAL_ROUND,
        output.getPrecision() * 100 + output.getScale(),
        2);
    if (!emit(args.get(0))) {
      return false;
    }
    if (args.size() == 2) {
      return emit(args.get(1));
    }
    add(KIND_LIT_INT, longs.size(), 0);
    longs.add(0L);
    return true;
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
        + name.toUpperCase(Locale.ROOT)
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

  /** Reuses the literal-set trim kernels for SQL's BOTH/LEADING/TRAILING syntax. */
  private boolean emitTrim(RexCall call) {
    List<RexNode> operands = call.getOperands();
    if (operands.size() != 3
        || !(operands.get(0) instanceof RexLiteral flag)
        || !(operands.get(1) instanceof RexLiteral trim)
        || !isCharacter(trim)
        || !isCharacter(operands.get(2))) {
      return reject("TRIM requires a literal trim set");
    }
    int op = switch (String.valueOf(flag.getValue())) {
      case "BOTH" -> 113;
      case "LEADING" -> 139;
      case "TRAILING" -> 140;
      default -> -1;
    };
    if (op < 0) {
      return reject("unsupported TRIM direction");
    }
    add(KIND_CALL, op, 2);
    return emit(operands.get(2)) && emit(trim);
  }

  /**
   * The op code for an admitted scalar function (matched by name, since Flink delivers them as
   * {@code OTHER_FUNCTION} calls), or -1. ASCII-equivalent to the host; the Unicode edges (case
   * folding, code-point vs UTF-16 length) are recorded in divergences/07.
   */
  private static int functionOpCode(String name) {
    // Compatible (always-native) unary functions only. Functions whose native result can diverge
    // from
    // the host (UPPER/LOWER, transcendental math) are handled by the incompatible dispatch in
    // emitCall, gated behind the allowIncompatible flag — see INCOMPATIBLE_UNARY.
    switch (name.toUpperCase(Locale.ROOT)) {
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

  /** Whether {@code call} needs Flink's resolved decimal result precision and scale. */
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
