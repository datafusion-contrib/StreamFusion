package tech.streamfusion.planner;

import java.lang.reflect.Field;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalCorrelate;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalCorrelateBase;

/**
 * Recognizes the INNER {@code UNNEST} of an array the native operator reproduces: a {@link
 * StreamPhysicalCorrelate} whose table function is Flink's internal {@code $UNNEST_ROWS$} over a
 * single {@code ARRAY}, {@code MAP}, or {@code MULTISET} column with an INNER or LEFT join. The native
 * operator fans each input row out to one row per element — exactly Flink's `$UNNEST_ROWS$`. A scalar
 * ARRAY element is appended as one column; an {@code ARRAY<ROW>} element is flattened into one column
 * per struct field; a {@code MAP} appends a key and a value column; a {@code MULTISET} appends the
 * element, repeated by its count; {@code WITH ORDINALITY} appends a trailing 1-based ordinal; a LEFT
 * unnest keeps a row whose collection is empty/null with the appended columns null (Flink's behavior).
 * A filter pushed into the correlate (e.g. {@code … WHERE element > x}) becomes a {@code condition} on
 * the node; it is applied as a native filter over the unnest output (INNER only), so it is supported
 * when the expression engine can encode it. A condition the expression engine can't encode, or a
 * condition over a LEFT unnest, fall back.
 */
final class UnnestMatcher {

  private UnnestMatcher() {}

  // Internal names of Flink's BuiltInFunctionDefinitions.INTERNAL_UNNEST_ROWS and its
  // with-ordinality variant (the latter appends a 1-based ordinality column).
  private static final String UNNEST_ROWS = "$UNNEST_ROWS$1";
  private static final String UNNEST_ROWS_WITH_ORDINALITY = "$UNNEST_ROWS_WITH_ORDINALITY$1";

  static boolean matches(StreamPhysicalCorrelate correlate) {
    JoinRelType joinType = joinType(correlate);
    if (joinType != JoinRelType.INNER && joinType != JoinRelType.LEFT) {
      return false; // INNER, or LEFT (null-pads an empty/null collection)
    }
    RexNode call = correlate.scan().getCall();
    if (!(call instanceof RexCall)) {
      return false;
    }
    String operator = ((RexCall) call).getOperator().getName();
    if (!operator.equals(UNNEST_ROWS) && !operator.equals(UNNEST_ROWS_WITH_ORDINALITY)) {
      return false;
    }
    RexCall unnest = (RexCall) call;
    if (unnest.getOperands().size() != 1
        || !(unnest.getOperands().get(0) instanceof RexFieldAccess)) {
      return false; // a single, directly-referenced array column
    }
    int inputArity = correlate.getInput().getRowType().getFieldCount();
    RelDataType collType =
        correlate.getInput().getRowType().getFieldList().get(arrayColumn(correlate)).getType();
    // Columns appended to the input row: a scalar ARRAY element adds one, an ARRAY<ROW> flattens to
    // one per struct field, a MAP adds two (key, value), a MULTISET adds one (the element, repeated by
    // its count), and WITH ORDINALITY adds a trailing ordinal.
    int appended;
    switch (collType.getSqlTypeName()) {
      case ARRAY:
        RelDataType elementType = collType.getComponentType();
        appended = elementType.isStruct() ? elementType.getFieldCount() : 1;
        break;
      case MAP:
        appended = 2;
        break;
      case MULTISET:
        appended = 1;
        break;
      default:
        return false; // the native side reads a List or Map
    }
    if (withOrdinality(correlate)) {
      appended += 1;
    }
    if (correlate.getRowType().getFieldCount() != inputArity + appended) {
      return false;
    }
    if (!FilterCalcMatcher.convertibleRow(correlate.getRowType())) {
      return false;
    }
    if (!correlate.condition().isDefined()) {
      return true;
    }
    // A pushed condition over a LEFT unnest changes outer semantics (a filtered-out element should
    // still leave a null-pad), which the native filter doesn't model; only INNER applies it as a
    // post-unnest filter, and only when the expression engine can encode it.
    return !isLeft(correlate) && encodedCondition(correlate) != null;
  }

  /** Whether this is a LEFT (outer) unnest, which null-pads an empty/null collection. */
  static boolean isLeft(StreamPhysicalCorrelate correlate) {
    return joinType(correlate) == JoinRelType.LEFT;
  }

  /** Whether the unnested column is a MULTISET (a MAP&lt;T,count&gt;: each element repeated by count). */
  static boolean isMultiset(StreamPhysicalCorrelate correlate) {
    return correlate.getInput().getRowType().getFieldList().get(arrayColumn(correlate)).getType()
            .getSqlTypeName()
        == SqlTypeName.MULTISET;
  }

  /** Whether this is {@code UNNEST … WITH ORDINALITY} (a trailing 1-based ordinal column). */
  static boolean withOrdinality(StreamPhysicalCorrelate correlate) {
    return ((RexCall) correlate.scan().getCall())
        .getOperator()
        .getName()
        .equals(UNNEST_ROWS_WITH_ORDINALITY);
  }

  /**
   * The pushed correlate condition, encoded against the unnest output row, or null if there is none.
   * Flink's correlate condition indexes the table-function output (the element at function-output 0);
   * shift its refs by the input arity so they index the {@code [input cols.., element]} output the
   * native filter sees.
   */
  static RexExpression encodedCondition(StreamPhysicalCorrelate correlate) {
    if (!correlate.condition().isDefined()) {
      return null;
    }
    int inputArity = correlate.getInput().getRowType().getFieldCount();
    RexNode shifted = RexUtil.shift(correlate.condition().get(), inputArity);
    RexNode expanded = RexUtil.expandSearch(correlate.getCluster().getRexBuilder(), null, shifted);
    return RexExpression.encode(expanded);
  }

  /** Index of the unnested array column in the correlate's input row. */
  static int arrayColumn(StreamPhysicalCorrelate correlate) {
    RexFieldAccess access =
        (RexFieldAccess) ((RexCall) correlate.scan().getCall()).getOperands().get(0);
    return access.getField().getIndex();
  }

  /** The join type, read reflectively (the base rel keeps it in a private field). */
  private static JoinRelType joinType(StreamPhysicalCorrelate correlate) {
    try {
      Field field = StreamPhysicalCorrelateBase.class.getDeclaredField("joinType");
      field.setAccessible(true);
      return (JoinRelType) field.get(correlate);
    } catch (ReflectiveOperationException e) {
      return null; // can't confirm INNER → decline (clean fallback)
    }
  }

  static String unsupportedReason(StreamPhysicalCorrelate correlate) {
    return "correlate: only an INNER/LEFT UNNEST of a single ARRAY (scalar or ROW element), MAP, or"
        + " MULTISET column, optionally WITH ORDINALITY, is supported — a non-encodable condition or a"
        + " condition over a LEFT unnest fall back";
  }

  static RelNode substitute(StreamPhysicalCorrelate correlate, PlanContext ctx) {
    RelNode unnest =
        new StreamPhysicalNativeUnnest(
            correlate.getCluster(),
            correlate.getTraitSet(),
            correlate.getInputs().get(0),
            correlate.getRowType(),
            UnnestMatcher.arrayColumn(correlate),
            UnnestMatcher.withOrdinality(correlate),
            UnnestMatcher.isLeft(correlate),
            UnnestMatcher.isMultiset(correlate));
    RexExpression condition = UnnestMatcher.encodedCondition(correlate);
    if (condition == null) {
      return unnest;
    }
    // A filter pushed into the correlate (… WHERE element > x) is applied as a native filter
    // over the unnest output, with an identity projection (the unnest already produced the
    // correlate's output columns). The condition's refs were shifted to index that output.
    int arity = correlate.getRowType().getFieldCount();
    int[] identity = new int[arity];
    for (int i = 0; i < arity; i++) {
      identity[i] = i;
    }
    return new StreamPhysicalNativeFilter(
        correlate.getCluster(),
        correlate.getTraitSet(),
        unnest,
        correlate.getRowType(),
        identity,
        condition.kinds(),
        condition.payload(),
        condition.childCounts(),
        condition.longs(),
        condition.doubles(),
        condition.strings(),
        condition.udfBinding());
  }
}
