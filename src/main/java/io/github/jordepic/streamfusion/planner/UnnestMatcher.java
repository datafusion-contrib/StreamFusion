package io.github.jordepic.streamfusion.planner;

import java.lang.reflect.Field;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory$;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalCorrelate;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalCorrelateBase;

/**
 * Recognizes the INNER {@code UNNEST} of an array the native operator reproduces: a {@link
 * StreamPhysicalCorrelate} whose table function is Flink's internal {@code $UNNEST_ROWS$} over a
 * single {@code ARRAY} column, with an INNER join and no extra condition, where the unnested element
 * is a single column (output arity = input arity + 1). The native operator fans each input row out to
 * one row per array element — exactly Flink's `$UNNEST_ROWS$` — appending the element. A `MAP`/
 * `MULTISET` unnest (element is a key/value row), an `ARRAY<ROW>` unnest (element flattens to several
 * columns), {@code WITH ORDINALITY}, a LEFT (outer) unnest, or an extra condition all fall back.
 */
final class UnnestMatcher {

  private UnnestMatcher() {}

  // The internal name of Flink's BuiltInFunctionDefinitions.INTERNAL_UNNEST_ROWS (the with-ordinality
  // variant has a distinct name, so it is naturally excluded).
  private static final String UNNEST_ROWS = "$UNNEST_ROWS$1";

  static boolean matches(StreamPhysicalCorrelate correlate) {
    if (correlate.condition().isDefined()) {
      return false; // an extra correlate condition is not reproduced
    }
    if (joinType(correlate) != JoinRelType.INNER) {
      return false; // only INNER UNNEST (a LEFT unnest null-pads empty arrays)
    }
    RexNode call = correlate.scan().getCall();
    if (!(call instanceof RexCall)
        || !((RexCall) call).getOperator().getName().equals(UNNEST_ROWS)) {
      return false;
    }
    RexCall unnest = (RexCall) call;
    if (unnest.getOperands().size() != 1
        || !(unnest.getOperands().get(0) instanceof RexFieldAccess)) {
      return false; // a single, directly-referenced array column
    }
    int inputArity = correlate.getInput().getRowType().getFieldCount();
    // Single appended element column: a MAP (key,value), ARRAY<ROW> (flattened fields), or
    // with-ordinality unnest would widen the output by more than one.
    if (correlate.getRowType().getFieldCount() != inputArity + 1) {
      return false;
    }
    if (correlate.getInput().getRowType().getFieldList().get(arrayColumn(correlate)).getType()
            .getSqlTypeName()
        != SqlTypeName.ARRAY) {
      return false; // the unnested column must be an ARRAY (the native side reads a List)
    }
    return FilterCalcMatcher.convertibleRow(correlate.getRowType());
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
    return "correlate: only an INNER UNNEST of a single ARRAY column (element a single supported"
        + " column) is supported — MAP/MULTISET, ARRAY<ROW>, WITH ORDINALITY, LEFT, or an extra"
        + " condition fall back";
  }
}
