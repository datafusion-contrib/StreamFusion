package tech.streamfusion.planner.common;

import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.types.logical.RowType;

/** Shared, immutable Calc declaration after the native output schema has been verified. */
public final class CalcSpec {
  private final RowType outputType;
  private final PreparedExpression expression;

  private CalcSpec(RowType outputType, EncodedExpression expression) {
    this.outputType = outputType;
    this.expression = PreparedExpression.copyOf(expression);
  }

  public static Admission<CalcSpec> check(Calc calc, EncodedExpression expression, boolean retained) {
    return check(calc.getInput().getRowType(),
        FlinkTypeFactory.toLogicalRowType(calc.getRowType()), expression, retained);
  }

  public static Admission<CalcSpec> check(
      RelDataType input, RowType output, EncodedExpression expression, boolean retained) {
    String mismatch = retained
        ? CalcOutputTypeCheck.retainedMismatch(expression, input, output)
        : CalcOutputTypeCheck.mismatch(expression, input, output);
    return mismatch == null
        ? Admission.accepted(new CalcSpec(output, expression)) : Admission.declined(mismatch);
  }

  public RowType outputType() { return outputType; }

  public PreparedExpression expression() { return expression; }
}
