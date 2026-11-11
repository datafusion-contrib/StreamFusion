package tech.streamfusion.planner.stream;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.RowType;
import tech.streamfusion.operator.WatermarkExpression;
import tech.streamfusion.planner.ExpressionPlanning;
import tech.streamfusion.planner.common.CalcOutputTypeCheck;
import tech.streamfusion.planner.common.EncodedExpression;

/** Shared watermark admission and native type validation for assigners and source pushdown. */
final class WatermarkExpressionPlanner {
  private WatermarkExpressionPlanner() {}

  static WatermarkExpression encode(
      RexNode expression, int rowtimeColumn, boolean epochMillis, RelDataType inputType) {
    EncodedExpression encoded =
        ExpressionPlanning.encodeWatermark(expression, rowtimeColumn, epochMillis);
    if (encoded == null
        || CalcOutputTypeCheck.mismatch(encoded, inputType, RowType.of(new BigIntType())) != null) {
      return null;
    }
    return new WatermarkExpression(
        encoded.kinds(),
        encoded.payload(),
        encoded.childCounts(),
        encoded.longs(),
        encoded.doubles(),
        encoded.strings(),
        expression.toString());
  }
}
