package io.github.jordepic.streamfusion.planner;

import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalWatermarkAssigner;

/**
 * Recognizes a {@link StreamPhysicalWatermarkAssigner} the native columnar assigner can reproduce:
 * the bounded out-of-orderness form, where the watermark is the rowtime itself ({@code WATERMARK FOR
 * rt AS rt}, delay 0) or the rowtime minus an interval constant ({@code rt - INTERVAL '5' SECOND}).
 * Any other watermark expression — a non-constant delay, a different column, an unsupported function
 * — falls back to the host. The interval literal's value is already milliseconds for a day-time
 * interval, which is what the operator subtracts.
 */
final class WatermarkAssignerMatcher {

  private WatermarkAssignerMatcher() {}

  static boolean matches(StreamPhysicalWatermarkAssigner wm) {
    return delayMillis(wm) != null;
  }

  static int rowtimeColumn(StreamPhysicalWatermarkAssigner wm) {
    return wm.rowtimeFieldIndex();
  }

  /** The bounded-out-of-orderness delay in millis, or null if the expression is not that shape. */
  static Long delayMillis(StreamPhysicalWatermarkAssigner wm) {
    RexNode expr = wm.watermarkExpr();
    int rowtime = wm.rowtimeFieldIndex();
    // WATERMARK FOR rt AS rt — no delay.
    if (expr instanceof RexInputRef && ((RexInputRef) expr).getIndex() == rowtime) {
      return 0L;
    }
    // WATERMARK FOR rt AS rt - INTERVAL <constant>.
    if (expr instanceof RexCall) {
      RexCall call = (RexCall) expr;
      if (call.getOperator().getKind() == SqlKind.MINUS && call.getOperands().size() == 2) {
        RexNode left = call.getOperands().get(0);
        RexNode right = call.getOperands().get(1);
        if (left instanceof RexInputRef
            && ((RexInputRef) left).getIndex() == rowtime
            && right instanceof RexLiteral) {
          Long millis = ((RexLiteral) right).getValueAs(Long.class);
          if (millis != null && millis >= 0) {
            return millis;
          }
        }
      }
    }
    return null;
  }
}
