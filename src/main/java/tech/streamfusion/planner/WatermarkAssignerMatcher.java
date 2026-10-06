package tech.streamfusion.planner;

import org.apache.calcite.rel.RelNode;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalWatermarkAssigner;
import org.apache.flink.table.planner.utils.ShortcutUtils;
import tech.streamfusion.operator.WatermarkExpression;

/**
 * Recognizes a {@link StreamPhysicalWatermarkAssigner} the native columnar assigner can reproduce:
 * the rowtime itself or a sequence of subtractions by day-time or year-month constants. The shared
 * expression encoder and native type checker decide admission; other expressions stay on Flink.
 */
final class WatermarkAssignerMatcher {

  private WatermarkAssignerMatcher() {}

  static boolean matches(StreamPhysicalWatermarkAssigner wm) {
    return ShortcutUtils.unwrapTableConfig(wm)
                .get(ExecutionConfigOptions.TABLE_EXEC_SOURCE_IDLE_TIMEOUT)
                .toMillis()
            <= 0
        && expression(wm) != null;
  }

  static int rowtimeColumn(StreamPhysicalWatermarkAssigner wm) {
    return wm.rowtimeFieldIndex();
  }

  static WatermarkExpression expression(StreamPhysicalWatermarkAssigner wm) {
    return WatermarkExpressionPlanner.encode(
        wm.watermarkExpr(), wm.rowtimeFieldIndex(), false, wm.getInput().getRowType());
  }

  static RelNode substitute(StreamPhysicalWatermarkAssigner wm, PlanContext ctx) {
    return new StreamPhysicalNativeWatermarkAssigner(
        wm.getCluster(),
        wm.getTraitSet(),
        wm.getInputs().get(0),
        wm.getRowType(),
        WatermarkAssignerMatcher.rowtimeColumn(wm),
        WatermarkAssignerMatcher.expression(wm));
  }
}
