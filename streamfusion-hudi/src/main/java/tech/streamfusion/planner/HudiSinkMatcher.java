package tech.streamfusion.planner;

import org.apache.calcite.rel.RelNode;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory$;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalRel;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalSink;
import org.apache.flink.table.planner.plan.utils.ChangelogPlanUtils;
import org.apache.hudi.table.HoodieTableSink;
import tech.streamfusion.hudi.NativeHudiAppendSink;

final class HudiSinkMatcher {
  static boolean appliesTo(StreamPhysicalSink sink) {
    return sink.tableSink() instanceof HoodieTableSink;
  }

  static RelNode substitute(StreamPhysicalSink sink, PlanContext context) {
    if (sink.upsertMaterialize()
        || !ChangelogPlanUtils.isInsertOnly((StreamPhysicalRel) sink.getInput())) {
      context.decline("Hudi native append requires insert-only input without materialization");
      return null;
    }
    String constraint = SinkConstraintGate.fallbackReason(sink, true);
    if (constraint != null) {
      context.decline(constraint);
      return null;
    }
    var table = (HoodieTableSink) sink.tableSink();
    var type = FlinkTypeFactory$.MODULE$.toLogicalRowType(sink.getInput().getRowType());
    NativeHudiAppendSink.Plan plan;
    try {
      plan = NativeHudiAppendSink.plan(table, type);
    } catch (IllegalArgumentException | LinkageError failure) {
      context.decline("Hudi sink: " + failure.getMessage());
      return null;
    }
    return new StreamPhysicalNativeHudiSink(
        sink.getCluster(),
        sink.getTraitSet(),
        sink.getInput(),
        sink.getRowType(),
        plan,
        NativePlanDetails.from(sink));
  }
}
