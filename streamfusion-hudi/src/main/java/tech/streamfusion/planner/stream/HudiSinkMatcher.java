package tech.streamfusion.planner.stream;

import org.apache.calcite.rel.RelNode;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory$;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalRel;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalSink;
import org.apache.flink.table.planner.plan.utils.ChangelogPlanUtils;
import org.apache.hudi.configuration.OptionsResolver;
import org.apache.hudi.table.HoodieTableSink;
import tech.streamfusion.hudi.HudiSinkPlan;
import tech.streamfusion.hudi.NativeHudiAppendSink;
import tech.streamfusion.hudi.NativeHudiMorSink;
import tech.streamfusion.planner.common.NativePlanDetails;
import tech.streamfusion.planner.common.StreamPlanningContext;

final class HudiSinkMatcher {
  static boolean appliesTo(StreamPhysicalSink sink) {
    return sink.tableSink() instanceof HoodieTableSink;
  }

  static RelNode substitute(StreamPhysicalSink sink, StreamPlanningContext context) {
    if (sink.upsertMaterialize()
        || ChangelogPlanUtils.generateUpdateBefore((StreamPhysicalRel) sink.getInput())) {
      context.decline("Hudi native writes require post-images without sink materialization");
      return null;
    }
    String constraint = SinkConstraintGate.fallbackReason(sink, true);
    if (constraint != null) {
      context.decline(constraint);
      return null;
    }
    var table = (HoodieTableSink) sink.tableSink();
    var type = FlinkTypeFactory$.MODULE$.toLogicalRowType(sink.getInput().getRowType());
    HudiSinkPlan plan;
    try {
      if (OptionsResolver.isAppendMode(table.getConf())) {
        if (!ChangelogPlanUtils.isInsertOnly((StreamPhysicalRel) sink.getInput())) {
          context.decline("Hudi native append requires insert-only input");
          return null;
        }
        plan = NativeHudiAppendSink.plan(table, type);
      } else {
        plan = NativeHudiMorSink.plan(table, type);
      }
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
