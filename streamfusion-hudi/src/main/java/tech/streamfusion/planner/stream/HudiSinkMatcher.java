package tech.streamfusion.planner.stream;

import org.apache.calcite.rel.RelNode;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory$;
import org.apache.flink.table.planner.plan.metadata.FlinkRelMetadataQuery;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalRel;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalSink;
import org.apache.flink.table.planner.plan.utils.ChangelogPlanUtils;
import org.apache.flink.table.planner.plan.utils.UpsertKeyUtil;
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
    if (sink.upsertMaterialize() && context.keyedStateUnsupportedReason() != null) {
      context.decline(context.keyedStateUnsupportedReason());
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
    int[] materializationKeys = null;
    int[] inputUpsertKeys = null;
    RelNode input = sink.getInput();
    if (sink.upsertMaterialize()) {
      var primary = sink.contextResolvedTable().getResolvedSchema().getPrimaryKey().orElseThrow();
      materializationKeys =
          primary.getColumns().stream().mapToInt(type.getFieldNames()::indexOf).toArray();
      var metadata = FlinkRelMetadataQuery.reuseOrCreate(sink.getCluster().getMetadataQuery());
      inputUpsertKeys =
          UpsertKeyUtil.getSmallestKey(metadata.getUpsertKeys(context.originalNode(input)));
      input =
          new StreamPhysicalNativeColumnarExchange(
              sink.getCluster(),
              input.getTraitSet(),
              input,
              input.getRowType(),
              materializationKeys,
              false);
    }
    return new StreamPhysicalNativeHudiSink(
        sink.getCluster(),
        sink.getTraitSet(),
        input,
        sink.getRowType(),
        plan,
        materializationKeys,
        inputUpsertKeys,
        NativePlanDetails.from(sink));
  }

}
