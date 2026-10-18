package tech.streamfusion.planner;

import org.apache.calcite.rel.RelNode;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalRel;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalSink;
import org.apache.flink.table.planner.plan.utils.ChangelogPlanUtils;
import org.apache.iceberg.flink.IcebergTableSink;
import tech.streamfusion.iceberg.IcebergSinkPlan;

final class IcebergSinkMatcher {
  private IcebergSinkMatcher() {}

  static boolean appliesTo(StreamPhysicalSink sink) {
    return sink.tableSink() instanceof IcebergTableSink;
  }

  static RelNode substitute(StreamPhysicalSink sink, PlanContext context) {
    if (!NativeConfig.operatorEnabled("icebergSink")) {
      context.decline(Substitution.disabledReason("icebergSink"));
      return null;
    }
    if (sink.upsertMaterialize()) {
      context.decline("Iceberg native sinks do not support an upsert materializer");
      return null;
    }
    try {
      IcebergSinkPlan plan =
          IcebergSinkPlan.capture(
              (IcebergTableSink) sink.tableSink(),
              FlinkTypeFactory.toLogicalRowType(sink.getInput().getRowType()));
      if (plan.table.schema().identifierFieldIds().isEmpty()
          && !ChangelogPlanUtils.isInsertOnly((StreamPhysicalRel) sink.getInput())) {
        context.decline("Updating Iceberg input requires table identifier fields");
        return null;
      }
      return new StreamPhysicalNativeIcebergSink(
          sink.getCluster(), sink.getTraitSet(), sink.getInput(), sink.getRowType(), plan);
    } catch (IllegalArgumentException | LinkageError e) {
      context.decline("Iceberg sink: " + e.getMessage());
      return null;
    }
  }
}
