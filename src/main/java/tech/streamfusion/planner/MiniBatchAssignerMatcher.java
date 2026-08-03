package tech.streamfusion.planner;

import org.apache.calcite.rel.RelNode;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalMiniBatchAssigner;
import org.apache.flink.table.planner.plan.trait.MiniBatchInterval;
import org.apache.flink.table.planner.plan.trait.MiniBatchIntervalTraitDef$;
import org.apache.flink.table.planner.plan.trait.MiniBatchMode;

/**
 * The mini-batch assigner needs no shape test — every assigner is a candidate — so unlike its
 * siblings it has only a substitution, which reports the one mode the native marker cannot
 * reproduce.
 */
final class MiniBatchAssignerMatcher {

  private MiniBatchAssignerMatcher() {}

  static RelNode substitute(StreamPhysicalMiniBatchAssigner assigner, PlanContext ctx) {
    MiniBatchInterval interval =
        assigner
            .getTraitSet()
            .getTrait(MiniBatchIntervalTraitDef$.MODULE$.INSTANCE())
            .getMiniBatchInterval();
    if (interval.getMode() != MiniBatchMode.ProcTime
        && interval.getMode() != MiniBatchMode.RowTime) {
      ctx.decline("miniBatchAssigner: unsupported mini-batch mode " + interval.getMode());
      return null;
    }
    if (!NativeConfig.operatorEnabled("miniBatchAssigner")) {
      ctx.decline(Substitution.disabledReason("miniBatchAssigner"));
      return null;
    }
    return new StreamPhysicalNativeMiniBatchAssigner(
        assigner.getCluster(),
        assigner.getTraitSet(),
        assigner.getInputs().get(0),
        assigner.getRowType(),
        interval.getInterval(),
        interval.getMode() == MiniBatchMode.RowTime);
  }
}
