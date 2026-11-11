package tech.streamfusion.planner.stream;

import java.util.List;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalTableSourceScan;
import tech.streamfusion.planner.NativePlannerExtension;
import tech.streamfusion.planner.common.StreamRule;

/** Optional Hudi admission leaves unsupported contracts with the released connector. */
public class HudiPlannerExtension implements NativePlannerExtension {
  @Override
  public void addSubstitutions(List<StreamRule<?>> substitutions) {
    substitutions.add(
        StreamRule.of(
                org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalSink.class,
                "hudiSink",
                HudiSinkMatcher::substitute)
            .matching(HudiSinkMatcher::appliesTo)
            .changelogSafe());
    substitutions.add(
        StreamRule.of(
                StreamPhysicalTableSourceScan.class, "hudiSource", HudiSourceMatcher::substitute)
            .matching(HudiSourceMatcher::appliesTo)
            .changelogSafe());
  }
}
