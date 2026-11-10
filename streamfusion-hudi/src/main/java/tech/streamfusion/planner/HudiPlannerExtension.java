package tech.streamfusion.planner;

import java.util.List;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalTableSourceScan;

/** Optional Hudi admission leaves unsupported contracts with the released connector. */
public final class HudiPlannerExtension implements NativePlannerExtension {
  @Override
  public void addSubstitutions(List<Substitution<?>> substitutions) {
    substitutions.add(
        Substitution.of(
                StreamPhysicalTableSourceScan.class, "hudiSource", HudiSourceMatcher::substitute)
            .matching(HudiSourceMatcher::appliesTo)
            .changelogSafe());
  }
}
