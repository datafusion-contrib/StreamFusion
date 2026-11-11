package tech.streamfusion.planner;

import java.util.List;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalSink;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalTableSourceScan;
import tech.streamfusion.planner.common.StreamRule;

/** Streaming Paimon source and sink rewrites contributed by the optional Paimon module. */
public final class PaimonPlannerExtension implements NativePlannerExtension {
  @Override
  public void addSubstitutions(List<StreamRule<?>> substitutions) {
    substitutions.add(
        StreamRule.of(
                StreamPhysicalTableSourceScan.class,
                "paimonSource",
                PaimonSourceMatcher::substitute)
            .matching(PaimonSourceMatcher::appliesTo)
            .changelogSafe());
    substitutions.add(
        StreamRule.of(StreamPhysicalSink.class, PaimonSinkMatcher::substitute)
            .matching(PaimonSinkMatcher::appliesTo)
            .changelogSafe());
  }
}
