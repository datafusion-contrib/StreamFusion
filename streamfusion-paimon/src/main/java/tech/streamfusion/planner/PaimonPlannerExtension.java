package tech.streamfusion.planner;

import java.util.List;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalSink;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalTableSourceScan;

/** Streaming Paimon source and sink rewrites contributed by the optional Paimon module. */
public final class PaimonPlannerExtension implements NativePlannerExtension {
  @Override
  public void addSubstitutions(List<Substitution<?>> substitutions) {
    substitutions.add(
        Substitution.of(
                StreamPhysicalTableSourceScan.class,
                "paimonSource",
                PaimonSourceMatcher::substitute)
            .matching(PaimonSourceMatcher::appliesTo)
            .changelogSafe());
    substitutions.add(
        Substitution.of(StreamPhysicalSink.class, PaimonSinkMatcher::substitute)
            .matching(PaimonSinkMatcher::appliesTo)
            .changelogSafe());
  }
}
