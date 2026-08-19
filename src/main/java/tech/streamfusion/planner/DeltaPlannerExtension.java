package tech.streamfusion.planner;

import java.util.List;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalSink;

/** Delta sink rewrite contributed by streamfusion-delta. */
public final class DeltaPlannerExtension implements NativePlannerExtension {
  @Override
  public void addSubstitutions(List<Substitution<?>> substitutions) {
    substitutions.add(
        Substitution.of(StreamPhysicalSink.class, DeltaSinkMatcher::substitute)
            .matching(DeltaSinkMatcher::appliesTo)
            .changelogSafe());
  }
}
