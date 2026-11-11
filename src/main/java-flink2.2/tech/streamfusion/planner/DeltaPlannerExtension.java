package tech.streamfusion.planner;

import java.util.List;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalSink;
import tech.streamfusion.planner.common.StreamRule;

/** Delta sink rewrite contributed by streamfusion-delta. */
public final class DeltaPlannerExtension implements NativePlannerExtension {
  @Override
  public void addSubstitutions(List<StreamRule<?>> substitutions) {
    substitutions.add(
        StreamRule.of(StreamPhysicalSink.class, DeltaSinkMatcher::substitute)
            .matching(DeltaSinkMatcher::appliesTo)
            .changelogSafe());
  }
}
