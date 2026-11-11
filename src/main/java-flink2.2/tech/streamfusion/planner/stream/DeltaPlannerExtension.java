package tech.streamfusion.planner.stream;

import java.util.List;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalSink;
import tech.streamfusion.planner.NativePlannerExtension;
import tech.streamfusion.planner.common.StreamRule;

/** Delta sink rewrite contributed by streamfusion-delta. */
public class DeltaPlannerExtension implements NativePlannerExtension {
  @Override
  public void addSubstitutions(List<StreamRule<?>> substitutions) {
    substitutions.add(
        StreamRule.of(StreamPhysicalSink.class, DeltaSinkMatcher::substitute)
            .matching(DeltaSinkMatcher::appliesTo)
            .changelogSafe());
  }
}
