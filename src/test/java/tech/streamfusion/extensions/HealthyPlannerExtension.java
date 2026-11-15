package tech.streamfusion.extensions;

import java.util.List;
import org.apache.calcite.rel.RelNode;
import tech.streamfusion.planner.NativePlannerExtension;
import tech.streamfusion.planner.common.StreamRule;

/** Verifies that optional rules can be contributed from outside the planner implementation. */
public final class HealthyPlannerExtension implements NativePlannerExtension {
  public static int sharingCalls;

  @Override
  public void addSubstitutions(List<StreamRule<?>> entries) {
    entries.add(StreamRule.of(RelNode.class, (node, context) -> node).matching(node -> false));
  }

  @Override
  public String sourceSharingKey(RelNode node) {
    sharingCalls++;
    return null;
  }
}
