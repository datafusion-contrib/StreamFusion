package tech.streamfusion.planner;

import java.util.List;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalSink;

/** Optional Iceberg rewrites retain the host's streaming commit topology. */
public final class IcebergPlannerExtension implements NativePlannerExtension {
  @Override
  public void addSubstitutions(List<Substitution<?>> substitutions) {
    substitutions.add(
        Substitution.of(
                org.apache.flink.table.planner.plan.nodes.physical.stream
                    .StreamPhysicalTableSourceScan.class,
                "icebergSource",
                IcebergSourceMatcher::substitute)
            .matching(IcebergSourceMatcher::appliesTo));
    substitutions.add(
        Substitution.of(StreamPhysicalSink.class, "icebergSink", IcebergSinkMatcher::substitute)
            .matching(IcebergSinkMatcher::appliesTo)
            .changelogSafe());
  }
}
