package tech.streamfusion.planner;

import java.util.List;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalSink;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalTableSourceScan;

/** Optional Fluss transport integration; primary-key production always stays on Flink. */
public final class FlussPlannerExtension implements NativePlannerExtension {
  @Override
  public void addSubstitutions(List<Substitution<?>> entries) {
    entries.add(
        Substitution.of(StreamPhysicalTableSourceScan.class, "flussSource", FlussTables::source)
            .matching(scan -> enabled() && FlussTables.isSource(scan))
            .changelogSafe());
    entries.add(
        Substitution.of(StreamPhysicalSink.class, "flussSink", FlussTables::sink)
            .matching(sink -> enabled() && FlussTables.isSink(sink))
            .changelogSafe());
  }

  private static boolean enabled() {
    return Boolean.parseBoolean(System.getProperty("streamfusion.fluss.enabled", "true"));
  }

  @Override
  public String sourceSharingKey(org.apache.calcite.rel.RelNode node) {
    if (enabled()
        && node instanceof StreamPhysicalTableSourceScan scan
        && FlussTables.isSource(scan)) return FlussTables.sourceSharingKey(scan);
    return null;
  }
}
