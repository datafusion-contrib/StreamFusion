package tech.streamfusion.planner;

import java.util.List;
import java.util.Map;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalTableSourceScan;

/** Fluss source rewrite contributed only by streamfusion-fluss. */
public final class FlussPlannerExtension implements NativePlannerExtension {

  @Override
  public void addSubstitutions(List<Substitution<?>> entries) {
    entries.add(
        Substitution.of(StreamPhysicalTableSourceScan.class, FlussTables::substitute)
            .matching(
                scan -> {
                  Map<String, String> options = FilesystemTables.options(scan);
                  boolean connectorOption =
                      options != null && "fluss".equals(options.get("connector"));
                  return (connectorOption || FlussTables.isFlussTableSource(scan))
                      && NativeConfig.operatorEnabled("flussSource");
                })
            .yieldingOnDecline());
  }
}
