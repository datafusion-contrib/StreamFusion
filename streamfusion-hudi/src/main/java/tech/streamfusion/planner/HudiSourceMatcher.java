package tech.streamfusion.planner;

import java.util.Set;
import org.apache.calcite.rel.RelNode;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory$;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalTableSourceScan;
import org.apache.flink.table.planner.plan.schema.TableSourceTable;
import org.apache.hudi.table.HoodieTableSource;
import tech.streamfusion.hudi.NativeHudiSource;

final class HudiSourceMatcher {
  static boolean appliesTo(StreamPhysicalTableSourceScan scan) {
    var table = scan.getTable().unwrap(TableSourceTable.class);
    return table != null && table.tableSource() instanceof HoodieTableSource;
  }

  static RelNode substitute(StreamPhysicalTableSourceScan scan, PlanContext context) {
    var table = scan.getTable().unwrap(TableSourceTable.class);
    for (var ability : table.abilitySpecs()) {
      if (ability
              instanceof org.apache.flink.table.planner.plan.abilities.source.ReadingMetadataSpec
          && ((org.apache.flink.table.planner.plan.abilities.source.ReadingMetadataSpec) ability)
              .getMetadataKeys()
              .isEmpty()) continue;
      if (!Set.of("ProjectPushDownSpec").contains(ability.getClass().getSimpleName())) {
        context.decline(
            "Hudi source ability is not admitted: " + ability.getClass().getSimpleName());
        return null;
      }
    }
    var source = (HoodieTableSource) table.tableSource();
    NativeHudiSource.Plan plan;
    try {
      plan =
          NativeHudiSource.plan(
              source, FlinkTypeFactory$.MODULE$.toLogicalRowType(scan.getRowType()));
    } catch (IllegalArgumentException | LinkageError failure) {
      context.decline("Hudi source: " + failure.getMessage());
      return null;
    }
    return new StreamPhysicalNativeHudiSource(
        scan.getCluster(),
        scan.getTraitSet(),
        scan.getRowType(),
        plan,
        NativePlanDetails.from(scan));
  }
}
