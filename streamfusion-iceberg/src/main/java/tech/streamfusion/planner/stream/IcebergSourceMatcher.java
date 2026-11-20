package tech.streamfusion.planner.stream;

import java.util.Set;
import org.apache.calcite.rel.RelNode;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalTableSourceScan;
import org.apache.flink.table.planner.plan.schema.TableSourceTable;
import org.apache.iceberg.flink.source.IcebergTableSource;
import tech.streamfusion.iceberg.IcebergSourcePlan;
import tech.streamfusion.planner.NativeConfig;
import tech.streamfusion.planner.common.StreamPlanningContext;

final class IcebergSourceMatcher {
  private IcebergSourceMatcher() {}

  static boolean appliesTo(StreamPhysicalTableSourceScan scan) {
    var table = scan.getTable().unwrap(TableSourceTable.class);
    return table != null && table.tableSource() instanceof IcebergTableSource;
  }

  static RelNode substitute(StreamPhysicalTableSourceScan scan, StreamPlanningContext context) {
    if (!NativeConfig.operatorEnabled("icebergSource")) {
      context.decline(Substitution.disabledReason("icebergSource"));
      return null;
    }
    try {
      var plan = plan(scan, scan.getTable().unwrap(TableSourceTable.class), false);
      return new StreamPhysicalNativeIcebergSource(
          scan.getCluster(), scan.getTraitSet(), scan.getRowType(), plan);
    } catch (IllegalArgumentException | LinkageError e) {
      context.decline("Iceberg source: " + e.getMessage());
      return null;
    }
  }

  static IcebergSourcePlan plan(RelNode scan, TableSourceTable table, boolean bounded) {
    if (table.isStreamingMode() == bounded)
      throw new IllegalArgumentException("Iceberg scan mode must match the job mode");
    var abilities =
        bounded
            ? Set.of("ProjectPushDownSpec", "FilterPushDownSpec")
            : Set.of("ProjectPushDownSpec", "FilterPushDownSpec", "SourceWatermarkSpec");
    for (var ability : table.abilitySpecs()) {
      String name = ability.getClass().getSimpleName();
      if (!abilities.contains(name))
        throw new IllegalArgumentException("Iceberg source ability is not admitted: " + name);
    }
    return IcebergSourcePlan.capture(
        (IcebergTableSource) table.tableSource(),
        FlinkTypeFactory.toLogicalRowType(scan.getRowType()),
        bounded);
  }
}
