package tech.streamfusion.planner;

import java.util.Set;
import org.apache.calcite.rel.RelNode;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalTableSourceScan;
import org.apache.flink.table.planner.plan.schema.TableSourceTable;
import org.apache.iceberg.flink.source.IcebergTableSource;
import tech.streamfusion.iceberg.IcebergSourcePlan;

final class IcebergSourceMatcher {
  private IcebergSourceMatcher() {}

  static boolean appliesTo(StreamPhysicalTableSourceScan scan) {
    var table = scan.getTable().unwrap(TableSourceTable.class);
    return table != null && table.tableSource() instanceof IcebergTableSource;
  }

  static RelNode substitute(StreamPhysicalTableSourceScan scan, PlanContext context) {
    if (!NativeConfig.operatorEnabled("icebergSource")) {
      context.decline(Substitution.disabledReason("icebergSource"));
      return null;
    }
    var table = scan.getTable().unwrap(TableSourceTable.class);
    if (!table.isStreamingMode()) {
      context.decline("Iceberg reads support streaming jobs only");
      return null;
    }
    for (var ability : table.abilitySpecs()) {
      if (!Set.of("ProjectPushDownSpec", "FilterPushDownSpec", "SourceWatermarkSpec")
          .contains(ability.getClass().getSimpleName())) {
        context.decline(
            "Iceberg source ability is not admitted: " + ability.getClass().getSimpleName());
        return null;
      }
    }
    try {
      var plan =
          IcebergSourcePlan.capture(
              (IcebergTableSource) table.tableSource(),
              FlinkTypeFactory.toLogicalRowType(scan.getRowType()));
      return new StreamPhysicalNativeIcebergSource(
          scan.getCluster(), scan.getTraitSet(), scan.getRowType(), plan);
    } catch (IllegalArgumentException | LinkageError e) {
      context.decline("Iceberg source: " + e.getMessage());
      return null;
    }
  }
}
