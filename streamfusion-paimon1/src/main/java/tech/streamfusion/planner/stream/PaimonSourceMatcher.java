package tech.streamfusion.planner.stream;

import java.util.TreeMap;
import org.apache.calcite.rel.RelNode;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory$;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalTableSourceScan;
import org.apache.flink.table.planner.plan.schema.TableSourceTable;
import org.apache.flink.table.planner.plan.utils.FlinkRelOptUtil;
import org.apache.paimon.flink.source.DataTableSource;
import org.apache.paimon.table.FileStoreTable;
import tech.streamfusion.planner.common.NativePlanDetails;
import tech.streamfusion.planner.common.StreamPlanningContext;

/** Admission for the hybrid streaming source; all pushed predicates remain Flink residuals. */
final class PaimonSourceMatcher {
  static boolean appliesTo(StreamPhysicalTableSourceScan scan) {
    TableSourceTable resolved = scan.getTable().unwrap(TableSourceTable.class);
    return resolved != null && resolved.tableSource() instanceof DataTableSource;
  }

  static String fallback(StreamPhysicalTableSourceScan scan) {
    var reason =
        tech.streamfusion.planner.common.PaimonSourceAdmission.fallback(
            scan.getTable().unwrap(TableSourceTable.class),
            FlinkTypeFactory$.MODULE$.toLogicalRowType(scan.getRowType()),
            true);
    if (reason != null) return reason;
    return ScanWatermarkSpec.of(scan) == ScanWatermarkSpec.UNSUPPORTED
        ? "unsupported source watermark"
        : null;
  }

  static RelNode substitute(StreamPhysicalTableSourceScan scan, StreamPlanningContext context) {
    String reason = fallback(scan);
    if (reason != null) {
      context.decline("Paimon source: " + reason);
      return null;
    }
    var source = (DataTableSource) scan.getTable().unwrap(TableSourceTable.class).tableSource();
    return new StreamPhysicalNativePaimonSource(
        scan.getCluster(),
        scan.getTraitSet(),
        scan.getRowType(),
        (FileStoreTable) source.getTable(),
        ScanWatermarkSpec.of(scan),
        null,
        FlinkRelOptUtil.getDigest(scan)
            + "|"
            + scan.getRowType().getFullTypeString()
            + "|"
            + new TreeMap<>(source.getTable().options()),
        NativePlanDetails.from(scan));
  }
}
