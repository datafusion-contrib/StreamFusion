package tech.streamfusion.planner.stream;

import java.util.ArrayList;
import java.util.TreeMap;
import org.apache.calcite.rel.RelNode;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory$;
import org.apache.flink.table.planner.plan.abilities.source.FilterPushDownSpec;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalTableSourceScan;
import org.apache.flink.table.planner.plan.schema.TableSourceTable;
import org.apache.flink.table.planner.plan.utils.FlinkRelOptUtil;
import org.apache.flink.table.types.logical.RowType;
import org.apache.paimon.flink.LogicalTypeConversion;
import org.apache.paimon.flink.source.DataTableSource;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
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
    String reason =
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
        partitionPredicate(scan, (FileStoreTable) source.getTable()),
        FlinkRelOptUtil.getDigest(scan)
            + "|"
            + scan.getRowType().getFullTypeString()
            + "|"
            + new TreeMap<>(source.getTable().options()),
        NativePlanDetails.from(scan));
  }

  private static Predicate partitionPredicate(
      StreamPhysicalTableSourceScan scan, FileStoreTable table) {
    RowType input = (RowType) LogicalTypeConversion.toLogicalType(table.rowType());
    var predicates = new ArrayList<Predicate>();
    for (var ability : scan.getTable().unwrap(TableSourceTable.class).abilitySpecs()) {
      if (ability instanceof FilterPushDownSpec) {
        for (var expression : ((FilterPushDownSpec) ability).getPredicates()) {
          Predicate predicate =
              PaimonPartitionPredicate.of(
                  expression, input, table.rowType(), table.partitionKeys());
          if (predicate != null) predicates.add(predicate);
        }
      }
      // Ability expressions refer to the schema at that point in the ordered pushdown chain.
      if (ability.getProducedType().isPresent()) input = ability.getProducedType().get();
    }
    return predicates.isEmpty() ? null : PredicateBuilder.and(predicates);
  }
}
