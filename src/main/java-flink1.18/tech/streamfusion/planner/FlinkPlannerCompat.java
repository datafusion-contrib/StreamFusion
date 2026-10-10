package tech.streamfusion.planner;

import java.util.List;
import org.apache.calcite.rel.RelNode;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory$;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalDeduplicate;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalRel;
import org.apache.flink.table.planner.plan.utils.ChangelogPlanUtils;
import tech.streamfusion.operator.RowDataArrowConverter;

/** Flink 1.18 uses a dedicated physical node instead of a rank-1 node. */
final class FlinkPlannerCompat {
  private FlinkPlannerCompat() {}

  static List<RelNode> prepareTimestampComparisons(List<RelNode> roots) {
    return TimestampComparisonCoercion.normalize(roots);
  }

  static RelNode prepareForRewrite(RelNode node) {
    if (node
        instanceof
        org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalLegacySink<?>) {
      org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalLegacySink<?> sink =
          ((org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalLegacySink<?>)
              node);

      return PreparedLegacySink.prepare(sink);
    }
    if (node
        instanceof
        org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalWindowDeduplicate) {
      org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalWindowDeduplicate
          dedup =
              ((org.apache.flink.table.planner.plan.nodes.physical.stream
                      .StreamPhysicalWindowDeduplicate)
                  node);

      Boolean keepLast = WindowDeduplicateMatcher.keepLastRow(dedup);
      if (keepLast != null)
        return tech.streamfusion.compat.FlinkCompat.prepareWindowDeduplicate(dedup, keepLast);
    }
    return node;
  }

  static void addSubstitutions(List<Substitution<?>> entries) {
    entries.add(
        Substitution.of(
                StreamPhysicalDeduplicate.class, "deduplicate", FlinkPlannerCompat::deduplicate)
            .keyedState()
            .matching(
                node ->
                    ChangelogPlanUtils.isInsertOnly((StreamPhysicalRel) node.getInput())
                        && RowDataArrowConverter.supports(
                            FlinkTypeFactory$.MODULE$.toLogicalRowType(node.getRowType()))
                        && (!node.isRowtime() || rowtimeColumn(node) >= 0))
            .reason(
                node ->
                    "deduplication: needs insert-only input, supported Arrow columns and a rowtime"
                        + " attribute for event-time ordering")
            .changelogSafe());
  }

  private static int rowtimeColumn(StreamPhysicalDeduplicate node) {
    var fields = node.getInput().getRowType().getFieldList();
    for (int i = 0; i < fields.size(); i++) {
      if (FlinkTypeFactory$.MODULE$.isRowtimeIndicatorType(fields.get(i).getType())) return i;
    }
    return -1;
  }

  private static RelNode deduplicate(StreamPhysicalDeduplicate node, PlanContext context) {
    int[] keys = node.getUniqueKeys();
    return new StreamPhysicalNativeDeduplicate(
        node.getCluster(),
        node.getTraitSet(),
        context.columnarInput(node.getInput(), keys),
        node.getRowType(),
        keys,
        rowtimeColumn(node),
        node.keepLastRow(),
        ChangelogPlanUtils.generateUpdateBefore(node),
        !node.isRowtime());
  }
}
