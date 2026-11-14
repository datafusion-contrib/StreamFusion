package tech.streamfusion.planner.stream;

import java.util.List;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalGroupAggregate;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalLegacySink;

final class FlinkPlannerCompat {
  private FlinkPlannerCompat() {}

  static List<RelNode> prepareTimestampComparisons(List<RelNode> roots) {
    return roots;
  }

  static RelNode prepareForRewrite(RelNode node) {
    if (node instanceof StreamPhysicalLegacySink<?>
        && node.getInput(0) instanceof StreamPhysicalGroupAggregate) {
      var group = (StreamPhysicalGroupAggregate) node.getInput(0);
      if (JvmGroupAggregateMatcher.enabledAggregate(group)
          && JvmGroupAggregateMatcher.matches(group)
          && group.getRowType().getFieldList().stream()
              .anyMatch(
                  field ->
                      field.getType().getSqlTypeName() == SqlTypeName.MULTISET
                          || field.getType().getSqlTypeName() == SqlTypeName.ARRAY))
        return PreparedLegacySink.prepare((StreamPhysicalLegacySink<?>) node);
    }
    return node;
  }

  static boolean isLegacyRowSink(RelNode node) {
    return node instanceof PreparedLegacySink;
  }

  static void addSubstitutions(List<Substitution<?>> entries) {}
}
