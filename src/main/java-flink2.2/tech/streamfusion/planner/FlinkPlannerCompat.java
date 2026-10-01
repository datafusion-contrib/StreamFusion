package tech.streamfusion.planner;

import java.util.List;

final class FlinkPlannerCompat {
  private FlinkPlannerCompat() {}

  static List<org.apache.calcite.rel.RelNode> prepareTimestampComparisons(
      List<org.apache.calcite.rel.RelNode> roots) {
    return roots;
  }

  static org.apache.calcite.rel.RelNode prepareForRewrite(org.apache.calcite.rel.RelNode node) {
    return node;
  }

  static void addSubstitutions(List<Substitution<?>> entries) {}
}
