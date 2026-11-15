package tech.streamfusion.planner.stream;

import org.apache.calcite.rel.RelNode;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory$;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalChangelogNormalize;
import org.apache.flink.table.planner.plan.utils.ChangelogPlanUtils;
import tech.streamfusion.operator.RowDataArrowConverter;

/**
 * Recognizes the changelog normalization the native operator reproduces: a {@link
 * StreamPhysicalChangelogNormalize} keyed by a unique key, with no filter condition and not part of a
 * source-reuse rewrite. The native operator keeps the last full row per key and emits Flink's
 * normalized changelog (a faithful port of {@code ProcTimeDeduplicateKeepLastRowFunction}'s
 * keep-last-on-changelog path), so an upsert (e.g. upsert-kafka) or duplicate-bearing changelog
 * becomes a regular INSERT/UPDATE_BEFORE/UPDATE_AFTER/DELETE stream. A pushed filter condition, the
 * source-reuse variant, or a row type the Arrow conversion cannot carry all fall back.
 */
final class ChangelogNormalizeMatcher {

  private ChangelogNormalizeMatcher() {}

  static boolean matches(StreamPhysicalChangelogNormalize node) {
    if (tech.streamfusion.compat.FlinkCompat.normalizeHasFilter(node)) {
      return false; // a pushed filter condition is not yet reproduced
    }
    if (tech.streamfusion.compat.FlinkCompat.normalizeSharesSource(node)) {
      return false; // the source-reuse rewrite changes the operator's contract
    }
    return RowDataArrowConverter.supports(
        FlinkTypeFactory$.MODULE$.toLogicalRowType(node.getRowType()));
  }

  static int[] keyColumns(StreamPhysicalChangelogNormalize node) {
    return node.uniqueKeys();
  }

  static boolean generateUpdateBefore(StreamPhysicalChangelogNormalize node) {
    return ChangelogPlanUtils.generateUpdateBefore(node);
  }

  static String unsupportedReason(StreamPhysicalChangelogNormalize node) {
    if (tech.streamfusion.compat.FlinkCompat.normalizeHasFilter(node)) {
      return "changelog normalize: a pushed filter condition is not supported";
    }
    if (tech.streamfusion.compat.FlinkCompat.normalizeSharesSource(node)) {
      return "changelog normalize: the source-reuse variant is not supported";
    }
    return "changelog normalize: needs a row type the Arrow conversion supports";
  }

  static RelNode substitute(StreamPhysicalChangelogNormalize normalize, PlanContext ctx) {
    int[] keyColumns = ChangelogNormalizeMatcher.keyColumns(normalize);
    return new StreamPhysicalNativeChangelogNormalize(
        normalize.getCluster(),
        normalize.getTraitSet(),
        ctx.columnarInput(normalize.getInputs().get(0), keyColumns),
        normalize.getRowType(),
        keyColumns,
        ChangelogNormalizeMatcher.generateUpdateBefore(normalize));
  }
}
