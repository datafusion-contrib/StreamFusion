package io.github.jordepic.streamfusion.planner;

import io.github.jordepic.streamfusion.operator.RowDataArrowConverter;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory$;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalChangelogNormalize;
import org.apache.flink.table.planner.plan.utils.ChangelogPlanUtils;

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
    if (node.filterCondition() != null) {
      return false; // a pushed filter condition is not yet reproduced
    }
    if (node.sourceReused() || node.commonFilter().length > 0) {
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
    if (node.filterCondition() != null) {
      return "changelog normalize: a pushed filter condition is not supported";
    }
    if (node.sourceReused() || node.commonFilter().length > 0) {
      return "changelog normalize: the source-reuse variant is not supported";
    }
    return "changelog normalize: needs a row type the Arrow conversion supports";
  }
}
