package io.github.jordepic.streamfusion.planner;

import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalUnion;

/**
 * Recognizes a {@link StreamPhysicalUnion} the native union reproduces: a UNION ALL whose row type
 * the Arrow conversion can carry. A union is a pure stream merge — it forwards every input record
 * unchanged (no per-row work, no shuffle), so it neither needs an operator nor touches the
 * {@code $row_kind$} tag, making it changelog-transparent. The host only ever plans UNION ALL at the
 * physical stream level (UNION distinct is rewritten to a GROUP BY), but the {@code all} flag is
 * checked defensively.
 */
final class UnionMatcher {

  private UnionMatcher() {}

  static boolean matches(StreamPhysicalUnion union) {
    return union.all && FilterCalcMatcher.convertibleRow(union.getRowType());
  }

  static String unsupportedReason(StreamPhysicalUnion union) {
    if (!union.all) {
      return "union: only UNION ALL is supported (UNION distinct rewrites to a GROUP BY)";
    }
    return "union: needs a row type the Arrow conversion supports";
  }
}
