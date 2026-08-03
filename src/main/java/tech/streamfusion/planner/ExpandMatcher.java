package tech.streamfusion.planner;

import tech.streamfusion.operator.RowDataArrowConverter;
import java.util.List;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory$;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalExpand;

/**
 * Recognizes the GROUPING SETS / CUBE / ROLLUP expansion the native operator reproduces: a {@link
 * StreamPhysicalExpand} whose every project cell is an {@link RexInputRef} (copy a column), a NULL
 * literal (a grouped-out key), or — at the expand-id column ({@code expandIdIndex}) — a non-null
 * integer literal (the per-set grouping id). The host always plans these expansions this way, so the
 * native fan-out (copy / typed-null / stamped id, per grouping set) matches Flink's {@code
 * ExpandFunction} row for row, and the downstream GROUP BY over the keys plus the expand-id then
 * aggregates each set. A project cell that is any other expression (a computed column) falls back.
 */
final class ExpandMatcher {

  private ExpandMatcher() {}

  static boolean matches(StreamPhysicalExpand expand) {
    int idIdx = expand.expandIdIndex();
    for (List<RexNode> project : expand.projects()) {
      for (int c = 0; c < project.size(); c++) {
        RexNode cell = project.get(c);
        if (c == idIdx) {
          if (!(cell instanceof RexLiteral) || ((RexLiteral) cell).getValue() == null) {
            return false; // the expand id must be a non-null integer literal
          }
        } else if (cell instanceof RexInputRef) {
          // a copied column — fine
        } else if (cell instanceof RexLiteral && ((RexLiteral) cell).getValue() == null) {
          // a grouped-out key (typed NULL) — fine
        } else {
          return false; // any other expression (a computed cell) is not reproduced
        }
      }
    }
    return RowDataArrowConverter.supports(
        FlinkTypeFactory$.MODULE$.toLogicalRowType(expand.getRowType()));
  }

  /** Flattened {@code [expandRow][outputCol]} copy map: input column index, or -1 for a literal. */
  static int[] copyIndices(StreamPhysicalExpand expand) {
    List<List<RexNode>> projects = expand.projects();
    int cols = projects.get(0).size();
    int[] out = new int[projects.size() * cols];
    for (int r = 0; r < projects.size(); r++) {
      for (int c = 0; c < cols; c++) {
        RexNode cell = projects.get(r).get(c);
        out[r * cols + c] = cell instanceof RexInputRef ? ((RexInputRef) cell).getIndex() : -1;
      }
    }
    return out;
  }

  /** The grouping id literal for each expand row, in order. */
  static long[] expandIdValues(StreamPhysicalExpand expand) {
    List<List<RexNode>> projects = expand.projects();
    int idIdx = expand.expandIdIndex();
    long[] out = new long[projects.size()];
    for (int r = 0; r < projects.size(); r++) {
      out[r] = ((RexLiteral) projects.get(r).get(idIdx)).getValueAs(Long.class);
    }
    return out;
  }

  static int numExpandRows(StreamPhysicalExpand expand) {
    return expand.projects().size();
  }

  static int numOutputColumns(StreamPhysicalExpand expand) {
    return expand.projects().get(0).size();
  }

  /** Whether the expand-id column is BIGINT (Int64) rather than INT (Int32). */
  static boolean expandIdIsLong(StreamPhysicalExpand expand) {
    return expand.getRowType().getFieldList().get(expand.expandIdIndex()).getType().getSqlTypeName()
        == SqlTypeName.BIGINT;
  }

  static String unsupportedReason(StreamPhysicalExpand expand) {
    return "expand: every GROUPING SETS/CUBE/ROLLUP project cell must be a column reference, a NULL,"
        + " or the integer expand id (a computed cell falls back), over Arrow-supported column types";
  }

  static RelNode substitute(StreamPhysicalExpand expand, PlanContext ctx) {
    return new StreamPhysicalNativeExpand(
        expand.getCluster(),
        expand.getTraitSet(),
        expand.getInputs().get(0),
        expand.getRowType(),
        ExpandMatcher.numExpandRows(expand),
        ExpandMatcher.numOutputColumns(expand),
        expand.expandIdIndex(),
        ExpandMatcher.expandIdIsLong(expand),
        ExpandMatcher.copyIndices(expand),
        ExpandMatcher.expandIdValues(expand));
  }
}
