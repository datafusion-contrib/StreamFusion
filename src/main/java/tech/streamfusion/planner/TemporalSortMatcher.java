package tech.streamfusion.planner;

import java.util.List;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalTemporalSort;

/**
 * Recognizes the event-time sort the native sorter implements: a {@link StreamPhysicalTemporalSort}
 * ordering by a single time attribute ascending (`ORDER BY rowtime`). The host only produces this
 * node when the sole order key is an event-time attribute, so the sort is watermark-driven; the
 * native operator buffers rows and releases them in rowtime order as the watermark advances. A
 * secondary order key, a descending order, or input columns the row/Arrow conversion can't carry all
 * fall back.
 */
final class TemporalSortMatcher {

  private TemporalSortMatcher() {}

  static boolean matches(StreamPhysicalTemporalSort sort) {
    List<RelFieldCollation> collations = sort.getCollation().getFieldCollations();
    if (collations.size() != 1) {
      return false; // only a single time-attribute order key
    }
    RelFieldCollation collation = collations.get(0);
    if (collation.getDirection().isDescending()) {
      return false; // event-time sort is ascending (watermark-driven, forward in time)
    }
    RelDataType rowtime = sort.getInput().getRowType().getFieldList().get(collation.getFieldIndex()).getType();
    switch (rowtime.getSqlTypeName()) {
      case TIMESTAMP:
      case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
        break;
      default:
        return false; // the order key must be a timestamp (event-time) attribute
    }
    return FilterCalcMatcher.convertibleRow(sort.getInput().getRowType());
  }

  static int rowtimeColumn(StreamPhysicalTemporalSort sort) {
    return sort.getCollation().getFieldCollations().get(0).getFieldIndex();
  }

  static String unsupportedReason(StreamPhysicalTemporalSort sort) {
    return "event-time sort: needs ORDER BY a single time attribute ascending, with input columns the"
        + " Arrow conversion supports";
  }

  static RelNode substitute(StreamPhysicalTemporalSort sort, PlanContext ctx) {
    return new StreamPhysicalNativeTemporalSort(
        sort.getCluster(),
        sort.getTraitSet(),
        ctx.columnarInput(sort.getInputs().get(0), new int[0]),
        sort.getRowType(),
        TemporalSortMatcher.rowtimeColumn(sort));
  }
}
