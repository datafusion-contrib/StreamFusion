package io.github.jordepic.streamfusion.planner;

import io.github.jordepic.streamfusion.operator.RowDataArrowConverter;
import java.lang.reflect.Field;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory$;
import org.apache.flink.table.planner.plan.logical.WindowAttachedWindowingStrategy;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalWindowDeduplicate;

/**
 * Recognizes window deduplication — {@code ROW_NUMBER() OVER (PARTITION BY window, key ORDER BY
 * rowtime) = 1} over a windowing-TVF input, planned as a {@link StreamPhysicalWindowDeduplicate}.
 * It is the {@code limit = 1} case of window Top-N: per window and key keep the first (keep-first,
 * rowtime ascending) or last (keep-last, descending) row, emitted when a watermark closes the
 * window. Both are append-only, so both route; it reuses the window-rank operator with a single
 * rowtime sort column and {@code limit = 1}.
 *
 * <p>The keep-first/keep-last direction is the node's {@code keepLastRow} flag, which has no public
 * accessor; it is read reflectively. If that ever fails (a Flink internals change), the matcher
 * declines and the query falls back cleanly rather than risking a wrong direction.
 */
final class WindowDeduplicateMatcher {

  private WindowDeduplicateMatcher() {}

  static boolean matches(StreamPhysicalWindowDeduplicate dedup) {
    if (!(dedup.getWindowingStrategy() instanceof WindowAttachedWindowingStrategy)) {
      return false; // the window must be attached as columns (the windowing-TVF output)
    }
    if (keepLastRow(dedup) == null) {
      return false; // cannot determine keep-first/keep-last → fall back
    }
    return RowDataArrowConverter.supports(
        FlinkTypeFactory$.MODULE$.toLogicalRowType(dedup.getRowType()));
  }

  private static WindowAttachedWindowingStrategy windowing(StreamPhysicalWindowDeduplicate dedup) {
    return (WindowAttachedWindowingStrategy) dedup.getWindowingStrategy();
  }

  static int windowStartColumn(StreamPhysicalWindowDeduplicate dedup) {
    return windowing(dedup).getWindowStart();
  }

  static int windowEndColumn(StreamPhysicalWindowDeduplicate dedup) {
    return windowing(dedup).getWindowEnd();
  }

  static int[] partitionColumns(StreamPhysicalWindowDeduplicate dedup) {
    return dedup.partitionKeys();
  }

  /** The single order column: the rowtime, ascending for keep-first and descending for keep-last. */
  static int[] sortIndices(StreamPhysicalWindowDeduplicate dedup) {
    return new int[] {dedup.orderKey()};
  }

  static int[] sortAscending(StreamPhysicalWindowDeduplicate dedup) {
    return new int[] {Boolean.TRUE.equals(keepLastRow(dedup)) ? 0 : 1};
  }

  static int[] sortNullsFirst(StreamPhysicalWindowDeduplicate dedup) {
    return new int[] {0}; // the rowtime order key is never null
  }

  static boolean isProctime(StreamPhysicalWindowDeduplicate dedup) {
    return windowing(dedup).isProctime();
  }

  static long windowMillis(StreamPhysicalWindowDeduplicate dedup) {
    return WindowAggregateMatcher.windowSize(windowing(dedup));
  }

  static long slideMillis(StreamPhysicalWindowDeduplicate dedup) {
    return WindowAggregateMatcher.windowSlide(windowing(dedup));
  }

  static boolean cumulative(StreamPhysicalWindowDeduplicate dedup) {
    return WindowAggregateMatcher.isCumulative(windowing(dedup));
  }

  /** Reads the node's private {@code keepLastRow} flag; null if it cannot be read. */
  static Boolean keepLastRow(StreamPhysicalWindowDeduplicate dedup) {
    try {
      Field field = StreamPhysicalWindowDeduplicate.class.getDeclaredField("keepLastRow");
      field.setAccessible(true);
      return field.getBoolean(dedup);
    } catch (ReflectiveOperationException e) {
      return null;
    }
  }

  static String unsupportedReason(StreamPhysicalWindowDeduplicate dedup) {
    return "window deduplication: needs ROW_NUMBER() OVER (PARTITION BY window, key ORDER BY rowtime)"
        + " = 1 over a windowing-TVF input, with input columns the Arrow conversion supports";
  }
}
