package io.github.jordepic.streamfusion.planner;

import io.github.jordepic.streamfusion.operator.RowDataArrowConverter;
import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory$;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalGlobalGroupAggregate;
import org.apache.flink.table.planner.plan.utils.ChangelogPlanUtils;

/**
 * Recognizes the global half of a two-phase non-windowed {@code GROUP BY}: it merges the local
 * half's partials into the final per-key result, emitting a changelog exactly like the single-phase
 * {@link GroupAggregateMatcher} — so it reuses the same native group-aggregate operator. Its input
 * is the local's {@code [grouping.., partial0..]}; the partials are positional, walked with a
 * per-aggregate offset because an AVG spans <em>two</em> partial columns (the widened sum, then the
 * bigint count) where every other aggregate spans one.
 *
 * <p>Each merge folds a partial into the running result: SUM/MIN/MAX merge as themselves, a COUNT
 * merges by <em>summing</em> the partial counts (the global treats COUNT as a SUM over its bigint
 * partial column), and an AVG folds its pre-summed sum partial into the ordinary AVG state while the
 * count partial bumps the state's non-null count — so the final emit (divide, truncate toward zero,
 * cast back to the value type) is byte-identical to the single-phase AVG. Scope matches the local
 * half: bigint/int/double partials, and grouping keys the boundary carries.
 */
final class GlobalGroupAggregateMatcher {

  private GlobalGroupAggregateMatcher() {}

  static boolean matches(StreamPhysicalGlobalGroupAggregate agg) {
    return unsupportedReason(agg) == null;
  }

  static String unsupportedReason(StreamPhysicalGlobalGroupAggregate agg) {
    RelDataType inputType = agg.getInput().getRowType();
    if (!RowDataArrowConverter.supports(
        FlinkTypeFactory$.MODULE$.toLogicalRowType(agg.getRowType()))) {
      return "global group aggregate: an output column type the boundary cannot carry";
    }
    int[] grouping = agg.grouping();
    for (int column : grouping) {
      if (!WindowAggregateMatcher.supportedGroupingKeyType(
          inputType.getFieldList().get(column).getType().getSqlTypeName())) {
        return "global group aggregate: grouping keys must be bigint/int/string/boolean/date/"
            + "timestamp/decimal";
      }
    }
    int offset = grouping.length;
    for (int i = 0; i < agg.aggCalls().size(); i++) {
      AggregateCall call = agg.aggCalls().apply(i);
      int kind = WindowAggregateMatcher.aggregateKind(call.getAggregation().getKind());
      if (kind < 0 || call.getArgList().size() > 1) {
        return "global group aggregate: only single-field SUM/MIN/MAX/COUNT/AVG merges";
      }
      if (kind == WindowAggregateMatcher.KIND_AVG) {
        // AVG merges a (widened sum, count) partial pair into the final result column, whose type
        // must be one the AVG state's cast-back supports and must agree with the sum's width.
        if (offset + 1 >= inputType.getFieldCount()) {
          return "global group aggregate: AVG expects a (sum, count) partial pair";
        }
        SqlTypeName sumType = inputType.getFieldList().get(offset).getType().getSqlTypeName();
        SqlTypeName countType =
            inputType.getFieldList().get(offset + 1).getType().getSqlTypeName();
        SqlTypeName resultType =
            agg.getRowType().getFieldList().get(grouping.length + i).getType().getSqlTypeName();
        boolean intAvg =
            sumType == SqlTypeName.BIGINT
                && (resultType == SqlTypeName.BIGINT || resultType == SqlTypeName.INTEGER);
        boolean doubleAvg = sumType == SqlTypeName.DOUBLE && resultType == SqlTypeName.DOUBLE;
        if (!(intAvg || doubleAvg) || countType != SqlTypeName.BIGINT) {
          return "global group aggregate: AVG merge partials must be (bigint, bigint) for an"
              + " integer average or (double, bigint) for a double one";
        }
        offset += 2;
        continue;
      }
      SqlTypeName partialType = inputType.getFieldList().get(offset).getType().getSqlTypeName();
      offset++;
      if (partialCode(partialType) < 0) {
        return "global group aggregate: partial columns must be bigint/int/double";
      }
    }
    return null;
  }

  /** Native value-type code for a partial column, or -1 if unsupported. */
  private static int partialCode(SqlTypeName type) {
    switch (type) {
      case BIGINT:
        return 0;
      case DOUBLE:
        return 1;
      case INTEGER:
        return 2;
      default:
        return -1;
    }
  }

  static int[] keyColumns(StreamPhysicalGlobalGroupAggregate agg) {
    return agg.grouping();
  }

  /** The positional partial columns, one per aggregate — an AVG's is its sum partial. */
  static int[] valueColumns(StreamPhysicalGlobalGroupAggregate agg) {
    int[] columns = new int[agg.aggCalls().size()];
    int offset = agg.grouping().length;
    for (int i = 0; i < columns.length; i++) {
      columns[i] = offset;
      offset += spanOf(agg, i);
    }
    return columns;
  }

  /** Per-aggregate count-partial column for an AVG merge, -1 otherwise. */
  static int[] countColumns(StreamPhysicalGlobalGroupAggregate agg) {
    int[] columns = new int[agg.aggCalls().size()];
    int offset = agg.grouping().length;
    for (int i = 0; i < columns.length; i++) {
      columns[i] = spanOf(agg, i) == 2 ? offset + 1 : -1;
      offset += spanOf(agg, i);
    }
    return columns;
  }

  /**
   * Per-aggregate native value-type codes. A non-AVG merge is typed by its partial column; an AVG by
   * its FINAL result type (the state widens the sum itself and casts back on emit, exactly as the
   * single-phase AVG does).
   */
  static int[] valueTypeCodes(StreamPhysicalGlobalGroupAggregate agg) {
    RelDataType inputType = agg.getInput().getRowType();
    List<Integer> codes = new ArrayList<>();
    int grouping = agg.grouping().length;
    int offset = grouping;
    for (int i = 0; i < agg.aggCalls().size(); i++) {
      if (spanOf(agg, i) == 2) {
        SqlTypeName resultType =
            agg.getRowType().getFieldList().get(grouping + i).getType().getSqlTypeName();
        codes.add(partialCode(resultType));
      } else {
        codes.add(partialCode(inputType.getFieldList().get(offset).getType().getSqlTypeName()));
      }
      offset += spanOf(agg, i);
    }
    int[] array = new int[codes.size()];
    for (int i = 0; i < array.length; i++) {
      array[i] = codes.get(i);
    }
    return array;
  }

  /** Merge kinds: COUNT merges by summing its partial counts; AVG keeps the ordinary AVG state. */
  static int[] kinds(StreamPhysicalGlobalGroupAggregate agg) {
    int[] kinds = new int[agg.aggCalls().size()];
    for (int i = 0; i < kinds.length; i++) {
      int kind =
          WindowAggregateMatcher.aggregateKind(agg.aggCalls().apply(i).getAggregation().getKind());
      kinds[i] = kind == WindowAggregateMatcher.KIND_COUNT ? WindowAggregateMatcher.KIND_SUM : kind;
    }
    return kinds;
  }

  /** How many positional partial columns aggregate {@code i} spans (2 for AVG, 1 otherwise). */
  private static int spanOf(StreamPhysicalGlobalGroupAggregate agg, int i) {
    return WindowAggregateMatcher.aggregateKind(agg.aggCalls().apply(i).getAggregation().getKind())
            == WindowAggregateMatcher.KIND_AVG
        ? 2
        : 1;
  }

  static boolean generateUpdateBefore(StreamPhysicalGlobalGroupAggregate agg) {
    return ChangelogPlanUtils.generateUpdateBefore(agg);
  }
}
