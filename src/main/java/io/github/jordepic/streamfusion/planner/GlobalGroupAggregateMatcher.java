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
      // COUNT/SUM(DISTINCT x): the merge folds the local bundles' (value, count) view entries into
      // the per-key distinct set — the positional partial (the bundle's count/sum) is carried but
      // not consumed, exactly as the host's distinct merge re-accumulates from the view. The value
      // type comes from the ORIGINAL local input (the call's args point into it). Same scope as the
      // local half: COUNT over the set-carriable types, SUM over exact integer arithmetic only.
      if (call.isDistinct()) {
        if (call.getArgList().size() != 1) {
          return "global group aggregate: a distinct merge must have exactly one argument";
        }
        SqlTypeName valueType =
            agg.localAggInputRowType()
                .getFieldList()
                .get(call.getArgList().get(0))
                .getType()
                .getSqlTypeName();
        SqlTypeName partialType = inputType.getFieldList().get(offset).getType().getSqlTypeName();
        offset++;
        boolean countDistinct =
            kind == WindowAggregateMatcher.KIND_COUNT
                && partialType == SqlTypeName.BIGINT
                && LocalGroupAggregateMatcher.supportedDistinctValueType(valueType);
        boolean sumDistinct =
            kind == WindowAggregateMatcher.KIND_SUM
                && (valueType == SqlTypeName.BIGINT || valueType == SqlTypeName.INTEGER)
                && partialType == valueType;
        if (!countDistinct && !sumDistinct) {
          return "global group aggregate: distinct merges are COUNT (over set-carriable value"
              + " types) and SUM (over bigint/int)";
        }
        continue;
      }
      if (kind == WindowAggregateMatcher.KIND_AVG) {
        // AVG merges a (widened sum, count) partial pair into the final result column, whose type
        // must be one the AVG state's cast-back supports and must agree with the sum's width.
        if (offset + 1 >= inputType.getFieldCount()) {
          return "global group aggregate: AVG expects a (sum, count) partial pair";
        }
        RelDataType sumRel = inputType.getFieldList().get(offset).getType();
        SqlTypeName sumType = sumRel.getSqlTypeName();
        SqlTypeName countType =
            inputType.getFieldList().get(offset + 1).getType().getSqlTypeName();
        RelDataType resultRel = agg.getRowType().getFieldList().get(grouping.length + i).getType();
        SqlTypeName resultType = resultRel.getSqlTypeName();
        boolean intAvg = sumType == SqlTypeName.BIGINT && isIntegerAvgResult(resultType);
        boolean doubleAvg = sumType == SqlTypeName.DOUBLE && isFloatAvgResult(resultType);
        // A decimal AVG merges SUM's DECIMAL(38, s) partial and reports findAvgAggType's
        // DECIMAL(38, max(6, s)) — the exact-division emit the single-phase state implements.
        boolean decimalAvg =
            sumType == SqlTypeName.DECIMAL
                && resultType == SqlTypeName.DECIMAL
                && resultRel.getPrecision() == 38
                && resultRel.getScale() == Math.max(6, sumRel.getScale());
        if (!(intAvg || doubleAvg || decimalAvg) || countType != SqlTypeName.BIGINT) {
          return "global group aggregate: AVG merge partials must be (bigint, bigint) for an"
              + " integer average, (double, bigint) for a float/double one, or"
              + " (decimal(38, s), bigint) for a decimal one";
        }
        offset += 2;
        continue;
      }
      RelDataType partialRel = inputType.getFieldList().get(offset).getType();
      offset++;
      if (partialCode(partialRel) < 0) {
        return "global group aggregate: partial columns must be bigint/int/double/decimal";
      }
    }
    // The distinct views must be exactly the trailing input fields, one per unique distinct arg —
    // the positional contract behind distinctViewColumns().
    if (offset + distinctViewCount(agg) != inputType.getFieldCount()) {
      return "global group aggregate: unexpected partial layout (retracting local input?)";
    }
    return null;
  }

  /** Native value-type code for a partial column, or -1 if unsupported. */
  private static int partialCode(RelDataType type) {
    switch (type.getSqlTypeName()) {
      case BIGINT:
        return 0;
      case DOUBLE:
        return 1;
      case INTEGER:
        return 2;
      case DECIMAL:
        // A decimal SUM partial arrives pre-widened to DECIMAL(38, s); MIN/MAX keep DECIMAL(p, s).
        // The packed code carries the partial's own precision/scale either way.
        return io.github.jordepic.streamfusion.operator.NativeWindowOperatorCore.decimalCode(
            type.getPrecision(), type.getScale());
      default:
        return -1;
    }
  }

  /** AVG result types the bigint-summed merge casts back to (Flink's integer AvgAggFunctions). */
  private static boolean isIntegerAvgResult(SqlTypeName type) {
    return type == SqlTypeName.BIGINT
        || type == SqlTypeName.INTEGER
        || type == SqlTypeName.SMALLINT
        || type == SqlTypeName.TINYINT;
  }

  /** AVG result types the double-summed merge casts back to (float narrows, double stays). */
  private static boolean isFloatAvgResult(SqlTypeName type) {
    return type == SqlTypeName.DOUBLE || type == SqlTypeName.FLOAT || type == SqlTypeName.REAL;
  }

  /** Native value-type code for an AVG's final result (the state casts back to it on emit). */
  private static int avgResultCode(SqlTypeName type) {
    switch (type) {
      case BIGINT:
        return 0;
      case DOUBLE:
        return 1;
      case INTEGER:
        return 2;
      case SMALLINT:
        return 4;
      case TINYINT:
        return 5;
      case FLOAT:
      case REAL:
        return 6;
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
      AggregateCall call = agg.aggCalls().apply(i);
      if (call.isDistinct()) {
        // The distinct set is keyed by the original value (the call's args point into the local's
        // input row), so its code carries that value's own type.
        codes.add(
            WindowAggregateMatcher.typeCode(
                agg.localAggInputRowType().getFieldList().get(call.getArgList().get(0)).getType()));
      } else if (spanOf(agg, i) == 2) {
        // An AVG state is typed by its final result — except decimal, whose state is the sum
        // partial's DECIMAL(38, s) (the emit derives the result scale max(6, s) itself).
        RelDataType sumRel = inputType.getFieldList().get(offset).getType();
        RelDataType resultRel = agg.getRowType().getFieldList().get(grouping + i).getType();
        codes.add(
            sumRel.getSqlTypeName() == SqlTypeName.DECIMAL
                ? partialCode(sumRel)
                : avgResultCode(resultRel.getSqlTypeName()));
      } else {
        codes.add(partialCode(inputType.getFieldList().get(offset).getType()));
      }
      offset += spanOf(agg, i);
    }
    int[] array = new int[codes.size()];
    for (int i = 0; i < array.length; i++) {
      array[i] = codes.get(i);
    }
    return array;
  }

  /**
   * Merge kinds: COUNT merges by summing its partial counts; AVG keeps the ordinary AVG state; a
   * distinct COUNT/SUM keeps the distinct-set state (kind 7/9) fed from its view column.
   */
  static int[] kinds(StreamPhysicalGlobalGroupAggregate agg) {
    int[] kinds = new int[agg.aggCalls().size()];
    for (int i = 0; i < kinds.length; i++) {
      AggregateCall call = agg.aggCalls().apply(i);
      int kind = WindowAggregateMatcher.aggregateKind(call.getAggregation().getKind());
      if (call.isDistinct()) {
        kinds[i] =
            kind == WindowAggregateMatcher.KIND_COUNT
                ? LocalGroupAggregateMatcher.KIND_COUNT_DISTINCT
                : LocalGroupAggregateMatcher.KIND_SUM_DISTINCT;
      } else {
        kinds[i] =
            kind == WindowAggregateMatcher.KIND_COUNT ? WindowAggregateMatcher.KIND_SUM : kind;
      }
    }
    return kinds;
  }

  /**
   * The number of trailing distinct-view columns on the DECLARED input row type: one per unique
   * distinct arg list (Flink shares a view across filtered instances via a bitmask map value).
   */
  private static int distinctViewCount(StreamPhysicalGlobalGroupAggregate agg) {
    List<List<Integer>> seen = new ArrayList<>();
    for (int i = 0; i < agg.aggCalls().size(); i++) {
      AggregateCall call = agg.aggCalls().apply(i);
      if (call.isDistinct() && !seen.contains(call.getArgList())) {
        seen.add(call.getArgList());
      }
    }
    return seen.size();
  }

  /**
   * Per-aggregate distinct-view column (-1 for a non-distinct merge) in the NATIVE local's emitted
   * batch: the views trail the partials, one per unique (arg list, filter) pair in
   * first-appearance order — the native local's sharing rule (each filtered instance merges a set
   * that saw only its filter's rows), not Flink's declared per-arg-list sharing. These indices
   * address the native Arrow batch, never the declared rel row type.
   */
  static int[] distinctViewColumns(StreamPhysicalGlobalGroupAggregate agg) {
    int firstView = agg.grouping().length;
    for (int i = 0; i < agg.aggCalls().size(); i++) {
      firstView += spanOf(agg, i);
    }
    List<String> seen = new ArrayList<>();
    int[] columns = new int[agg.aggCalls().size()];
    for (int i = 0; i < columns.length; i++) {
      AggregateCall call = agg.aggCalls().apply(i);
      if (!call.isDistinct()) {
        columns[i] = -1;
        continue;
      }
      String key = LocalGroupAggregateMatcher.distinctViewKey(call);
      if (!seen.contains(key)) {
        seen.add(key);
      }
      columns[i] = firstView + seen.indexOf(key);
    }
    return columns;
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
