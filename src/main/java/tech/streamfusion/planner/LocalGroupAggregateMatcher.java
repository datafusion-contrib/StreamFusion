package tech.streamfusion.planner;

import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory$;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalLocalGroupAggregate;
import org.apache.flink.table.planner.plan.utils.ChangelogPlanUtils;
import scala.collection.Seq;
import tech.streamfusion.operator.RowDataArrowConverter;

/**
 * Recognizes the local half of a two-phase non-windowed {@code GROUP BY}: a stateless per-batch
 * pre-aggregate that emits one partial row per key ({@code [grouping.., partial0..]}) for the
 * {@link GlobalGroupAggregateMatcher global half} to merge. Scope mirrors the single-phase {@link
 * GroupAggregateMatcher}: SUM/MIN/MAX/COUNT over integer/float/double/decimal values, AVG over
 * Flink's AvgAggFunction numerics (the narrow integers and float included — the sum partial widens
 * to bigint/double), and COUNT/SUM/AVG(DISTINCT) whose per-bundle value set rides a trailing view
 * column, with grouping keys the boundary carries. A FILTER clause is native: the boolean column
 * gates each local fold and the merged partials are already filtered.
 *
 * <p>The native local emits each aggregate's partial in its running type: {@code SUM/MIN/MAX} keep
 * the value's own type (Flink's SUM partial does not widen — verified against the planner), COUNT
 * is bigint, and an AVG contributes <em>two</em> partial columns — the widened running sum (bigint
 * for integer inputs, double for double, Flink's {@code AvgAggFunction}) and the bigint non-null
 * count. Retracting integer/FLOAT/DOUBLE SUM also carries a bigint non-null count beside its typed
 * sum. Partials are positional, so the accessors below expand an AVG into its two native states (a
 * widened-sum state plus a COUNT over the same column) and the type checks walk the output row with
 * a per-aggregate offset. The global reads each partial by its declared (Flink) type, so every
 * declared partial column must equal what the native side emits — anything else falls back cleanly
 * rather than mismatching at the boundary.
 */
final class LocalGroupAggregateMatcher {

  private LocalGroupAggregateMatcher() {}

  static boolean preservesComplexValues(StreamPhysicalLocalGroupAggregate agg) {
    if (!matches(agg)) return false;
    RelDataType input = agg.getInput().getRowType();
    for (int i = 0; i < agg.aggCalls().size(); i++) {
      AggregateCall call = agg.aggCalls().apply(i);
      if (call.isDistinct()
          && call.getAggregation().getKind() == org.apache.calcite.sql.SqlKind.COUNT
          && call.getArgList().size() == 1
          && input.getFieldList().get(call.getArgList().get(0)).getType().getSqlTypeName()
              == SqlTypeName.ARRAY) return true;
    }
    return false;
  }

  static int aggregateKind(org.apache.calcite.sql.SqlKind kind) {
    if (kind == org.apache.calcite.sql.SqlKind.JSON_OBJECTAGG) return 42;
    if (kind == org.apache.calcite.sql.SqlKind.LISTAGG)
      return GroupAggregateMatcher.KIND_LISTAGG_APPEND;
    if (kind == org.apache.calcite.sql.SqlKind.SUM0) return GroupAggregateMatcher.KIND_SUM_ZERO;
    return kind == org.apache.calcite.sql.SqlKind.SINGLE_VALUE
        ? 14
        : WindowAggregateMatcher.aggregateKind(kind);
  }

  static int jsonObjectKind(AggregateCall call) {
    return 42
        + (tech.streamfusion.compat.JsonRuntimeCompat.LEGACY_SQL_JSON ? 2 : 0)
        + (((org.apache.calcite.sql.fun.SqlJsonObjectAggAggFunction) call.getAggregation())
                    .getNullClause()
                == org.apache.calcite.sql.SqlJsonConstructorNullClause.ABSENT_ON_NULL
            ? 0
            : 1);
  }

  static boolean supportedJsonObjectPartial(RelDataType partial) {
    if (partial.isStruct()) {
      if (partial.getFieldCount() != 1 || !partial.getFieldList().get(0).getName().equals("map"))
        return false;
      partial = partial.getFieldList().get(0).getType();
    }
    SqlTypeName type = partial.getSqlTypeName();
    return type == SqlTypeName.OTHER || type == SqlTypeName.ANY || type == SqlTypeName.MAP;
  }

  static boolean supportedJsonObjectCall(AggregateCall call, RelDataType input) {
    return !call.isDistinct()
        && call.filterArg < 0
        && call.getArgList().size() == 2
        && call.getCollation().getFieldCollations().isEmpty()
        && call.getArgList().stream()
            .allMatch(
                column ->
                    GroupAggregateMatcher.isStringType(
                        input.getFieldList().get(column).getType().getSqlTypeName()));
  }

  static int partialWidth(AggregateCall call, boolean retracting) {
    int kind = aggregateKind(call.getAggregation().getKind());
    if (kind == GroupAggregateMatcher.KIND_LISTAGG_APPEND && retracting) return 1;
    return kind == GroupAggregateMatcher.KIND_LISTAGG_APPEND
            || kind == 14
            || kind == WindowAggregateMatcher.KIND_AVG
            || isRetractingSum(call, retracting)
        ? 2
        : 1;
  }

  private static int stateWidth(AggregateCall call, boolean retracting) {
    return aggregateKind(call.getAggregation().getKind())
                == GroupAggregateMatcher.KIND_LISTAGG_APPEND
            || isRetractingExtreme(call, retracting)
        ? 1
        : partialWidth(call, retracting);
  }

  static boolean supportedListAggPartials(RelDataType separator, RelDataType prefix) {
    return GroupAggregateMatcher.isStringType(separator.getSqlTypeName())
        && GroupAggregateMatcher.isStringType(prefix.getSqlTypeName());
  }

  static boolean supportedRetractingListPartial(RelDataType partial, boolean separator) {
    if (!partial.isStruct() || partial.getFieldCount() != (separator ? 3 : 2)) return false;
    if (!partial.getFieldList().get(0).getName().equals("list")
        || !partial.getFieldList().get(1).getName().equals("retractList")) return false;
    for (int i = 0; i < 2; i++) {
      SqlTypeName type = partial.getFieldList().get(i).getType().getSqlTypeName();
      if (type != SqlTypeName.ARRAY && type != SqlTypeName.OTHER && type != SqlTypeName.ANY)
        return false;
    }
    return !separator
        || GroupAggregateMatcher.isStringType(
            partial.getFieldList().get(2).getType().getSqlTypeName());
  }

  static boolean isRetractingExtreme(AggregateCall call, boolean retracting) {
    int kind = aggregateKind(call.getAggregation().getKind());
    return retracting
        && !call.isDistinct()
        && (kind == WindowAggregateMatcher.KIND_MIN || kind == WindowAggregateMatcher.KIND_MAX);
  }

  private static boolean mappedExtreme(StreamPhysicalLocalGroupAggregate agg, int index) {
    int offset = agg.grouping().length;
    for (int i = 0; i < index; i++)
      offset += partialWidth(agg.aggCalls().apply(i), needRetraction(agg));
    return isRetractingExtreme(agg.aggCalls().apply(index), needRetraction(agg))
        && agg.getRowType().getFieldList().get(offset).getType().isStruct();
  }

  static boolean supportedExtremePartials(RelDataType value, RelDataType partial) {
    if (!partial.isStruct() || partial.getFieldCount() != 3) return false;
    RelDataType selected = partial.getFieldList().get(0).getType();
    RelDataType size = partial.getFieldList().get(1).getType();
    SqlTypeName map = partial.getFieldList().get(2).getType().getSqlTypeName();
    return WindowAggregateMatcher.supportedAggregateValueType(
            WindowAggregateMatcher.KIND_MIN, value.getSqlTypeName())
        && org.apache.calcite.sql.type.SqlTypeUtil.equalSansNullability(value, selected)
        && size.getSqlTypeName() == SqlTypeName.BIGINT
        && (map == SqlTypeName.OTHER || map == SqlTypeName.ANY || map == SqlTypeName.MAP);
  }

  static boolean isRetractingSum(AggregateCall call, boolean retracting) {
    return retracting
        && aggregateKind(call.getAggregation().getKind()) == WindowAggregateMatcher.KIND_SUM;
  }

  static boolean supportedRetractingSumPartials(
      RelDataType value, RelDataType sum, RelDataType count) {
    return (GroupAggregateMatcher.isRunningType(value.getSqlTypeName())
                && org.apache.calcite.sql.type.SqlTypeUtil.equalSansNullability(value, sum)
            || value.getSqlTypeName() == SqlTypeName.DECIMAL && isWidenedDecimal(sum, value))
        && count.getSqlTypeName() == SqlTypeName.BIGINT;
  }

  static boolean supportedSinglePartials(
      RelDataType value, RelDataType partial, RelDataType count) {
    SqlTypeName type = value.getSqlTypeName();
    return GroupAggregateMatcher.orderedValueType(type)
        && (org.apache.calcite.sql.type.SqlTypeUtil.equalSansNullability(value, partial)
            // Flink's TIME single-value buffer may declare TIME(3) for a TIME(0) input.
            // Both use the same millisecond Arrow storage, preserving the input exactly.
            || type == SqlTypeName.TIME && partial.getSqlTypeName() == SqlTypeName.TIME)
        && count.getSqlTypeName() == SqlTypeName.INTEGER;
  }

  static boolean matches(StreamPhysicalLocalGroupAggregate agg) {
    RelDataType inputType = agg.getInput().getRowType();
    // The input row crosses into the native local; the partials never reach the host (they flow to
    // the native global), so only the input types must be carriable.
    if (!RowDataArrowConverter.supports(FlinkTypeFactory$.MODULE$.toLogicalRowType(inputType))) {
      return false;
    }
    int[] grouping = agg.grouping();
    RelDataType outputType = agg.getRowType(); // [grouping.., partial0..]
    Seq<AggregateCall> aggCalls = agg.aggCalls();
    // A retracting input (the aggregate consumes another aggregate's changelog, Nexmark q4's
    // shape): the local folds -U/-D rows by subtracting. COUNT, AVG and exact SUM0 use their
    // append-only layouts, while retracting MIN/MAX declare a structured signed value map.
    // SINGLE_VALUE keeps its
    // typed value and signed INT count; SUM_RETRACT carries a
    // separate non-null count; a distinct view's map value switches to per-filter counts.
    boolean needRetraction = needRetraction(agg);
    int offset = grouping.length;
    for (int i = 0; i < aggCalls.size(); i++) {
      AggregateCall call = aggCalls.apply(i);
      if (call.isApproximate()) {
        return false;
      }
      int retractKind = aggregateKind(call.getAggregation().getKind());
      if (needRetraction
          && (retractKind != WindowAggregateMatcher.KIND_COUNT
              && retractKind != WindowAggregateMatcher.KIND_AVG
              && retractKind != GroupAggregateMatcher.KIND_SUM_ZERO
              && retractKind != WindowAggregateMatcher.KIND_SUM
              && retractKind != GroupAggregateMatcher.KIND_LISTAGG_APPEND
              && retractKind != 42
              && retractKind != 14
              && retractKind != WindowAggregateMatcher.KIND_MIN
              && retractKind != WindowAggregateMatcher.KIND_MAX)) {
        return false;
      }
      // A FILTER is a boolean input column (the planner materializes `IS TRUE(p)` in the Calc
      // below); the native local gates every fold — plain and distinct — on it, so the emitted
      // partials are already filtered and the global merge stays filter-blind — exactly Flink's
      // split.
      if (call.filterArg >= 0
          && inputType.getFieldList().get(call.filterArg).getType().getSqlTypeName()
              != SqlTypeName.BOOLEAN) {
        return false;
      }
      int kind = aggregateKind(call.getAggregation().getKind());
      if (kind == 42) {
        if (!supportedJsonObjectCall(call, inputType)
            || offset >= outputType.getFieldCount()
            || !supportedJsonObjectPartial(outputType.getFieldList().get(offset).getType()))
          return false;
        offset++;
        continue;
      }
      if (kind == GroupAggregateMatcher.KIND_LISTAGG_APPEND) {
        if (call.isDistinct()
            || !call.getCollation().getFieldCollations().isEmpty()
            || call.getArgList().isEmpty()
            || call.getArgList().size() > 2
            || !GroupAggregateMatcher.isStringType(
                inputType.getFieldList().get(call.getArgList().get(0)).getType().getSqlTypeName())
            || call.getArgList().size() == 2
                && !GroupAggregateMatcher.literalStringColumn(
                    agg.getInput(), call.getArgList().get(1))) return false;
        if (needRetraction) {
          if (offset >= outputType.getFieldCount()
              || !supportedRetractingListPartial(
                  outputType.getFieldList().get(offset).getType(), call.getArgList().size() == 2))
            return false;
          offset++;
        } else {
          if (offset + 1 >= outputType.getFieldCount()
              || !supportedListAggPartials(
                  outputType.getFieldList().get(offset).getType(),
                  outputType.getFieldList().get(offset + 1).getType())) return false;
          offset += 2;
        }
        continue;
      }
      if (kind < 0 || call.getArgList().size() > 1) {
        return false; // SUM/MIN/MAX/COUNT/AVG/SINGLE_VALUE only
      }
      if ((kind == WindowAggregateMatcher.KIND_AVG || kind == GroupAggregateMatcher.KIND_SUM_ZERO)
          && call.getArgList().size() == 1) {
        var value = inputType.getFieldList().get(call.getArgList().get(0)).getType();
        if (value.getSqlTypeName() == SqlTypeName.DECIMAL && value.getScale() == 38) return false;
      }
      if (kind == GroupAggregateMatcher.KIND_SUM_ZERO
          && (call.getArgList().size() != 1
              || !WindowAggregateMatcher.supportedZeroSumValue(
                  inputType.getFieldList().get(call.getArgList().get(0)).getType()))) return false;
      // COUNT/SUM/AVG(DISTINCT x): the partial is the bundle's distinct count / distinct sum, and
      // the
      // bundle's (value, count) set rides a trailing view column (Flink's MapView partial) that the
      // global merges with multiplicities. Floating and wide decimal reductions retain the host's
      // membership-map iteration order. MIN/MAX DISTINCT still fall back two-phase.
      if (call.isDistinct()) {
        if (call.getArgList().size() != 1) {
          return false;
        }
        RelDataType value = inputType.getFieldList().get(call.getArgList().get(0)).getType();
        SqlTypeName valueType = value.getSqlTypeName();
        int width = partialWidth(call, needRetraction);
        if (offset + width > outputType.getFieldCount()) return false;
        RelDataType partial = outputType.getFieldList().get(offset).getType();
        SqlTypeName partialType = partial.getSqlTypeName();
        RelDataType count = width == 2 ? outputType.getFieldList().get(offset + 1).getType() : null;
        offset += width;
        if (kind == WindowAggregateMatcher.KIND_COUNT) {
          if (partialType != SqlTypeName.BIGINT || !supportedDistinctValueType(valueType)) {
            return false;
          }
        } else if (kind == WindowAggregateMatcher.KIND_SUM
            || kind == GroupAggregateMatcher.KIND_SUM_ZERO) {
          if (!supportedDistinctSumPartial(value, partial)
              || count != null && count.getSqlTypeName() != SqlTypeName.BIGINT) return false;
        } else if (kind == WindowAggregateMatcher.KIND_AVG) {
          if (!supportedDistinctAveragePartials(value, partial, count)) return false;
        } else {
          return false;
        }
        continue;
      }
      if (kind == 14) {
        if (call.getArgList().size() != 1
            || offset + 1 >= outputType.getFieldCount()
            || !supportedSinglePartials(
                inputType.getFieldList().get(call.getArgList().get(0)).getType(),
                outputType.getFieldList().get(offset).getType(),
                outputType.getFieldList().get(offset + 1).getType())) return false;
        offset += 2;
        continue;
      }
      if (isRetractingExtreme(call, needRetraction)
          && offset < outputType.getFieldCount()
          && outputType.getFieldList().get(offset).getType().isStruct()) {
        if (call.getArgList().size() != 1
            || offset >= outputType.getFieldCount()
            || !supportedExtremePartials(
                inputType.getFieldList().get(call.getArgList().get(0)).getType(),
                outputType.getFieldList().get(offset).getType())) return false;
        offset++;
        continue;
      }
      if (isRetractingSum(call, needRetraction)) {
        if (call.getArgList().size() != 1
            || offset + 1 >= outputType.getFieldCount()
            || !supportedRetractingSumPartials(
                inputType.getFieldList().get(call.getArgList().get(0)).getType(),
                outputType.getFieldList().get(offset).getType(),
                outputType.getFieldList().get(offset + 1).getType())) return false;
        offset += 2;
        continue;
      }
      if (kind == WindowAggregateMatcher.KIND_AVG) {
        // AVG spans two positional partials: the widened running sum, then the bigint count.
        if (call.getArgList().isEmpty() || offset + 1 >= outputType.getFieldCount()) {
          return false;
        }
        RelDataType valueRel = inputType.getFieldList().get(call.getArgList().get(0)).getType();
        RelDataType sumRel = outputType.getFieldList().get(offset).getType();
        // A decimal AVG's sum partial is SUM's DECIMAL(38, s) accumulator at the value's scale; the
        // other numerics widen to bigint/double.
        boolean widenedOk =
            valueRel.getSqlTypeName() == SqlTypeName.DECIMAL
                ? isWidenedDecimal(sumRel, valueRel)
                : sumRel.getSqlTypeName() == widenedSumType(valueRel.getSqlTypeName());
        if (!widenedOk
            || outputType.getFieldList().get(offset + 1).getType().getSqlTypeName()
                != SqlTypeName.BIGINT) {
          return false;
        }
        offset += 2;
        continue;
      }
      RelDataType partialRel = outputType.getFieldList().get(offset).getType();
      SqlTypeName partialType = partialRel.getSqlTypeName();
      offset++;
      if (kind == WindowAggregateMatcher.KIND_COUNT) {
        // COUNT(*) (empty argList) or COUNT(col); the partial is the bigint running count either
        // way.
        if (partialType != SqlTypeName.BIGINT) {
          return false;
        }
        continue;
      }
      // SUM/MIN/MAX read a typed running value; it must be a running type, DECIMAL, or (MIN/MAX
      // only) a string, and the partial Flink declares must equal what the native side emits — the
      // value type itself, except a decimal SUM whose partial widens to DECIMAL(38, s) — so the
      // emit and the global read agree.
      RelDataType valueRel = inputType.getFieldList().get(call.getArgList().get(0)).getType();
      SqlTypeName valueType = valueRel.getSqlTypeName();
      if (valueType == SqlTypeName.DECIMAL) {
        boolean matches =
            (kind == WindowAggregateMatcher.KIND_SUM || kind == GroupAggregateMatcher.KIND_SUM_ZERO)
                ? isWidenedDecimal(partialRel, valueRel)
                : partialType == SqlTypeName.DECIMAL
                    && partialRel.getPrecision() == valueRel.getPrecision()
                    && partialRel.getScale() == valueRel.getScale();
        if (!matches) {
          return false;
        }
        continue;
      }
      // MIN/MAX over a string keep the extreme byte-lexicographically (matching Flink's
      // BinaryStringData comparison, divergences/07) — the partial is the value's own type.
      if (isStringExtreme(kind, valueType)) {
        if (partialType != valueType) {
          return false;
        }
        continue;
      }
      if (isTimestampExtreme(kind, valueType)) {
        if (partialType != valueType || partialRel.getPrecision() != valueRel.getPrecision()) {
          return false;
        }
        continue;
      }
      if (isTemporalBooleanExtreme(kind, valueType)) {
        if (!org.apache.calcite.sql.type.SqlTypeUtil.equalSansNullability(partialRel, valueRel)) {
          return false;
        }
        continue;
      }
      if (!GroupAggregateMatcher.isRunningType(valueType) || partialType != valueType) {
        return false;
      }
    }
    // Under retraction Flink appends a COUNT(*) accumulator ("count1", the global's per-key record
    // counter) unless a bare COUNT(*) already exists to reuse — one extra bigint partial after the
    // real aggregates'.
    if (countStarInserted(agg)) {
      if (offset >= outputType.getFieldCount()
          || outputType.getFieldList().get(offset).getType().getSqlTypeName()
              != SqlTypeName.BIGINT) {
        return false;
      }
      offset++;
    }
    // Flink's declared trailing view columns: one per unique distinct arg list (instances with
    // different filters SHARE a declared view whose map value is a per-filter bitmask). The native
    // side instead emits one view per unique (arg list, filter) pair — each backed by a set that
    // saw only its filter's rows — which yields the same final output because a filtered distinct
    // is exactly an unfiltered distinct over the filtered row subset. The declared layout is only
    // validated here; the emitted layout is a native-internal contract with the global (the
    // partial row never crosses to the host — the island is all-or-nothing).
    return offset + declaredViewCount(agg) == outputType.getFieldCount();
  }

  /** Whether the local consumes a retracting changelog (Flink's needRetraction on this rel). */
  static boolean needRetraction(StreamPhysicalLocalGroupAggregate agg) {
    return !ChangelogPlanUtils.inputInsertOnly(agg);
  }

  /**
   * Whether Flink appended the count1 COUNT(*) accumulator: a retracting input with no reusable
   * bare COUNT(*) among the calls (Flink's insertCountStarAggCall reuse rule — plain, unfiltered,
   * non-distinct). Its bigint partial trails the real aggregates', before any distinct views.
   */
  static boolean countStarInserted(StreamPhysicalLocalGroupAggregate agg) {
    if (!needRetraction(agg)) {
      return false;
    }
    Seq<AggregateCall> aggCalls = agg.aggCalls();
    for (int i = 0; i < aggCalls.size(); i++) {
      AggregateCall call = aggCalls.apply(i);
      if (call.getAggregation().getKind() == org.apache.calcite.sql.SqlKind.COUNT
          && call.getArgList().isEmpty()
          && call.filterArg < 0
          && !call.isDistinct()
          && !call.isApproximate()) {
        return false;
      }
    }
    return true;
  }

  /** Flink's declared trailing view-column count: one per unique distinct arg list. */
  private static int declaredViewCount(StreamPhysicalLocalGroupAggregate agg) {
    List<List<Integer>> seen = new ArrayList<>();
    Seq<AggregateCall> aggCalls = agg.aggCalls();
    for (int i = 0; i < aggCalls.size(); i++) {
      AggregateCall call = aggCalls.apply(i);
      if (call.isDistinct() && !seen.contains(call.getArgList())) {
        seen.add(call.getArgList());
      }
    }
    return seen.size();
  }

  /**
   * Distinct value types the native set carries faithfully (the types {@code typeCode} maps; other
   * types would mis-key the specialized set, so they decline).
   */
  static boolean supportedDistinctValueType(SqlTypeName type) {
    if (type.getFamily() == org.apache.calcite.sql.type.SqlTypeFamily.INTERVAL_DAY_TIME
        || type.getFamily() == org.apache.calcite.sql.type.SqlTypeFamily.INTERVAL_YEAR_MONTH)
      return true;
    switch (type) {
      case ARRAY:
      case BIGINT:
      case INTEGER:
      case SMALLINT:
      case TINYINT:
      case FLOAT:
      case REAL:
      case DOUBLE:
      case CHAR:
      case VARCHAR:
      case DECIMAL:
      case BOOLEAN:
      case DATE:
      case TIME:
      case TIMESTAMP:
      case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
        return true;
      default:
        return false;
    }
  }

  /**
   * Per emitted distinct view column, the native partial-state index whose bundle set backs it: one
   * view per unique (arg list, filter) pair, in first-appearance order. Instances with the same
   * args but different filters get separate views (their sets saw different row subsets), unlike
   * Flink's shared bitmask view — see the layout note in {@link #matches}.
   */
  static int[] distinctViewSources(StreamPhysicalLocalGroupAggregate agg) {
    List<String> seen = new ArrayList<>();
    List<Integer> sources = new ArrayList<>();
    Seq<AggregateCall> aggCalls = agg.aggCalls();
    int position = 0;
    for (int i = 0; i < aggCalls.size(); i++) {
      AggregateCall call = aggCalls.apply(i);
      if (call.isDistinct() && !seen.contains(distinctViewKey(call))) {
        seen.add(distinctViewKey(call));
        sources.add(position);
      }
      position += stateWidth(call, needRetraction(agg));
    }
    return toArray(sources);
  }

  /** The native view-sharing key: instances share a set only with identical args AND filter. */
  static String distinctViewKey(AggregateCall call) {
    return call.getArgList() + "#" + call.filterArg;
  }

  /** True if {@code partial} is the widened decimal accumulator DECIMAL(38, s) of {@code value}. */
  private static boolean isWidenedDecimal(RelDataType partial, RelDataType value) {
    return partial.getSqlTypeName() == SqlTypeName.DECIMAL
        && partial.getPrecision() == 38
        && partial.getScale() == value.getScale();
  }

  static boolean supportedDistinctSumPartial(RelDataType value, RelDataType partial) {
    return (GroupAggregateMatcher.isIntegerType(value.getSqlTypeName())
            && partial.getSqlTypeName() == value.getSqlTypeName())
        || ((value.getSqlTypeName() == SqlTypeName.FLOAT
                || value.getSqlTypeName() == SqlTypeName.DOUBLE)
            && partial.getSqlTypeName() == value.getSqlTypeName())
        || (value.getSqlTypeName() == SqlTypeName.DECIMAL && isWidenedDecimal(partial, value));
  }

  static boolean supportedDistinctAveragePartials(
      RelDataType value, RelDataType sum, RelDataType count) {
    return count.getSqlTypeName() == SqlTypeName.BIGINT
        && ((GroupAggregateMatcher.isIntegerType(value.getSqlTypeName())
                && sum.getSqlTypeName() == SqlTypeName.BIGINT)
            || ((value.getSqlTypeName() == SqlTypeName.FLOAT
                    || value.getSqlTypeName() == SqlTypeName.DOUBLE)
                && sum.getSqlTypeName() == SqlTypeName.DOUBLE)
            || (value.getSqlTypeName() == SqlTypeName.DECIMAL && isWidenedDecimal(sum, value)));
  }

  /**
   * The declared type of an AVG's widened sum partial, or null if the value type isn't admitted.
   */
  private static SqlTypeName widenedSumType(SqlTypeName valueType) {
    switch (valueType) {
      case BIGINT:
      case INTEGER:
      case SMALLINT:
      case TINYINT:
        return SqlTypeName.BIGINT;
      case DOUBLE:
      case FLOAT:
      case REAL:
        return SqlTypeName.DOUBLE;
      default:
        return null;
    }
  }

  /** True for a MIN/MAX over CHAR/VARCHAR — the string-extremes multiset path. */
  static boolean isStringExtreme(int kind, SqlTypeName valueType) {
    return (kind == WindowAggregateMatcher.KIND_MIN || kind == WindowAggregateMatcher.KIND_MAX)
        && (valueType == SqlTypeName.CHAR || valueType == SqlTypeName.VARCHAR);
  }

  static boolean isTemporalBooleanExtreme(int kind, SqlTypeName valueType) {
    return (kind == WindowAggregateMatcher.KIND_MIN || kind == WindowAggregateMatcher.KIND_MAX)
        && (valueType == SqlTypeName.DATE
            || valueType == SqlTypeName.TIME
            || valueType == SqlTypeName.BOOLEAN);
  }

  static boolean isTimestampExtreme(int kind, SqlTypeName valueType) {
    return (kind == WindowAggregateMatcher.KIND_MIN || kind == WindowAggregateMatcher.KIND_MAX)
        && GroupAggregateMatcher.isTimestampType(valueType);
  }

  /**
   * Native aggregate kind 7 (COUNT(DISTINCT)); matches the convention in the Rust GroupAggState.
   */
  static final int KIND_COUNT_DISTINCT = 7;

  /** Native aggregate kind 9 (SUM(DISTINCT)); matches the convention in the Rust GroupAggState. */
  static final int KIND_SUM_DISTINCT = 9;

  /** Expanded native states. AVG/SINGLE_VALUE use two; LISTAGG emits two partials from one. */
  static int[] kinds(StreamPhysicalLocalGroupAggregate agg) {
    List<Integer> kinds = new ArrayList<>();
    Seq<AggregateCall> aggCalls = agg.aggCalls();
    for (int i = 0; i < aggCalls.size(); i++) {
      AggregateCall call = aggCalls.apply(i);
      int kind = aggregateKind(call.getAggregation().getKind());
      if (kind == GroupAggregateMatcher.KIND_LISTAGG_APPEND
          && needRetraction(agg)
          && !call.isDistinct()) {
        kinds.add(call.getArgList().size() == 2 ? 47 : 46);
      } else if (kind == 42) {
        kinds.add(jsonObjectKind(call));
      } else if (call.isDistinct()) {
        if (kind == WindowAggregateMatcher.KIND_AVG) {
          kinds.add(18); // DISTINCT widened sum partial; its value set retains the input type.
          kinds.add(KIND_COUNT_DISTINCT);
        } else {
          kinds.add(
              kind == WindowAggregateMatcher.KIND_COUNT
                  ? KIND_COUNT_DISTINCT
                  : kind == GroupAggregateMatcher.KIND_SUM_ZERO
                      ? GroupAggregateMatcher.KIND_SUM_ZERO_DISTINCT
                      : KIND_SUM_DISTINCT);
          if (isRetractingSum(call, needRetraction(agg))) kinds.add(KIND_COUNT_DISTINCT);
        }
      } else if (mappedExtreme(agg, i)) {
        kinds.add(kind == WindowAggregateMatcher.KIND_MIN ? 38 : 39);
      } else if (kind == 14) {
        kinds.add(14);
        kinds.add(21); // SINGLE_VALUE count is INT and includes NULL inputs.
      } else if (isRetractingSum(call, needRetraction(agg))) {
        // A zero net contribution count must not erase the bundle's sum adjustment.
        kinds.add(
            agg.getInput()
                        .getRowType()
                        .getFieldList()
                        .get(call.getArgList().get(0))
                        .getType()
                        .getSqlTypeName()
                    == SqlTypeName.DECIMAL
                ? GroupAggregateMatcher.KIND_RETRACTING_DECIMAL_SUM
                : GroupAggregateMatcher.KIND_RAW_SUM_PARTIAL);
        kinds.add(WindowAggregateMatcher.KIND_COUNT);
      } else if (kind == WindowAggregateMatcher.KIND_AVG) {
        kinds.add(WindowAggregateMatcher.KIND_AVG_PARTIAL_SUM);
        kinds.add(WindowAggregateMatcher.KIND_COUNT);
      } else {
        kinds.add(
            needRetraction(agg)
                    && (kind == WindowAggregateMatcher.KIND_MIN
                        || kind == WindowAggregateMatcher.KIND_MAX)
                ? (kind == WindowAggregateMatcher.KIND_MIN
                    ? GroupAggregateMatcher.KIND_MIN_APPEND
                    : GroupAggregateMatcher.KIND_MAX_APPEND)
                : kind);
      }
    }
    if (countStarInserted(agg)) {
      kinds.add(WindowAggregateMatcher.KIND_COUNT); // the appended count1 record counter
    }
    return toArray(kinds);
  }

  /** Expanded FILTER columns; both partials share the filter, and -1 means unfiltered. */
  static int[] filterColumns(StreamPhysicalLocalGroupAggregate agg) {
    List<Integer> columns = new ArrayList<>();
    Seq<AggregateCall> aggCalls = agg.aggCalls();
    for (int i = 0; i < aggCalls.size(); i++) {
      AggregateCall call = aggCalls.apply(i);
      columns.add(call.filterArg);
      if (stateWidth(call, needRetraction(agg)) == 2) {
        columns.add(call.filterArg);
      }
    }
    if (countStarInserted(agg)) {
      columns.add(-1); // count1 counts every row unfiltered
    }
    return toArray(columns);
  }

  /** Expanded value columns (both fields of a two-field accumulator read its value). */
  static int[] valueColumns(StreamPhysicalLocalGroupAggregate agg) {
    List<Integer> columns = new ArrayList<>();
    Seq<AggregateCall> aggCalls = agg.aggCalls();
    for (int i = 0; i < aggCalls.size(); i++) {
      AggregateCall call = aggCalls.apply(i);
      int column =
          call.getArgList().isEmpty()
              ? -1
              : call.getArgList()
                  .get(
                      call.getAggregation().getKind()
                              == org.apache.calcite.sql.SqlKind.JSON_OBJECTAGG
                          ? 1
                          : 0);
      columns.add(column);
      if (stateWidth(call, needRetraction(agg)) == 2) {
        columns.add(column);
      }
    }
    if (countStarInserted(agg)) {
      columns.add(-1); // count1 is a COUNT(*): no argument column
    }
    return toArray(columns);
  }

  /** Expanded value-type codes; an AVG's sum state is typed by its WIDENED partial type. */
  static int[] valueTypeCodes(StreamPhysicalLocalGroupAggregate agg) {
    RelDataType inputType = agg.getInput().getRowType();
    List<Integer> codes = new ArrayList<>();
    Seq<AggregateCall> aggCalls = agg.aggCalls();
    for (int i = 0; i < aggCalls.size(); i++) {
      AggregateCall call = aggCalls.apply(i);
      int kind = aggregateKind(call.getAggregation().getKind());
      if (call.isDistinct()) {
        // The distinct set is keyed by the value itself, so its code carries the value's own type.
        int code =
            GroupAggregateMatcher.retainedValueTypeCode(
                inputType.getFieldList().get(call.getArgList().get(0)).getType());
        codes.add(code);
        if (stateWidth(call, needRetraction(agg)) == 2) codes.add(code);
      } else if (kind == GroupAggregateMatcher.KIND_LISTAGG_APPEND || kind == 42) {
        codes.add(3);
      } else if (kind == 14) {
        int code =
            GroupAggregateMatcher.retainedValueTypeCode(
                inputType.getFieldList().get(call.getArgList().get(0)).getType());
        codes.add(code);
        codes.add(code); // Both local states read the original value; kind 21 emits INT.
      } else if (isRetractingSum(call, needRetraction(agg))) {
        codes.add(
            WindowAggregateMatcher.typeCode(
                inputType.getFieldList().get(call.getArgList().get(0)).getType()));
        codes.add(0);
      } else if (kind == WindowAggregateMatcher.KIND_AVG) {
        RelDataType valueRel = inputType.getFieldList().get(call.getArgList().get(0)).getType();
        // The widened sum: a packed decimal code (the native partial reports DECIMAL(38, s)),
        // double, or bigint.
        codes.add(
            valueRel.getSqlTypeName() == SqlTypeName.DECIMAL
                ? decimalCode(valueRel)
                : widenedSumType(valueRel.getSqlTypeName()) == SqlTypeName.DOUBLE ? 1 : 0);
        codes.add(0); // the bigint count
      } else if (call.getArgList().isEmpty()) {
        codes.add(0);
      } else {
        RelDataType valueRel = inputType.getFieldList().get(call.getArgList().get(0)).getType();
        codes.add(
            isTemporalBooleanExtreme(kind, valueRel.getSqlTypeName())
                ? GroupAggregateMatcher.retainedValueTypeCode(valueRel)
                : WindowAggregateMatcher.typeCode(valueRel));
      }
    }
    if (countStarInserted(agg)) {
      codes.add(0); // count1's bigint running count
    }
    return toArray(codes);
  }

  /** The packed native code carrying a decimal's precision and scale. */
  private static int decimalCode(RelDataType type) {
    return tech.streamfusion.operator.NativeWindowOperatorCore.decimalCode(
        type.getPrecision(), type.getScale());
  }

  static int[] keyColumns(StreamPhysicalLocalGroupAggregate agg) {
    return WindowAggregateMatcher.keyColumns(agg.grouping());
  }

  private static int[] toArray(List<Integer> values) {
    int[] array = new int[values.size()];
    for (int i = 0; i < array.length; i++) {
      array[i] = values.get(i);
    }
    return array;
  }

  private static int[] auxiliaryColumns(StreamPhysicalLocalGroupAggregate agg) {
    var columns = new ArrayList<Integer>();
    for (int i = 0; i < agg.aggCalls().size(); i++) {
      var call = agg.aggCalls().apply(i);
      columns.add(
          call.getAggregation().getKind() == org.apache.calcite.sql.SqlKind.JSON_OBJECTAGG
              ? call.getArgList().get(0)
              : call.getAggregation().getKind() == org.apache.calcite.sql.SqlKind.LISTAGG
                      && call.getArgList().size() == 2
                  ? call.getArgList().get(1)
                  : -1);
      if (stateWidth(call, needRetraction(agg)) == 2) columns.add(-1);
    }
    if (countStarInserted(agg)) columns.add(-1);
    return columns.stream().anyMatch(column -> column >= 0) ? toArray(columns) : new int[0];
  }

  static RelNode substitute(StreamPhysicalLocalGroupAggregate agg, PlanContext ctx) {
    return new StreamPhysicalNativeColumnarLocalGroupAggregate(
        agg.getCluster(),
        agg.getTraitSet(),
        agg.getInputs().get(0),
        agg.getRowType(),
        LocalGroupAggregateMatcher.kinds(agg),
        LocalGroupAggregateMatcher.valueTypeCodes(agg),
        LocalGroupAggregateMatcher.valueColumns(agg),
        LocalGroupAggregateMatcher.filterColumns(agg),
        LocalGroupAggregateMatcher.keyColumns(agg),
        LocalGroupAggregateMatcher.distinctViewSources(agg),
        auxiliaryColumns(agg));
  }

  static String unsupportedReason(StreamPhysicalLocalGroupAggregate agg) {
    for (int i = 0; i < agg.aggCalls().size(); i++) {
      var call = agg.aggCalls().apply(i);
      if (call.isDistinct()
          && call.getArgList().size() == 1
          && call.getAggregation().getKind() == org.apache.calcite.sql.SqlKind.COUNT) {
        var value =
            agg.getInput().getRowType().getFieldList().get(call.getArgList().get(0)).getType();
        if (!supportedDistinctValueType(value.getSqlTypeName())) {
          return "local group aggregate: unsupported COUNT(DISTINCT) value type " + value;
        }
      }
    }
    return "local group aggregate: needs SUM/MIN/MAX/COUNT over integer/float/double/decimal values"
        + " with no widening of the partial, MIN/MAX over"
        + " string/date/time/boolean/timestamp, AVG over any AvgAggFunction numeric, or"
        + " SINGLE_VALUE over supported scalar values, and"
        + " bigint/int/string/boolean/date/timestamp/decimal grouping keys";
  }
}
