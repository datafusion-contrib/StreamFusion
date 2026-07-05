package io.github.jordepic.streamfusion.planner;

import io.github.jordepic.streamfusion.operator.RowDataArrowConverter;
import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory$;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalLocalGroupAggregate;
import scala.collection.Seq;

/**
 * Recognizes the local half of a two-phase non-windowed {@code GROUP BY}: a stateless per-batch
 * pre-aggregate that emits one partial row per key ({@code [grouping.., partial0..]}) for the
 * {@link GlobalGroupAggregateMatcher global half} to merge. Scope mirrors the single-phase
 * {@link GroupAggregateMatcher}: SUM/MIN/MAX/COUNT over bigint/int/double values, AVG over any of
 * Flink's AvgAggFunction numerics (the narrow integers and float included — the sum partial widens
 * to bigint/double), and COUNT/SUM(DISTINCT) whose per-bundle value set rides a trailing view
 * column, with grouping keys the boundary carries.
 *
 * <p>The native local emits each aggregate's partial in its running type: {@code SUM/MIN/MAX} keep
 * the value's own type (Flink's SUM partial does not widen — verified against the planner), COUNT is
 * bigint, and an AVG contributes <em>two</em> partial columns — the widened running sum (bigint for
 * integer inputs, double for double, Flink's {@code AvgAggFunction}) and the bigint non-null count.
 * Partials are positional, so the accessors below expand an AVG into its two native states (a
 * widened-sum state plus a COUNT over the same column) and the type checks walk the output row with
 * a per-aggregate offset. The global reads each partial by its declared (Flink) type, so every
 * declared partial column must equal what the native side emits — anything else falls back cleanly
 * rather than mismatching at the boundary.
 */
final class LocalGroupAggregateMatcher {

  private LocalGroupAggregateMatcher() {}

  static boolean matches(StreamPhysicalLocalGroupAggregate agg) {
    RelDataType inputType = agg.getInput().getRowType();
    // The input row crosses into the native local; the partials never reach the host (they flow to
    // the native global), so only the input types must be carriable.
    if (!RowDataArrowConverter.supports(FlinkTypeFactory$.MODULE$.toLogicalRowType(inputType))) {
      return false;
    }
    int[] grouping = agg.grouping();
    for (int column : grouping) {
      if (!WindowAggregateMatcher.supportedGroupingKeyType(
          inputType.getFieldList().get(column).getType().getSqlTypeName())) {
        return false;
      }
    }
    RelDataType outputType = agg.getRowType(); // [grouping.., partial0..]
    Seq<AggregateCall> aggCalls = agg.aggCalls();
    int offset = grouping.length;
    for (int i = 0; i < aggCalls.size(); i++) {
      AggregateCall call = aggCalls.apply(i);
      if (call.isApproximate() || call.filterArg >= 0) {
        return false;
      }
      int kind = WindowAggregateMatcher.aggregateKind(call.getAggregation().getKind());
      if (kind < 0 || call.getArgList().size() > 1) {
        return false; // SUM/MIN/MAX/COUNT/AVG only
      }
      // COUNT/SUM(DISTINCT x): the partial is the bundle's distinct count / distinct sum, and the
      // bundle's (value, count) set rides a trailing view column (Flink's MapView partial) that the
      // global merges with multiplicities. SUM admits exact arithmetic only — the merge folds in
      // set-iteration order, so order-sensitive float/double sums stay on the host. MIN/MAX/AVG
      // over DISTINCT fall back two-phase.
      if (call.isDistinct()) {
        if (call.getArgList().size() != 1) {
          return false;
        }
        SqlTypeName valueType =
            inputType.getFieldList().get(call.getArgList().get(0)).getType().getSqlTypeName();
        SqlTypeName partialType = outputType.getFieldList().get(offset).getType().getSqlTypeName();
        offset++;
        if (kind == WindowAggregateMatcher.KIND_COUNT) {
          if (partialType != SqlTypeName.BIGINT || !supportedDistinctValueType(valueType)) {
            return false;
          }
        } else if (kind == WindowAggregateMatcher.KIND_SUM) {
          if ((valueType != SqlTypeName.BIGINT && valueType != SqlTypeName.INTEGER)
              || partialType != valueType) {
            return false;
          }
        } else {
          return false;
        }
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
        // COUNT(*) (empty argList) or COUNT(col); the partial is the bigint running count either way.
        if (partialType != SqlTypeName.BIGINT) {
          return false;
        }
        continue;
      }
      // SUM/MIN/MAX read a typed running value; it must be a running type or DECIMAL, and the
      // partial Flink declares must equal what the native side emits — the value type itself,
      // except a decimal SUM whose partial widens to DECIMAL(38, s) — so the emit and the global
      // read agree.
      RelDataType valueRel = inputType.getFieldList().get(call.getArgList().get(0)).getType();
      SqlTypeName valueType = valueRel.getSqlTypeName();
      if (valueType == SqlTypeName.DECIMAL) {
        boolean matches =
            kind == WindowAggregateMatcher.KIND_SUM
                ? isWidenedDecimal(partialRel, valueRel)
                : partialType == SqlTypeName.DECIMAL
                    && partialRel.getPrecision() == valueRel.getPrecision()
                    && partialRel.getScale() == valueRel.getScale();
        if (!matches) {
          return false;
        }
        continue;
      }
      if (!isRunningType(valueType) || partialType != valueType) {
        return false;
      }
    }
    // The distinct views are exactly the trailing output fields, one per unique distinct arg — the
    // positional contract the native flush and the global's view columns both assume.
    return offset + distinctViewSources(agg).length == outputType.getFieldCount();
  }

  /**
   * Distinct value types the native set carries faithfully (the types {@code typeCode} maps; other
   * types would mis-key the specialized set, so they decline).
   */
  static boolean supportedDistinctValueType(SqlTypeName type) {
    switch (type) {
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
        return true;
      default:
        return false;
    }
  }

  /**
   * Per distinct view column (the trailing output fields, in Flink's declared order — one per unique
   * distinct arg list), the index of the aggregate whose bundle set backs it.
   */
  static int[] distinctViewSources(StreamPhysicalLocalGroupAggregate agg) {
    List<List<Integer>> seen = new ArrayList<>();
    List<Integer> sources = new ArrayList<>();
    Seq<AggregateCall> aggCalls = agg.aggCalls();
    for (int i = 0; i < aggCalls.size(); i++) {
      AggregateCall call = aggCalls.apply(i);
      if (call.isDistinct() && !seen.contains(call.getArgList())) {
        seen.add(call.getArgList());
        sources.add(i);
      }
    }
    return toArray(sources);
  }

  /** True if {@code partial} is the widened decimal accumulator DECIMAL(38, s) of {@code value}. */
  private static boolean isWidenedDecimal(RelDataType partial, RelDataType value) {
    return partial.getSqlTypeName() == SqlTypeName.DECIMAL
        && partial.getPrecision() == 38
        && partial.getScale() == value.getScale();
  }

  /** The declared type of an AVG's widened sum partial, or null if the value type isn't admitted. */
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

  private static boolean isRunningType(SqlTypeName type) {
    return type == SqlTypeName.BIGINT
        || type == SqlTypeName.INTEGER
        || type == SqlTypeName.DOUBLE;
  }

  /** Native aggregate kind 7 (COUNT(DISTINCT)); matches the convention in the Rust GroupAggState. */
  static final int KIND_COUNT_DISTINCT = 7;

  /** Native aggregate kind 9 (SUM(DISTINCT)); matches the convention in the Rust GroupAggState. */
  static final int KIND_SUM_DISTINCT = 9;

  /** Expanded native kinds, one entry per partial column (an AVG is a widened sum plus a count). */
  static int[] kinds(StreamPhysicalLocalGroupAggregate agg) {
    List<Integer> kinds = new ArrayList<>();
    Seq<AggregateCall> aggCalls = agg.aggCalls();
    for (int i = 0; i < aggCalls.size(); i++) {
      AggregateCall call = aggCalls.apply(i);
      int kind = WindowAggregateMatcher.aggregateKind(call.getAggregation().getKind());
      if (call.isDistinct()) {
        kinds.add(
            kind == WindowAggregateMatcher.KIND_COUNT ? KIND_COUNT_DISTINCT : KIND_SUM_DISTINCT);
      } else if (kind == WindowAggregateMatcher.KIND_AVG) {
        kinds.add(WindowAggregateMatcher.KIND_AVG_PARTIAL_SUM);
        kinds.add(WindowAggregateMatcher.KIND_COUNT);
      } else {
        kinds.add(kind);
      }
    }
    return toArray(kinds);
  }

  /** Expanded value columns (an AVG's sum and count states both read its value column). */
  static int[] valueColumns(StreamPhysicalLocalGroupAggregate agg) {
    List<Integer> columns = new ArrayList<>();
    Seq<AggregateCall> aggCalls = agg.aggCalls();
    for (int i = 0; i < aggCalls.size(); i++) {
      AggregateCall call = aggCalls.apply(i);
      int kind = WindowAggregateMatcher.aggregateKind(call.getAggregation().getKind());
      int column = call.getArgList().isEmpty() ? -1 : call.getArgList().get(0);
      columns.add(column);
      if (kind == WindowAggregateMatcher.KIND_AVG) {
        columns.add(column);
      }
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
      int kind = WindowAggregateMatcher.aggregateKind(call.getAggregation().getKind());
      if (call.isDistinct()) {
        // The distinct set is keyed by the value itself, so its code carries the value's own type.
        codes.add(
            WindowAggregateMatcher.typeCode(
                inputType.getFieldList().get(call.getArgList().get(0)).getType()));
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
        SqlTypeName valueType = valueRel.getSqlTypeName();
        codes.add(
            valueType == SqlTypeName.DECIMAL
                ? decimalCode(valueRel)
                : valueType == SqlTypeName.DOUBLE ? 1 : valueType == SqlTypeName.INTEGER ? 2 : 0);
      }
    }
    return toArray(codes);
  }

  /** The packed native code carrying a decimal's precision and scale. */
  private static int decimalCode(RelDataType type) {
    return io.github.jordepic.streamfusion.operator.NativeWindowOperatorCore.decimalCode(
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
}
