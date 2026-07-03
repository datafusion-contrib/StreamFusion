package io.github.jordepic.streamfusion.planner;

import io.github.jordepic.streamfusion.operator.RowDataArrowConverter;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory$;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalGroupAggregate;
import org.apache.flink.table.planner.plan.utils.ChangelogPlanUtils;
import org.apache.flink.table.planner.utils.ShortcutUtils;
import org.apache.flink.table.types.logical.RowType;
import scala.collection.Seq;

/**
 * Decides whether a non-windowed {@code GROUP BY} aggregate can run natively, and pulls out the
 * aggregate kinds, value columns/types, and grouping keys the operator needs.
 *
 * <p>Scope: SUM/MIN/MAX/COUNT over bigint/int/double value columns (SUM/MIN/MAX also decimal), plus
 * AVG over bigint/int/smallint/tinyint/float/double (a running sum + non-null count, result cast back
 * to the input type — decimal AVG falls back), with any grouping keys and pass-through columns the
 * row/Arrow conversion supports. DISTINCT is native for COUNT (a per-key value set), SUM (the set plus
 * a running sum folded as values enter/leave), and MIN/MAX (semantically their plain forms); only
 * AVG(DISTINCT) falls back. The input may be append-only or a changelog; SUM/COUNT/AVG retract a
 * running value and MIN/MAX retract via a per-key value multiset, so all work over either input.
 */
final class GroupAggregateMatcher {

  private GroupAggregateMatcher() {}

  static boolean matches(StreamPhysicalGroupAggregate agg) {
    // The native operator never expires idle keys and suppresses an unchanged result, which matches
    // the host only with state retention off. With a TTL set the host instead refreshes downstream
    // (emitting unchanged updates) and deletes expired keys, so leave it on the host.
    if (!ShortcutUtils.unwrapTableConfig(agg)
        .get(ExecutionConfigOptions.IDLE_STATE_RETENTION)
        .isZero()) {
      return false;
    }
    RelDataType inputType = agg.getInput().getRowType();
    // The whole row crosses the boundary in both directions, so every input and output column must be
    // a type the conversion handles (this also covers the grouping-key and pass-through types).
    if (!RowDataArrowConverter.supports(
            FlinkTypeFactory$.MODULE$.toLogicalRowType(inputType))
        || !RowDataArrowConverter.supports(
            FlinkTypeFactory$.MODULE$.toLogicalRowType(agg.getRowType()))) {
      return false;
    }
    Seq<AggregateCall> aggCalls = agg.aggCalls();
    for (int i = 0; i < aggCalls.size(); i++) {
      AggregateCall call = aggCalls.apply(i);
      // A FILTER (call.filterArg >= 0) is native — the filter is a boolean input column the operator
      // gates each fold on; only an approximate aggregate is rejected.
      if (call.isApproximate()) {
        return false;
      }
      int kind = WindowAggregateMatcher.aggregateKind(call.getAggregation().getKind());
      if (kind < 0) {
        return false; // SUM/MIN/MAX/COUNT/AVG only
      }
      if (call.getArgList().size() > 1) {
        return false;
      }
      // DISTINCT: COUNT(DISTINCT x) keeps a value→multiplicity set (Flink's DistinctAccumulator) over
      // any type the row admits; SUM(DISTINCT x) adds a running sum folded as values enter/leave the
      // set (same value types as plain SUM, gated below); MIN/MAX(DISTINCT x) are semantically the
      // plain MIN/MAX — the extreme of the live values ignores multiplicity — so they run as such.
      // AVG(DISTINCT) falls back (its count-of-distinct division isn't modelled).
      if (call.isDistinct()) {
        if (call.getArgList().size() != 1 || kind == WindowAggregateMatcher.KIND_AVG) {
          return false;
        }
        if (kind == WindowAggregateMatcher.KIND_COUNT) {
          continue;
        }
        // SUM/MIN/MAX DISTINCT fall through to their plain forms' value-type gates.
      }
      // SUM/MIN/MAX read a present argument as a typed running value, so it must be a running type or
      // DECIMAL: SUM folds an i128 at the input scale → DECIMAL(38, s); MIN/MAX keep the extreme as an
      // i128 → DECIMAL(p, s) — both matching Flink. COUNT only reads null-ness, so it counts any column
      // the row already admits (incl. a complex ARRAY/MAP/ROW value); COUNT(*) (no argument) is
      // unrestricted.
      if (!call.getArgList().isEmpty() && kind != WindowAggregateMatcher.KIND_COUNT) {
        SqlTypeName valueType =
            inputType.getFieldList().get(call.getArgList().get(0)).getType().getSqlTypeName();
        if (kind == WindowAggregateMatcher.KIND_AVG) {
          // AVG keeps a (sum, count) running state: sum widens to bigint/double, result casts back to
          // the input type. Over bigint/int/smallint/tinyint/float/double only; decimal AVG (whose
          // precision/scale derivation is not modelled) falls back.
          if (!isAvgType(valueType)) {
            return false;
          }
        } else if (kind == KIND_MIN || kind == KIND_MAX) {
          // MIN/MAX keep a value multiset; admit the running numerics, DECIMAL, and strings (ordered
          // byte-lexicographically, matching Flink's BinaryStringData comparison).
          if (!isRunningType(valueType) && valueType != SqlTypeName.DECIMAL && !isStringType(valueType)) {
            return false;
          }
        } else if (!isRunningType(valueType) && valueType != SqlTypeName.DECIMAL) {
          return false; // SUM
        }
      }
    }
    return true;
  }

  /** The value types the native running aggregate folds directly: bigint, int, double. */
  private static boolean isRunningType(SqlTypeName type) {
    return type == SqlTypeName.BIGINT
        || type == SqlTypeName.INTEGER
        || type == SqlTypeName.DOUBLE;
  }

  /** The numeric types Flink's AvgAggFunction covers (its result casts back to the input type). */
  private static boolean isAvgType(SqlTypeName type) {
    return type == SqlTypeName.TINYINT
        || type == SqlTypeName.SMALLINT
        || type == SqlTypeName.INTEGER
        || type == SqlTypeName.BIGINT
        || type == SqlTypeName.FLOAT
        || type == SqlTypeName.REAL
        || type == SqlTypeName.DOUBLE;
  }

  /** Native aggregate kind 7 (COUNT(DISTINCT)); matches the convention in the Rust GroupAggState. */
  private static final int KIND_COUNT_DISTINCT = 7;

  /** Native aggregate kind 9 (SUM(DISTINCT)); matches the convention in the Rust GroupAggState. */
  private static final int KIND_SUM_DISTINCT = 9;

  /** Native MIN/MAX kinds (the Rust GroupAggState routes these to the Extremes multiset). */
  private static final int KIND_MIN = 1;
  private static final int KIND_MAX = 2;

  /** Character string types MIN/MAX admit (compared byte-lexicographically). */
  private static boolean isStringType(SqlTypeName type) {
    return type == SqlTypeName.CHAR || type == SqlTypeName.VARCHAR;
  }

  static int[] kinds(StreamPhysicalGroupAggregate agg) {
    Seq<AggregateCall> aggCalls = agg.aggCalls();
    int[] kinds = new int[aggCalls.size()];
    for (int i = 0; i < aggCalls.size(); i++) {
      AggregateCall call = aggCalls.apply(i);
      int kind = WindowAggregateMatcher.aggregateKind(call.getAggregation().getKind());
      if (call.isDistinct() && kind == WindowAggregateMatcher.KIND_COUNT) {
        kind = KIND_COUNT_DISTINCT;
      } else if (call.isDistinct() && kind == WindowAggregateMatcher.KIND_SUM) {
        kind = KIND_SUM_DISTINCT;
      }
      // MIN/MAX(DISTINCT) stay their plain kinds: the extreme ignores multiplicity either way.
      kinds[i] = kind;
    }
    return kinds;
  }

  static int[] valueColumns(StreamPhysicalGroupAggregate agg) {
    return WindowAggregateMatcher.valueColumns(agg.aggCalls());
  }

  static int[] valueTypeCodes(StreamPhysicalGroupAggregate agg) {
    return WindowAggregateMatcher.valueTypeCodes(agg.aggCalls(), agg.getInput().getRowType());
  }

  static int[] keyColumns(StreamPhysicalGroupAggregate agg) {
    return WindowAggregateMatcher.keyColumns(agg.grouping());
  }

  /** The FILTER boolean column index per aggregate (Calcite's {@code filterArg}), or -1 if none. */
  static int[] filterColumns(StreamPhysicalGroupAggregate agg) {
    Seq<AggregateCall> aggCalls = agg.aggCalls();
    int[] filters = new int[aggCalls.size()];
    for (int i = 0; i < aggCalls.size(); i++) {
      filters[i] = aggCalls.apply(i).filterArg;
    }
    return filters;
  }

  /** Whether the host wants UPDATE_BEFORE rows emitted on this node's output edge. */
  static boolean generateUpdateBefore(StreamPhysicalGroupAggregate agg) {
    return ChangelogPlanUtils.generateUpdateBefore(agg);
  }
}
