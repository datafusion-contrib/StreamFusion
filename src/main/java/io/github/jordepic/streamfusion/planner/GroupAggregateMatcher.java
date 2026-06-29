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
 * row/Arrow conversion supports. The input may be append-only or a changelog; SUM/COUNT/AVG retract a
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
      // DISTINCT is native only for COUNT(DISTINCT x) over one argument — the value→multiplicity set
      // counts distinct values (Flink's DistinctAccumulator). SUM/MIN/MAX DISTINCT fall back. The
      // value may be any type the row already admits (read as a scalar set key).
      if (call.isDistinct()) {
        if (kind != WindowAggregateMatcher.KIND_COUNT || call.getArgList().isEmpty()) {
          return false;
        }
        continue;
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
        } else if (!isRunningType(valueType) && valueType != SqlTypeName.DECIMAL) {
          return false;
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

  static int[] kinds(StreamPhysicalGroupAggregate agg) {
    Seq<AggregateCall> aggCalls = agg.aggCalls();
    int[] kinds = new int[aggCalls.size()];
    for (int i = 0; i < aggCalls.size(); i++) {
      AggregateCall call = aggCalls.apply(i);
      int kind = WindowAggregateMatcher.aggregateKind(call.getAggregation().getKind());
      kinds[i] =
          call.isDistinct() && kind == WindowAggregateMatcher.KIND_COUNT
              ? KIND_COUNT_DISTINCT
              : kind;
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
