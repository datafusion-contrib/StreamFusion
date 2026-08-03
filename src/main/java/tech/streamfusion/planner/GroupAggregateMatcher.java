package tech.streamfusion.planner;

import tech.streamfusion.operator.RowDataArrowConverter;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory$;
import org.apache.flink.table.planner.hint.StateTtlHint;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalGroupAggregate;
import org.apache.flink.table.planner.plan.utils.ChangelogPlanUtils;
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
    return unsupportedReason(agg) == null;
  }

  /** The specific reason this aggregate is not accelerable, or null if it is. */
  static String unsupportedReason(StreamPhysicalGroupAggregate agg) {
    RelDataType inputType = agg.getInput().getRowType();
    // The whole row crosses the boundary in both directions, so every input and output column must be
    // a type the conversion handles (this also covers the grouping-key and pass-through types).
    if (!RowDataArrowConverter.supports(
            FlinkTypeFactory$.MODULE$.toLogicalRowType(inputType))
        || !RowDataArrowConverter.supports(
            FlinkTypeFactory$.MODULE$.toLogicalRowType(agg.getRowType()))) {
      return "GROUP BY: an input or output column type the boundary cannot carry";
    }
    Seq<AggregateCall> aggCalls = agg.aggCalls();
    for (int i = 0; i < aggCalls.size(); i++) {
      AggregateCall call = aggCalls.apply(i);
      // A FILTER (call.filterArg >= 0) is native — the filter is a boolean input column the operator
      // gates each fold on; only an approximate aggregate is rejected.
      if (call.isApproximate()) {
        return "GROUP BY: an approximate aggregate";
      }
      int kind = WindowAggregateMatcher.aggregateKind(call.getAggregation().getKind());
      if (kind < 0) {
        return "GROUP BY: only SUM/MIN/MAX/COUNT/AVG aggregates";
      }
      if (call.getArgList().size() > 1) {
        return "GROUP BY: only single-argument aggregates";
      }
      // DISTINCT: COUNT(DISTINCT x) keeps a value→multiplicity set (Flink's DistinctAccumulator) over
      // any type the row admits; SUM(DISTINCT x) adds a running sum folded as values enter/leave the
      // set (same value types as plain SUM, gated below); MIN/MAX(DISTINCT x) are semantically the
      // plain MIN/MAX — the extreme of the live values ignores multiplicity — so they run as such.
      // AVG(DISTINCT) falls back (its count-of-distinct division isn't modelled).
      if (call.isDistinct()) {
        if (call.getArgList().size() != 1 || kind == WindowAggregateMatcher.KIND_AVG) {
          return "GROUP BY: DISTINCT is native for COUNT/SUM/MIN/MAX only";
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
          // AVG keeps a (sum, count) running state: for the numerics, the sum widens to bigint/double
          // and the result casts back to the input type; for DECIMAL(p, s), the sum is SUM's
          // DECIMAL(38, s) accumulator and the emit divides with Flink's exact decimal division,
          // reporting DECIMAL(38, max(6, s)) — findAvgAggType's derivation.
          if (!isAvgType(valueType) && valueType != SqlTypeName.DECIMAL) {
            return "GROUP BY: AVG over an unsupported value type";
          }
        } else if (kind == KIND_MIN || kind == KIND_MAX) {
          // MIN/MAX keep a value multiset; admit the running numerics, DECIMAL, and strings (ordered
          // byte-lexicographically, matching Flink's BinaryStringData comparison).
          if (!isRunningType(valueType) && valueType != SqlTypeName.DECIMAL && !isStringType(valueType)) {
            return "GROUP BY: MIN/MAX over an unsupported value type";
          }
        } else if (!isRunningType(valueType) && valueType != SqlTypeName.DECIMAL) {
          return "GROUP BY: SUM over an unsupported value type";
        }
      }
    }
    return null;
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

  private static final int KIND_MIN = WindowAggregateMatcher.KIND_MIN;
  private static final int KIND_MAX = WindowAggregateMatcher.KIND_MAX;

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

  static RelNode substitute(StreamPhysicalGroupAggregate agg, PlanContext ctx) {
    int[] keyColumns = GroupAggregateMatcher.keyColumns(agg);
    // A STATE_TTL hint overrides the job-wide retention for this aggregate alone (Flink's
    // StateMetadata precedence); null means no hint, resolved at translate time.
    Long stateTtlHint = StateTtlHint.getStateTtlFromHintOnSingleRel(agg.hints());
    // The aggregate is columnar (Arrow in/out). Keep the keyed shuffle columnar where the input
    // sits on a columnar producer (a native exchange splits the batch by the grouping keys);
    // otherwise the transition pass inserts a transpose at the host exchange boundary. Same key
    // co-location argument as the window aggregate (divergences/10).
    return new StreamPhysicalNativeColumnarGroupAggregate(
        agg.getCluster(),
        agg.getTraitSet(),
        ctx.columnarInput(agg.getInputs().get(0), keyColumns),
        agg.getRowType(),
        GroupAggregateMatcher.kinds(agg),
        GroupAggregateMatcher.valueTypeCodes(agg),
        GroupAggregateMatcher.valueColumns(agg),
        keyColumns,
        GroupAggregateMatcher.filterColumns(agg),
        new int[0], // single-phase: no AVG-merge count partials
        new int[0], // single-phase: distinct values fold per row, no view columns
        -1, // single-phase: liveness counts rows ±1, no count1 partial
        GroupAggregateMatcher.generateUpdateBefore(agg),
        stateTtlHint == null ? -1 : stateTtlHint);
  }
}
