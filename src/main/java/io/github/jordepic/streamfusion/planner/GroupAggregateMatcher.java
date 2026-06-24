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
 * <p>Scope of the first changelog-emitting operator: SUM/MIN/MAX/COUNT (no AVG — its multi-field
 * running state is not modelled yet) over bigint/int/double value columns, with any grouping keys
 * and pass-through columns the row/Arrow conversion supports. The caller separately requires the
 * input to be insert-only (append-only), so the count-reaches-zero delete case cannot arise.
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
      if (call.isDistinct() || call.isApproximate() || call.filterArg >= 0) {
        return false;
      }
      int kind = WindowAggregateMatcher.aggregateKind(call.getAggregation().getKind());
      if (kind < 0 || kind == WindowAggregateMatcher.KIND_AVG) {
        return false; // SUM/MIN/MAX/COUNT only
      }
      if (call.getArgList().size() > 1) {
        return false;
      }
      // A present argument column is read as a running value, so it must be one of the running types;
      // COUNT(*) (no argument) is unrestricted.
      if (!call.getArgList().isEmpty()) {
        SqlTypeName valueType =
            inputType.getFieldList().get(call.getArgList().get(0)).getType().getSqlTypeName();
        if (!isRunningType(valueType)) {
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

  static int[] kinds(StreamPhysicalGroupAggregate agg) {
    return WindowAggregateMatcher.kinds(agg.aggCalls());
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

  /** Whether the host wants UPDATE_BEFORE rows emitted on this node's output edge. */
  static boolean generateUpdateBefore(StreamPhysicalGroupAggregate agg) {
    return ChangelogPlanUtils.generateUpdateBefore(agg);
  }
}
