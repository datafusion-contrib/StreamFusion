package tech.streamfusion.planner.stream;

import java.util.List;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.flink.table.planner.plan.nodes.exec.spec.OverSpec;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks;
import tech.streamfusion.compat.FlinkPlanCompat;
import tech.streamfusion.planner.common.Admission;

/** Stock OVER extraction and the additional qualification required by canonical state binding. */
final class StockOverPlan {
  private final OverWindowSpec window;
  private final List<AggregateCall> calls;
  private final List<RexLiteral> constants;

  private StockOverPlan(OverWindowSpec window, OverSpec spec) {
    this.window = window;
    calls = List.copyOf(spec.getGroups().get(0).getAggCalls());
    constants = List.copyOf(spec.getConstants());
  }

  static Admission<StockOverPlan> prepare(OverSpec spec, RowType input) {
    if (spec.getGroups().size() != 1) return Admission.declined("OVER: only a single window group");
    int[] keys = spec.getPartition().getFieldIndices();
    if (keys.length > 1 || keys.length == 1 && !input.getTypeAt(keys[0]).is(LogicalTypeRoot.BIGINT))
      return Admission.declined("OVER: canonical state requires at most one BIGINT partition key");
    if (input.getChildren().stream().anyMatch(type ->
        !NativeOverAggExecNode.stockArithmeticValueType(type)
            && !((type.is(LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE)
                    || type.is(LogicalTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE))
                && LogicalTypeChecks.getPrecision(type) <= 3)))
      return Admission.declined("OVER: input types are not qualified for canonical arithmetic state");
    var frame = OverWindowSpec.compiled(spec, input);
    if (!frame.isAccepted()) return Admission.declined(frame.reason());
    if (frame.value().bounded() && frame.value().rows() && frame.value().offset() == Long.MAX_VALUE)
      return Admission.declined("OVER: canonical ROWS frame row count overflows");
    return Admission.accepted(new StockOverPlan(frame.value(), spec));
  }

  boolean acceptsStockFunction(Object function) {
    Class<?> expected = !window.bounded()
        ? window.processingTime()
            ? org.apache.flink.table.runtime.operators.over.ProcTimeUnboundedPrecedingFunction.class
            : FlinkPlanCompat.eventUnboundedOver(window.rows())
        : window.processingTime()
            ? org.apache.flink.table.runtime.operators.over.ProcTimeRowsBoundedPrecedingFunction.class
            : window.rows()
                ? org.apache.flink.table.runtime.operators.over.RowTimeRowsBoundedPrecedingFunction.class
                : org.apache.flink.table.runtime.operators.over.RowTimeRangeBoundedPrecedingFunction.class;
    return function.getClass() == expected;
  }

  OverWindowSpec window() { return window; }
  List<AggregateCall> calls() { return calls; }
  List<RexLiteral> constants() { return constants; }
}
