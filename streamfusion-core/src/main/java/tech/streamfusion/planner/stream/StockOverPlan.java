package tech.streamfusion.planner.stream;

import java.util.List;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.flink.table.planner.plan.nodes.exec.spec.OverSpec;
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
    if (java.util.Arrays.stream(keys)
        .anyMatch(
            key ->
                !tech.streamfusion.state.StockOverFoldStateContract.supportsProcessingPartitionKey(
                    input.getTypeAt(key))))
      return Admission.declined("OVER: unsupported canonical partition key");
    if (!tech.streamfusion.operator.RowDataArrowConverter.supports(input))
      return Admission.declined("OVER: unsupported Arrow input schema");
    var frame = OverWindowSpec.compiled(spec, input);
    if (!frame.isAccepted()) return Admission.declined(frame.reason());
    if (LogicalTypeChecks.getPrecision(input.getTypeAt(frame.value().timeColumn())) > 3)
      return Admission.declined("OVER: canonical time precision exceeds milliseconds");
    if (frame.value().bounded() && frame.value().rows() && frame.value().offset() == Long.MAX_VALUE)
      return Admission.declined("OVER: canonical ROWS frame row count overflows");
    return Admission.accepted(new StockOverPlan(frame.value(), spec));
  }

  boolean acceptsStockFunction(Object function) {
    Class<?> expected =
        !window.bounded()
            ? window.processingTime()
                ? org.apache.flink.table.runtime.operators.over.ProcTimeUnboundedPrecedingFunction
                    .class
                : FlinkPlanCompat.eventUnboundedOver(window.rows())
            : window.processingTime()
                ? org.apache.flink.table.runtime.operators.over.ProcTimeRowsBoundedPrecedingFunction
                    .class
                : window.rows()
                    ? org.apache.flink.table.runtime.operators.over
                        .RowTimeRowsBoundedPrecedingFunction.class
                    : org.apache.flink.table.runtime.operators.over
                        .RowTimeRangeBoundedPrecedingFunction.class;
    return function.getClass() == expected
        || !window.bounded()
            && !window.processingTime()
            && function.getClass() == FlinkPlanCompat.eventUnboundedOver(window.rows(), 1);
  }

  int eventTimeVersion(Object function) {
    return !window.bounded()
            && !window.processingTime()
            && function.getClass() == FlinkPlanCompat.eventUnboundedOver(window.rows(), 1)
        ? 1
        : FlinkPlanCompat.unboundedOverVersion();
  }

  OverWindowSpec window() {
    return window;
  }

  List<AggregateCall> calls() {
    return calls;
  }

  List<RexLiteral> constants() {
    return constants;
  }
}
