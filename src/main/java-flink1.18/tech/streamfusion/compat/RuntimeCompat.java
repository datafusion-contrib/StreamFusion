package tech.streamfusion.compat;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.runtime.operators.coordination.OperatorEventDispatcher;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

public final class RuntimeCompat {
  private RuntimeCompat() {}

  public static org.apache.flink.table.runtime.generated.GeneratedProjection
      tableFunctionProjection(
          org.apache.flink.configuration.ReadableConfig config,
          ClassLoader classLoader,
          org.apache.flink.table.types.logical.RowType inputType,
          org.apache.flink.table.types.logical.RowType outputType,
          int[] projection) {
    return org.apache.flink.table.planner.codegen.ProjectionCodeGenerator$.MODULE$
        .generateProjection(
            new org.apache.flink.table.planner.codegen.CodeGeneratorContext(config, classLoader),
            "TableFunctionProjection",
            inputType,
            outputType,
            projection,
            org.apache.flink.table.data.BoxedWrapperRowData.class,
            "in1",
            "out",
            "outWriter",
            true);
  }

  public static int applyParsedTimePrecision(int millis, int precision) {
    return millis;
  }

  public static boolean supportsNonTimeOverRowNumber() {
    return false;
  }

  public static boolean supportsCharOverValueSelection(String function, boolean retracting) {
    return "MIN".equals(function) && !retracting;
  }

  public static boolean supportsTemporalOverFirstLast() {
    return false;
  }

  public static boolean windowTvfPreservesRowKinds() {
    return false;
  }

  public static org.apache.flink.table.data.RowData copyRow(
      org.apache.flink.table.runtime.typeutils.RowDataSerializer serializer,
      org.apache.flink.table.data.RowData row) {
    // In 1.18, copying a custom ArrayData view can return the serializer's reused buffer.
    return serializer.toBinaryRow(row).copy();
  }

  public static int attempt(RuntimeContext context) {
    return context.getAttemptNumber();
  }

  public static String taskName(RuntimeContext context) {
    return context.getTaskNameWithSubtasks();
  }

  public static int parallelism(RuntimeContext context) {
    return context.getNumberOfParallelSubtasks();
  }

  public static int maxParallelism(RuntimeContext context) {
    return context.getMaxNumberOfParallelSubtasks();
  }

  public static int subtask(RuntimeContext context) {
    return context.getIndexOfThisSubtask();
  }

  public static <T> StreamOperatorParameters<T> withOutput(
      StreamOperatorParameters<?> parameters, Output<StreamRecord<T>> output) {
    return withOutput(parameters, output, parameters.getOperatorEventDispatcher());
  }

  public static <T> StreamOperatorParameters<T> withOutput(
      StreamOperatorParameters<?> parameters,
      Output<StreamRecord<T>> output,
      OperatorEventDispatcher dispatcher) {
    return new StreamOperatorParameters<>(
        parameters.getContainingTask(),
        parameters.getStreamConfig(),
        output,
        parameters::getProcessingTimeService,
        dispatcher);
  }
}
