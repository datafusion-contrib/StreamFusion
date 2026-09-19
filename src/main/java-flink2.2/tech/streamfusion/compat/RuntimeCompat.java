package tech.streamfusion.compat;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.runtime.operators.coordination.OperatorEventDispatcher;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

public final class RuntimeCompat {
  private RuntimeCompat() {}

  public static org.apache.flink.table.data.RowData copyRow(
      org.apache.flink.table.runtime.typeutils.RowDataSerializer serializer,
      org.apache.flink.table.data.RowData row) {
    return serializer.copy(row);
  }

  public static int attempt(RuntimeContext context) {
    return context.getTaskInfo().getAttemptNumber();
  }

  public static String taskName(RuntimeContext context) {
    return context.getTaskInfo().getTaskNameWithSubtasks();
  }

  public static int parallelism(RuntimeContext context) {
    return context.getTaskInfo().getNumberOfParallelSubtasks();
  }

  public static int subtask(RuntimeContext context) {
    return context.getTaskInfo().getIndexOfThisSubtask();
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
        dispatcher,
        parameters.getMailboxExecutor());
  }
}
