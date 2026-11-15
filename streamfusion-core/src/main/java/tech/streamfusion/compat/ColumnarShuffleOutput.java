package tech.streamfusion.compat;

import static org.apache.flink.util.Preconditions.checkState;

import java.util.List;
import org.apache.flink.runtime.io.network.api.writer.ResultPartitionWriter;
import org.apache.flink.streaming.api.graph.NonChainedOutput;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import tech.streamfusion.planner.ColumnarKeyGroupPartitioner;

/** Resolves the deployed keyed output, including when its splitter is chained into a producer. */
public final class ColumnarShuffleOutput {
  private ColumnarShuffleOutput() {}

  public static int parallelism(
      AbstractStreamOperator<?> operator,
      int maxParallelism,
      boolean recoverable,
      boolean singleton) {
    return parallelism(operator, maxParallelism, recoverable, singleton, false);
  }

  public static int parallelism(
      AbstractStreamOperator<?> operator,
      int maxParallelism,
      boolean recoverable,
      boolean singleton,
      boolean batch) {
    return layout(operator, maxParallelism, recoverable, singleton, batch)[0];
  }

  public static int[] layout(
      AbstractStreamOperator<?> operator,
      int maxParallelism,
      boolean recoverable,
      boolean singleton,
      boolean batch) {
    ClassLoader loader = operator.getUserCodeClassloader();
    List<NonChainedOutput> outputs =
        operator.getOperatorConfig().getOperatorNonChainedOutputs(loader);
    checkState(
        outputs.size() == 1 && operator.getOperatorConfig().getChainedOutputs(loader).isEmpty(),
        "Native shuffle splitter requires exactly one non-chained keyed output: batch=%s outputs=%s"
            + " chained=%s",
        batch,
        outputs,
        operator.getOperatorConfig().getChainedOutputs(loader));
    NonChainedOutput output = outputs.get(0);
    checkState(
        output.getPartitioner() instanceof ColumnarKeyGroupPartitioner
            && (batch
                ? output.getPartitionType().isBlockingOrBlockingPersistentResultPartition()
                : output.getPartitionType().mustBePipelinedConsumed()),
        "Native shuffle splitter requires a columnar key-group partitioner in its configured mode:"
            + " batch=%s partitioner=%s partitionType=%s",
        batch,
        output.getPartitioner(),
        output.getPartitionType());
    ColumnarKeyGroupPartitioner partitioner = (ColumnarKeyGroupPartitioner) output.getPartitioner();
    checkState(
        partitioner.requiresSingleton() == singleton,
        "Native shuffle splitter and partitioner disagree on SINGLETON distribution");
    checkState(
        partitioner.maxParallelism() == maxParallelism && partitioner.recoverable() == recoverable,
        "Native shuffle splitter and partitioner disagree on key groups or recovery protocol");

    List<NonChainedOutput> taskOutputs =
        operator.getContainingTask().getConfiguration().getVertexNonChainedOutputs(loader);
    // Task writer order follows the vertex outputs, not this chained operator's output list.
    int writerIndex = taskOutputs.indexOf(output);
    checkState(writerIndex >= 0, "Native shuffle output is missing from the task output list");
    checkState(
        writerIndex < operator.getContainingTask().getEnvironment().getAllWriters().length,
        "Native shuffle output writer %s is missing from the task environment",
        writerIndex);
    ResultPartitionWriter writer =
        operator.getContainingTask().getEnvironment().getWriter(writerIndex);
    int targetKeyGroups = writer.getNumTargetKeyGroups();
    int channels = writer.getNumberOfSubpartitions();
    if (singleton) {
      checkState(
          channels == 1,
          "Native SINGLETON shuffle requires exactly one downstream channel, deployed %s",
          channels);
    }
    // Batch scheduling does not retain the streaming key-group state namespace. Bind routing to
    // the deployed writer; its target parallelism may still be unresolved in NonChainedOutput.
    checkState(
        batch || targetKeyGroups == maxParallelism,
        "Native shuffle max parallelism changed after planning: expected %s key groups, deployed"
            + " %s. Keep max parallelism stable for native keyed state; ordinary parallelism may"
            + " change.",
        maxParallelism,
        targetKeyGroups);
    checkState(
        channels > 0 && (batch || channels <= maxParallelism),
        "Native shuffle has invalid runtime channel count %s for %s key groups",
        channels,
        maxParallelism);
    checkState(
        batch || output.getConsumerParallelism() == channels,
        "Native shuffle requires finalized consumer parallelism: consumer parallelism %s, channels"
            + " %s",
        output.getConsumerParallelism(),
        channels);
    int routingGroups = batch ? targetKeyGroups : maxParallelism;
    checkState(routingGroups >= channels, "Native shuffle has fewer routing groups than channels");
    return new int[] {channels, routingGroups};
  }
}
