package tech.streamfusion.operator;

import java.util.List;
import org.apache.flink.runtime.io.network.api.writer.ResultPartitionWriter;
import org.apache.flink.runtime.io.network.partition.MockResultPartitionWriter;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.streaming.api.graph.NonChainedOutput;
import org.apache.flink.streaming.runtime.partitioner.StreamPartitioner;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import tech.streamfusion.planner.ColumnarKeyGroupPartitioner;

final class ColumnarShuffleTestOutput {
  private ColumnarShuffleTestOutput() {}

  static void configure(
      OneInputStreamOperatorTestHarness<ArrowBatch, ArrowBatch> harness,
      int channels,
      int maxParallelism,
      boolean recoverable) {
    NonChainedOutput output =
        output(
            channels, maxParallelism, new ColumnarKeyGroupPartitioner(maxParallelism, recoverable));
    configure(harness, List.of(output), List.of(output), List.of(writer(channels, maxParallelism)));
  }

  static void configure(
      OneInputStreamOperatorTestHarness<ArrowBatch, ArrowBatch> harness,
      List<NonChainedOutput> operatorOutputs,
      List<NonChainedOutput> taskOutputs,
      List<ResultPartitionWriter> writers) {
    harness.getStreamConfig().setOperatorNonChainedOutputs(operatorOutputs);
    harness.getStreamConfig().setVertexNonChainedOutputs(taskOutputs);
    harness.getStreamConfig().serializeAllConfigs();
    harness.getEnvironment().addOutputs(writers);
  }

  static NonChainedOutput output(
      int channels, int maxParallelism, StreamPartitioner<?> partitioner) {
    return new NonChainedOutput(
        true,
        1,
        channels,
        maxParallelism,
        0,
        false,
        new IntermediateDataSetID(),
        null,
        partitioner,
        ResultPartitionType.PIPELINED_BOUNDED);
  }

  static ResultPartitionWriter writer(int channels, int maxParallelism) {
    return new MockResultPartitionWriter() {
      @Override
      public int getNumberOfSubpartitions() {
        return channels;
      }

      @Override
      public int getNumTargetKeyGroups() {
        return maxParallelism;
      }
    };
  }
}
