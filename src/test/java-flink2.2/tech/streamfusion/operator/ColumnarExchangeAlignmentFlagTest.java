package tech.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.apache.flink.runtime.io.network.api.writer.SubtaskStateMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.StreamEdge;
import org.apache.flink.streaming.api.transformations.PartitionTransformation;
import org.apache.flink.streaming.api.transformations.StreamExchangeMode;
import org.junit.jupiter.api.Test;
import tech.streamfusion.compat.ListCollectors;
import tech.streamfusion.planner.ColumnarKeyGroupPartitioner;

class ColumnarExchangeAlignmentFlagTest {
  @Test
  void partitionerUsesKeyGroupRangeRecoveryAndRequiresAlignedChannelState() {
    ColumnarKeyGroupPartitioner partitioner = new ColumnarKeyGroupPartitioner(128);
    assertEquals(SubtaskStateMapper.RANGE, partitioner.getDownstreamSubtaskStateMapper());
    assertFalse(
        partitioner.isSupportsUnalignedCheckpoint(),
        "a channel batch can span key groups that separate after rescaling");
  }

  @Test
  void recoverablePartitionerSupportsUnalignedRangeChannelState() {
    ColumnarKeyGroupPartitioner partitioner = new ColumnarKeyGroupPartitioner(128, true);
    assertEquals(SubtaskStateMapper.RANGE, partitioner.getDownstreamSubtaskStateMapper());
    assertTrue(partitioner.isSupportsUnalignedCheckpoint());
    assertTrue(partitioner.copy().isSupportsUnalignedCheckpoint());
  }

  @Test
  void streamGraphForcesAlignedColumnarExchange() {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.enableCheckpointing(10);
    env.getCheckpointConfig().enableUnalignedCheckpoints();
    DataStream<ArrowBatch> input =
        tech.streamfusion.compat.FlinkTestSources.fromData(env, 1)
            .map(ignored -> (ArrowBatch) null)
            .returns(ArrowBatchTypeInformation.INSTANCE);
    PartitionTransformation<ArrowBatch> partition =
        new PartitionTransformation<>(
            input.getTransformation(),
            new ColumnarKeyGroupPartitioner(128),
            StreamExchangeMode.PIPELINED);
    tech.streamfusion.compat.StreamTestSinks.discard(new DataStream<>(env, partition));

    List<StreamEdge> columnarEdges =
        env.getStreamGraph().getStreamNodes().stream()
            .flatMap(node -> node.getOutEdges().stream())
            .filter(edge -> edge.getPartitioner() instanceof ColumnarKeyGroupPartitioner)
            .collect(ListCollectors.toList());
    assertEquals(1, columnarEdges.size());
    assertFalse(columnarEdges.get(0).supportsUnalignedCheckpoints());
  }
}
