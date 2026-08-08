package tech.streamfusion.planner;

import tech.streamfusion.operator.ArrowBatch;
import org.apache.flink.runtime.io.network.api.writer.SubtaskStateMapper;
import org.apache.flink.runtime.plugable.SerializationDelegate;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.streaming.runtime.partitioner.ConfigurableStreamPartitioner;
import org.apache.flink.streaming.runtime.partitioner.StreamPartitioner;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

/**
 * Routes a key-partitioned Arrow batch from its stable key group to the channel that owns that
 * group at the current parallelism. Flink can therefore apply the same partitioner while restoring
 * channel state after an unaligned checkpoint and rescaling.
 */
public class ColumnarKeyGroupPartitioner extends StreamPartitioner<ArrowBatch>
    implements ConfigurableStreamPartitioner {

  private static final long serialVersionUID = 1L;

  private int maxParallelism;

  public ColumnarKeyGroupPartitioner(int maxParallelism) {
    configure(maxParallelism);
  }

  @Override
  public int selectChannel(SerializationDelegate<StreamRecord<ArrowBatch>> record) {
    int keyGroup = record.getInstance().getValue().keyGroup();
    if (keyGroup < 0) {
      return 0;
    }
    if (keyGroup >= maxParallelism) {
      throw new IllegalArgumentException(
          "Arrow batch key group " + keyGroup + " exceeds max parallelism " + maxParallelism);
    }
    return KeyGroupRangeAssignment.computeOperatorIndexForKeyGroup(
        maxParallelism, numberOfChannels, keyGroup);
  }

  @Override
  public StreamPartitioner<ArrowBatch> copy() {
    return this;
  }

  @Override
  public SubtaskStateMapper getDownstreamSubtaskStateMapper() {
    return SubtaskStateMapper.RANGE;
  }

  @Override
  public boolean isPointwise() {
    return false;
  }

  @Override
  public void configure(int maxParallelism) {
    KeyGroupRangeAssignment.checkParallelismPreconditions(maxParallelism);
    this.maxParallelism = maxParallelism;
  }

  @Override
  public String toString() {
    return "columnar-key-group";
  }
}
