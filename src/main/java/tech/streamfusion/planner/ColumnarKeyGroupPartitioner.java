package tech.streamfusion.planner;

import tech.streamfusion.operator.ArrowBatch;
import org.apache.flink.runtime.io.network.api.writer.SubtaskStateMapper;
import org.apache.flink.runtime.plugable.SerializationDelegate;
import org.apache.flink.streaming.runtime.partitioner.StreamPartitioner;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

/**
 * Routes a key-partitioned Arrow batch to the channel its split assigned it. Paired with {@link
 * tech.streamfusion.operator.SplitByKeyGroupOperator}, which emits each sub-batch
 * tagged with its destination channel, this keeps a keyed exchange columnar: each record carries one
 * destination, so Flink's one-record-to-one-channel partitioning routes a whole batch.
 */
public class ColumnarKeyGroupPartitioner extends StreamPartitioner<ArrowBatch> {

  public ColumnarKeyGroupPartitioner() {
    // An Arrow record is a batch that was split for the topology that produced it. An unaligned
    // checkpoint can retain that record in channel state, but after a rescale one old batch may
    // span several new key-group ranges and Flink's recovery filter can only keep or drop the whole
    // record. Force this edge aligned so recovery always replays rows through a splitter configured
    // for the restored topology.
    disableUnalignedCheckpoints();
  }

  @Override
  public int selectChannel(SerializationDelegate<StreamRecord<ArrowBatch>> record) {
    int destination = record.getInstance().getValue().destination();
    // A split tags each sub-batch with its channel; an unrouted batch (none) goes to channel 0.
    return destination >= 0 ? destination % numberOfChannels : 0;
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
  public String toString() {
    return "columnar-key-group";
  }
}
