package tech.streamfusion.planner;

import java.util.Objects;
import org.apache.flink.runtime.io.network.api.writer.SubtaskStateMapper;
import org.apache.flink.runtime.plugable.SerializationDelegate;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.streaming.runtime.partitioner.ConfigurableStreamPartitioner;
import org.apache.flink.streaming.runtime.partitioner.StreamPartitioner;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import tech.streamfusion.operator.ArrowBatch;

/**
 * Routes an Arrow shuffle record using its key-group tag. Destination-batched records force the
 * edge aligned; recovery-mode records contain exactly one key group and support Flink's ordinary
 * unaligned {@link SubtaskStateMapper#RANGE} filtering after rescaling.
 */
public class ColumnarKeyGroupPartitioner
    extends tech.streamfusion.compat.ColumnarPartitionerCompat<ArrowBatch>
    implements ConfigurableStreamPartitioner {

  private static final long serialVersionUID = 1L;

  private int maxParallelism;
  private final boolean recoverable;
  private final boolean singleton;

  public ColumnarKeyGroupPartitioner(int maxParallelism) {
    this(maxParallelism, false);
  }

  public ColumnarKeyGroupPartitioner(int maxParallelism, boolean recoverable) {
    this(maxParallelism, recoverable, false);
  }

  private ColumnarKeyGroupPartitioner(int maxParallelism, boolean recoverable, boolean singleton) {
    this.recoverable = recoverable;
    this.singleton = singleton;
    configure(maxParallelism);
    if (!recoverable) {
      disableUnalignedCheckpoints();
    }
  }

  /** Creates a single-destination exchange without changing its state key-group namespace. */
  public static ColumnarKeyGroupPartitioner singleton(int maxParallelism, boolean recoverable) {
    return new ColumnarKeyGroupPartitioner(maxParallelism, recoverable, true);
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
    ColumnarKeyGroupPartitioner copy =
        new ColumnarKeyGroupPartitioner(maxParallelism, recoverable, singleton);
    // Flink 1.18's recovery filter copies after setup and does not configure the copy again.
    copy.setup(numberOfChannels);
    return copy;
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

  public int maxParallelism() {
    return maxParallelism;
  }

  public boolean recoverable() {
    return recoverable;
  }

  /**
   * Whether the distribution requires one downstream subtask, even after resource planning. This
   * constraint is independent of maximum parallelism and the current channel count.
   */
  public boolean requiresSingleton() {
    return singleton;
  }

  @Override
  public boolean equals(Object other) {
    if (!super.equals(other)) {
      return false;
    }
    ColumnarKeyGroupPartitioner that = (ColumnarKeyGroupPartitioner) other;
    return maxParallelism == that.maxParallelism
        && recoverable == that.recoverable
        && singleton == that.singleton;
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), maxParallelism, recoverable, singleton);
  }

  @Override
  public String toString() {
    return "columnar-key-group";
  }
}
