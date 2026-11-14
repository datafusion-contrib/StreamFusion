package tech.streamfusion.compat;

import org.apache.flink.streaming.runtime.partitioner.StreamPartitioner;

/**
 * Flink 1.18 relies on the planner's aligned/recoverable exchange selection instead of an edge
 * flag.
 */
public abstract class ColumnarPartitionerCompat<T> extends StreamPartitioner<T> {
  protected final void disableUnalignedCheckpoints() {}
}
