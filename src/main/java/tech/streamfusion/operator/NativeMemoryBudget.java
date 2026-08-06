package tech.streamfusion.operator;

import java.util.concurrent.atomic.AtomicLong;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;

/** One native operator's ownership and attribution inside the TaskManager-wide off-heap pool. */
public final class NativeMemoryBudget implements AutoCloseable {

  public static final long UNACCOUNTED = -1;

  private final long ownerId;
  private final AtomicLong stateBytes = new AtomicLong();

  private NativeMemoryBudget(long ownerId) {
    this.ownerId = ownerId;
  }

  public static NativeMemoryBudget registerFor(AbstractStreamOperator<?> operator) {
    StreamingRuntimeContext context = operator.getRuntimeContext();
    TaskOffHeapMemory.initialize(context.getTaskManagerRuntimeInfo().getConfiguration());
    long ownerId =
        TaskOffHeapMemory.registerOwner("operator", context.getTaskInfo().getTaskNameWithSubtasks());
    TaskOffHeapMemory.registerMetrics(operator.getMetricGroup());
    return new NativeMemoryBudget(ownerId);
  }

  /** Negative values below -1 identify the JVM reservation owner while preserving legacy test caps. */
  public long nativeHandle() {
    return -ownerId - 1;
  }

  public void publishStateBytes(long value) {
    stateBytes.set(value);
  }

  public void registerStateMetric(MetricGroup group) {
    group.gauge("nativeStateBytes", stateBytes::get);
  }

  @Override
  public void close() {
    TaskOffHeapMemory.closeOwner(ownerId);
  }
}
