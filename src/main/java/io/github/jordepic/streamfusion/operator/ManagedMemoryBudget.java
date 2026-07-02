package io.github.jordepic.streamfusion.operator;

import java.util.concurrent.atomic.AtomicLong;
import org.apache.flink.core.memory.ManagedMemoryUseCase;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.memory.MemoryReservationException;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;

/**
 * A native operator's managed-memory budget: the operator-scope managed-memory fraction Flink
 * computed from the weight its transformation declared, reserved from the slot's {@link
 * MemoryManager} for the operator's lifetime and released at close. The native side enforces the
 * budget itself (a bounded DataFusion memory pool of this size), so state growth is bounded with no
 * JNI crossing on the hot path — the reserve-up-front model Flink's own off-heap consumer (the
 * RocksDB block cache) uses, rather than per-allocation reservation, which Flink's binary {@code
 * reserveMemory} (grant or throw, no partial grants and no cooperative spill) does not support the
 * way Spark's task memory manager supports Comet.
 *
 * <p>A transformation that declared no weight (an operator built outside the native planner, or
 * accounting switched off) yields no budget: the native side then runs unaccounted, exactly as
 * before accounting existed.
 */
public final class ManagedMemoryBudget implements AutoCloseable {

  /** The budget value meaning "no budget reserved; run unaccounted". */
  public static final long UNBOUNDED = -1;

  private static final ManagedMemoryBudget NONE = new ManagedMemoryBudget(null, UNBOUNDED);

  private final MemoryManager memoryManager;
  private final long bytes;
  // The native side's tracked state footprint, sampled by the operator on the task thread after
  // each batch (handles are not thread-safe) and read here by the metrics reporter's thread.
  private final AtomicLong stateBytes = new AtomicLong();

  private ManagedMemoryBudget(MemoryManager memoryManager, long bytes) {
    this.memoryManager = memoryManager;
    this.bytes = bytes;
  }

  /**
   * Reserves the operator's managed-memory budget, sized by the operator-scope fraction of the
   * slot's managed memory (zero when the transformation declared no weight — then nothing is
   * reserved and the budget is {@link #UNBOUNDED}).
   */
  public static ManagedMemoryBudget reserveFor(AbstractStreamOperator<?> operator)
      throws MemoryReservationException {
    StreamingRuntimeContext runtimeContext = operator.getRuntimeContext();
    double fraction =
        operator
            .getOperatorConfig()
            .getManagedMemoryFractionOperatorUseCaseOfSlot(
                ManagedMemoryUseCase.OPERATOR,
                runtimeContext.getJobConfiguration(),
                runtimeContext.getTaskManagerRuntimeInfo().getConfiguration(),
                runtimeContext.getUserCodeClassLoader());
    if (fraction <= 0) {
      // No declared weight (an operator whose exec node does not account yet, or a harness-built
      // operator); computeMemorySize rejects a zero fraction, so bail out before it.
      return NONE;
    }
    MemoryManager memoryManager =
        operator.getContainingTask().getEnvironment().getMemoryManager();
    long bytes = memoryManager.computeMemorySize(fraction);
    if (bytes <= 0) {
      return NONE;
    }
    ManagedMemoryBudget budget = new ManagedMemoryBudget(memoryManager, bytes);
    memoryManager.reserveMemory(budget, bytes);
    budget.registerMetrics(operator.getMetricGroup());
    return budget;
  }

  /**
   * Surfaces the native footprint in the Flink UI/metrics reporter next to the JVM numbers: the
   * reserved budget, the tracked state bytes drawing on it, and the process-wide Arrow FFI
   * allocator (the transient batch buffers crossing the JNI boundary — the same value on every
   * operator, repeated so it is visible wherever one looks).
   */
  private void registerMetrics(MetricGroup group) {
    group.gauge("nativeStateBudgetBytes", () -> bytes);
    group.gauge("nativeStateBytes", stateBytes::get);
    group.gauge("nativeArrowAllocatorBytes", NativeAllocator.SHARED::getAllocatedMemory);
  }

  /** Whether a budget was reserved — the native side tracks its state footprint only then. */
  public boolean bounded() {
    return bytes != UNBOUNDED;
  }

  /** Publishes the native side's tracked state size to the gauges; call from the task thread. */
  public void publishStateBytes(long value) {
    stateBytes.set(value);
  }

  /** The reserved budget in bytes, or {@link #UNBOUNDED} when none was reserved. */
  public long bytes() {
    return bytes;
  }

  @Override
  public void close() {
    if (memoryManager != null) {
      memoryManager.releaseAllMemory(this);
    }
  }
}
