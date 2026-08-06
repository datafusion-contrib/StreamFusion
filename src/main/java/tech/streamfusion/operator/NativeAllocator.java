package tech.streamfusion.operator;

import org.apache.arrow.c.CDataDictionaryProvider;
import org.apache.arrow.memory.AllocationListener;
import org.apache.arrow.memory.AllocationOutcome;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;

/**
 * One Arrow allocator shared by every native operator in this JVM for the buffers that cross the
 * native↔JVM C Data Interface, never closed during execution — the StreamFusion analog of
 * datafusion-comet's {@code CometArrowAllocator}.
 *
 * <p>Arrow Java buffers are reference-counted: an exported/imported batch keeps the underlying buffers
 * alive until its refcount reaches zero. A per-operator allocator closed at the operator's {@code
 * close()} is safe only while every consumer is synchronous (a chained {@code collect()} runs the
 * downstream to completion) or copied across the network; an async consumer that finishes after the
 * producer's allocator closes would make the allocator report a false leak (the failure a file
 * source's fetcher thread hit). Sharing one long-lived allocator removes that latent constraint, as
 * comet does for the same reason. Buffers are still reclaimed promptly by refcount as each batch's
 * vectors are closed downstream.
 *
 * <p>Every allocation is charged to the TaskManager-wide StreamFusion task-off-heap pool. The allocator
 * remains process-wide because batches can outlive their producing operator, while the listener makes
 * their reference-counted lifetime visible to the same authority as native state and connector queues.
 */
public final class NativeAllocator {

  public static final BufferAllocator SHARED =
      new RootAllocator(new TaskBudgetListener(), Long.MAX_VALUE);
  public static final CDataDictionaryProvider DICTIONARIES = new CDataDictionaryProvider();

  private NativeAllocator() {}

  public static void initializeFor(AbstractStreamOperator<?> operator) {
    TaskOffHeapMemory.initialize(
        operator.getRuntimeContext().getTaskManagerRuntimeInfo().getConfiguration());
    TaskOffHeapMemory.registerMetrics(operator.getMetricGroup());
  }

  static BufferAllocator newAllocatorForTests() {
    return new RootAllocator(new TaskBudgetListener(), Long.MAX_VALUE);
  }

  private static final class TaskBudgetListener implements AllocationListener {
    @Override
    public void onPreAllocation(long size) {
      TaskOffHeapMemory.reserveArrow(size);
    }

    @Override
    public void onRelease(long size) {
      TaskOffHeapMemory.releaseArrow(size);
    }

    @Override
    public boolean onFailedAllocation(long size, AllocationOutcome outcome) {
      TaskOffHeapMemory.releaseArrow(size);
      return false;
    }
  }
}
