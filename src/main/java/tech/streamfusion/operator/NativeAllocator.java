package tech.streamfusion.operator;

import tech.streamfusion.NativeMemoryLimitException;
import tech.streamfusion.planner.NativeConfig;
import org.apache.arrow.c.CDataDictionaryProvider;
import org.apache.arrow.memory.AllocationListener;
import org.apache.arrow.memory.AllocationOutcome;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;

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
 * <p>This is independent of memory accounting: limiting/attributing execution memory per operator is a
 * DataFusion memory-pool concern bridged to the framework's memory manager, not the allocator's scope.
 * Like comet's, the allocator runs uncapped by default — its traffic is transient per-batch crossings,
 * not state — but {@link NativeConfig#arrowAllocatorMaxMb()} can bound it for deployments that want a
 * fail-fast, attributed error instead of unaccounted process growth.
 */
public final class NativeAllocator {

  public static final BufferAllocator SHARED =
      newAllocator(NativeConfig.arrowAllocatorMaxMb());
  public static final CDataDictionaryProvider DICTIONARIES = new CDataDictionaryProvider();

  private NativeAllocator() {}

  static BufferAllocator newAllocator(long maxMb) {
    if (maxMb <= 0) {
      return new RootAllocator();
    }
    return new RootAllocator(new BudgetListener(maxMb), maxMb << 20);
  }

  private static final class BudgetListener implements AllocationListener {
    private final long maxMb;

    BudgetListener(long maxMb) {
      this.maxMb = maxMb;
    }

    @Override
    public boolean onFailedAllocation(long size, AllocationOutcome outcome) {
      throw new NativeMemoryLimitException(
          "Arrow FFI buffer of "
              + size
              + " bytes would exceed the "
              + maxMb
              + " MiB cap set by -Dstreamfusion.memory.arrow.max-mb; raise the cap (and the task"
              + " manager's off-heap sizing, see docs/native-memory-profiling.md) or unset it to"
              + " run uncapped");
    }
  }
}
