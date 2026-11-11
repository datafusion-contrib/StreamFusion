package tech.streamfusion.operator;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.metrics.MetricGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.streamfusion.NativeMemoryLimitException;

/** Process-wide authority for memory charged to Flink task off-heap memory. */
public final class TaskOffHeapMemory {

  private static final Logger LOG = LoggerFactory.getLogger(TaskOffHeapMemory.class);
  private static final AtomicLong NEXT_OWNER = new AtomicLong(1);
  private static final AtomicLong RESERVED = new AtomicLong();
  private static final AtomicLong PEAK = new AtomicLong();
  private static final AtomicLong DENIED = new AtomicLong();
  private static final ConcurrentMap<Long, Owner> OWNERS = new ConcurrentHashMap<>();

  private static volatile long capacity = -1;
  private static volatile long arrowOwner;
  private static long uninitializedArrowBytes;

  private TaskOffHeapMemory() {}

  /** Initializes the process pool from Flink's normal TaskManager task-off-heap setting. */
  public static synchronized void initialize(Configuration configuration) {
    long configured = configuration.get(TaskManagerOptions.TASK_OFF_HEAP_MEMORY).getBytes();
    if (configured <= 0) {
      // Operator test harnesses reuse an already initialized process pool but expose a mock
      // TaskManagerRuntimeInfo with an empty configuration. A real TaskManager initializes the
      // pool first and every task in that process must use that same authority.
      if (capacity > 0) {
        return;
      }
      throw new IllegalStateException(
          "StreamFusion requires "
              + TaskManagerOptions.TASK_OFF_HEAP_MEMORY.key()
              + " to be greater than zero");
    }
    if (capacity >= 0 && capacity != configured) {
      // Production has one TaskManager per JVM. Local tests may start sequential MiniClusters with
      // different configurations in one JVM; switching is safe only after every reservation left.
      if (RESERVED.get() != 0) {
        throw new IllegalStateException(
            "StreamFusion task off-heap memory was already initialized to "
                + new MemorySize(capacity)
                + " with live reservations, but this runtime reports "
                + new MemorySize(configured));
      }
      capacity = configured;
      PEAK.set(0);
      DENIED.set(0);
      return;
    }
    if (capacity < 0 && uninitializedArrowBytes != 0) {
      if (uninitializedArrowBytes > configured) {
        DENIED.incrementAndGet();
        throw exhausted("arrow [shared Arrow allocator]", uninitializedArrowBytes, configured, 0);
      }
      Owner owner = OWNERS.get(arrowOwner());
      if (!owner.reserve(uninitializedArrowBytes)) {
        throw new IllegalStateException("cannot adopt early Arrow reservations");
      }
      RESERVED.set(uninitializedArrowBytes);
      PEAK.accumulateAndGet(uninitializedArrowBytes, Math::max);
      uninitializedArrowBytes = 0;
    }
    // Publish the budget only after early buffers have an owner that can accept their release.
    capacity = configured;
  }

  public static long registerOwner(String category, String name) {
    requireInitialized();
    long id = NEXT_OWNER.getAndIncrement();
    OWNERS.put(id, new Owner(category, name));
    return id;
  }

  /** Acquires a fixed-lifetime reservation, used for bounded native connector queues. */
  public static Reservation reserve(String category, String name, long bytes) {
    long ownerId = registerOwner(category, name);
    NativeMemoryLimitException failure = tryReserve(OWNERS.get(ownerId), bytes);
    if (failure != null) {
      closeOwner(ownerId);
      throw failure;
    }
    return new Reservation(ownerId, bytes);
  }

  public static void closeOwner(long ownerId) {
    Owner owner = OWNERS.get(ownerId);
    if (owner == null) {
      return;
    }
    long released = owner.close();
    OWNERS.remove(ownerId, owner);
    if (released != 0) {
      RESERVED.addAndGet(-released);
    }
  }

  /** JNI entry used by the native DataFusion memory pool. */
  public static boolean tryReserve(long ownerId, long bytes) {
    if (bytes <= 0) {
      return true;
    }
    Owner owner = OWNERS.get(ownerId);
    if (owner == null) {
      LOG.warn(
          "Task off-heap reservation rejected for unregistered owner {}: {} bytes", ownerId, bytes);
      return false;
    }
    NativeMemoryLimitException failure = tryReserve(owner, bytes);
    if (failure != null) {
      // The JNI boolean contract also serves optional caches that can recover from denial.
      LOG.warn(
          "Task off-heap reservation rejected for owner {}: {}", ownerId, failure.getMessage());
      return false;
    }
    return true;
  }

  /** JNI entry used by the native DataFusion memory pool. */
  public static void release(long ownerId, long bytes) {
    if (bytes <= 0) {
      return;
    }
    Owner owner = OWNERS.get(ownerId);
    if (owner == null) {
      return;
    }
    if (owner.release(bytes)) {
      RESERVED.addAndGet(-bytes);
    }
  }

  /** Spillable native state leaves space for Arrow input, copying and output import. */
  public static boolean tryReserveWithHeadroom(long ownerId, long bytes, long headroom) {
    if (bytes <= 0) return true;
    Owner owner = OWNERS.get(ownerId);
    if (owner == null) return false;
    NativeMemoryLimitException failure = reserveGlobal(owner, bytes, Math.max(0, headroom));
    if (failure != null) return false;
    if (!owner.reserve(bytes)) {
      RESERVED.addAndGet(-bytes);
      return false;
    }
    return true;
  }

  static void reserveArrow(long bytes) {
    if (capacity < 0) {
      synchronized (TaskOffHeapMemory.class) {
        if (capacity < 0) {
          uninitializedArrowBytes = Math.addExact(uninitializedArrowBytes, bytes);
          return;
        }
      }
    }
    NativeMemoryLimitException failure = tryReserve(OWNERS.get(arrowOwner()), bytes);
    if (failure != null) {
      throw failure;
    }
  }

  static void releaseArrow(long bytes) {
    if (capacity < 0) {
      synchronized (TaskOffHeapMemory.class) {
        if (capacity < 0) {
          if (bytes > uninitializedArrowBytes) {
            throw new IllegalStateException("early Arrow buffers released more than reserved");
          }
          uninitializedArrowBytes -= bytes;
          return;
        }
      }
    }
    long owner = arrowOwner;
    if (owner != 0) {
      release(owner, bytes);
    }
  }

  public static NativeMemoryLimitException exhausted(String consumer, long requested) {
    return exhausted(consumer, requested, capacityBytes(), reservedBytes());
  }

  private static NativeMemoryLimitException exhausted(
      String consumer, long requested, long limit, long reserved) {
    return new NativeMemoryLimitException(
        consumer
            + " requested "
            + requested
            + " task off-heap bytes with "
            + Math.max(0, limit - reserved)
            + " available (capacity="
            + limit
            + ", reserved="
            + reserved
            + "); increase the task/vertex off-heap resource budget or "
            + TaskManagerOptions.TASK_OFF_HEAP_MEMORY.key());
  }

  public static void registerMetrics(MetricGroup group) {
    group.gauge("nativeOffHeapCapacityBytes", TaskOffHeapMemory::capacityBytes);
    group.gauge("nativeOffHeapReservedBytes", TaskOffHeapMemory::reservedBytes);
    group.gauge("nativeOffHeapAvailableBytes", TaskOffHeapMemory::availableBytes);
    group.gauge("nativeOffHeapPeakBytes", TaskOffHeapMemory::peakBytes);
    group.gauge("nativeOffHeapDeniedReservations", DENIED::get);
    group.gauge("nativeArrowAllocatorBytes", NativeAllocator.SHARED::getAllocatedMemory);
  }

  public static long capacityBytes() {
    return Math.max(0, capacity);
  }

  public static long reservedBytes() {
    return RESERVED.get();
  }

  public static long availableBytes() {
    return Math.max(0, capacityBytes() - reservedBytes());
  }

  public static long peakBytes() {
    return PEAK.get();
  }

  static synchronized void resetForTests(long bytes) {
    if (RESERVED.get() != 0 || uninitializedArrowBytes != 0) {
      throw new IllegalStateException("cannot reset task off-heap memory with live reservations");
    }
    OWNERS.clear();
    RESERVED.set(0);
    PEAK.set(0);
    DENIED.set(0);
    NEXT_OWNER.set(1);
    arrowOwner = 0;
    uninitializedArrowBytes = 0;
    capacity = bytes;
  }

  private static NativeMemoryLimitException reserveGlobal(Owner owner, long bytes) {
    return reserveGlobal(owner, bytes, 0);
  }

  private static NativeMemoryLimitException reserveGlobal(Owner owner, long bytes, long headroom) {
    while (true) {
      long current = RESERVED.get();
      long limit = Math.max(0, capacity - Math.min(capacity, headroom));
      long next;
      try {
        next = Math.addExact(current, bytes);
      } catch (ArithmeticException ignored) {
        return denied(owner, bytes, limit, current);
      }
      if (next > limit) {
        return denied(owner, bytes, limit, current);
      }
      if (!RESERVED.compareAndSet(current, next)) {
        continue;
      }
      PEAK.accumulateAndGet(next, Math::max);
      return null;
    }
  }

  private static synchronized long arrowOwner() {
    if (arrowOwner == 0) {
      long id = NEXT_OWNER.getAndIncrement();
      OWNERS.put(id, new Owner("arrow", "shared Arrow allocator"));
      arrowOwner = id;
    }
    return arrowOwner;
  }

  private static NativeMemoryLimitException tryReserve(Owner owner, long bytes) {
    if (bytes <= 0) {
      return null;
    }
    NativeMemoryLimitException failure = reserveGlobal(owner, bytes);
    if (failure != null) {
      return failure;
    }
    if (!owner.reserve(bytes)) {
      RESERVED.addAndGet(-bytes);
      DENIED.incrementAndGet();
      return new NativeMemoryLimitException(
          "Task off-heap owner "
              + owner.name
              + " closed or overflowed during reservation of "
              + bytes);
    }
    return null;
  }

  private static NativeMemoryLimitException denied(
      Owner owner, long bytes, long limit, long reserved) {
    DENIED.incrementAndGet();
    // Capture the failed capacity check, not a later sample after other tasks release memory.
    return exhausted(owner.category + " [" + owner.name + "]", bytes, limit, reserved);
  }

  private static void requireInitialized() {
    if (capacity < 0) {
      throw new IllegalStateException("StreamFusion task off-heap memory is not initialized");
    }
  }

  private static final class Owner {
    private static final long CLOSED = -1;

    private final String category;
    private final String name;
    private final AtomicLong reserved = new AtomicLong();

    private Owner(String category, String name) {
      this.category = category;
      this.name = name;
    }

    private boolean reserve(long bytes) {
      while (true) {
        long held = reserved.get();
        if (held == CLOSED) {
          return false;
        }
        final long next;
        try {
          next = Math.addExact(held, bytes);
        } catch (ArithmeticException ignored) {
          return false;
        }
        if (reserved.compareAndSet(held, next)) {
          return true;
        }
      }
    }

    private boolean release(long bytes) {
      while (true) {
        long held = reserved.get();
        // closeOwner already returned the complete balance to the global pool.
        if (held == CLOSED) {
          return false;
        }
        if (bytes > held) {
          throw new IllegalStateException(
              "StreamFusion memory owner " + name + " released " + bytes + " of " + held);
        }
        if (reserved.compareAndSet(held, held - bytes)) {
          return true;
        }
      }
    }

    private long close() {
      long held = reserved.getAndSet(CLOSED);
      return held == CLOSED ? 0 : held;
    }
  }

  public static final class Reservation implements AutoCloseable {
    private final long ownerId;
    private final long bytes;
    private boolean closed;

    private Reservation(long ownerId, long bytes) {
      this.ownerId = ownerId;
      this.bytes = bytes;
    }

    public long bytes() {
      return bytes;
    }

    @Override
    public synchronized void close() {
      if (!closed) {
        closed = true;
        closeOwner(ownerId);
      }
    }
  }
}
