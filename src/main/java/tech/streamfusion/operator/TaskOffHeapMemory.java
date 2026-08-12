package tech.streamfusion.operator;

import tech.streamfusion.NativeMemoryLimitException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.metrics.MetricGroup;

/** Process-wide authority for memory charged to Flink task off-heap memory. */
public final class TaskOffHeapMemory {

  private static final AtomicLong NEXT_OWNER = new AtomicLong(1);
  private static final AtomicLong RESERVED = new AtomicLong();
  private static final AtomicLong PEAK = new AtomicLong();
  private static final AtomicLong DENIED = new AtomicLong();
  private static final ConcurrentMap<Long, Owner> OWNERS = new ConcurrentHashMap<>();

  private static volatile long capacity = -1;
  private static volatile long arrowOwner;

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
    if (!tryReserve(ownerId, bytes)) {
      closeOwner(ownerId);
      throw exhausted(name, bytes);
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
      return false;
    }
    if (!reserveGlobal(bytes)) {
      DENIED.incrementAndGet();
      return false;
    }
    if (!owner.reserve(bytes)) {
      RESERVED.addAndGet(-bytes);
      DENIED.incrementAndGet();
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

  static void reserveArrow(long bytes) {
    if (capacity < 0) {
      return;
    }
    long owner = arrowOwner();
    if (!tryReserve(owner, bytes)) {
      throw exhausted("Arrow", bytes);
    }
  }

  static void releaseArrow(long bytes) {
    long owner = arrowOwner;
    if (owner != 0) {
      release(owner, bytes);
    }
  }

  public static NativeMemoryLimitException exhausted(String consumer, long requested) {
    return new NativeMemoryLimitException(
        consumer
            + " requested "
            + requested
            + " task off-heap bytes with "
            + availableBytes()
            + " available; raise "
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
    OWNERS.clear();
    RESERVED.set(0);
    PEAK.set(0);
    DENIED.set(0);
    NEXT_OWNER.set(1);
    arrowOwner = 0;
    capacity = bytes;
  }

  private static boolean reserveGlobal(long bytes) {
    while (true) {
      long current = RESERVED.get();
      long next;
      try {
        next = Math.addExact(current, bytes);
      } catch (ArithmeticException ignored) {
        return false;
      }
      if (next > capacity || !RESERVED.compareAndSet(current, next)) {
        if (next > capacity) {
          return false;
        }
        continue;
      }
      PEAK.accumulateAndGet(next, Math::max);
      return true;
    }
  }

  private static synchronized long arrowOwner() {
    if (arrowOwner == 0) {
      arrowOwner = registerOwner("arrow", "shared Arrow allocator");
    }
    return arrowOwner;
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
