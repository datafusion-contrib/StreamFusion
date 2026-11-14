package tech.streamfusion.paimon;

import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.memory.MemoryReservationException;

/** Accounts retained Arrow buffers against either the writer budget or Flink managed memory. */
final class PaimonBufferMemory implements AutoCloseable {
  private final long limit;
  private final MemoryManager manager;
  private long reserved;

  PaimonBufferMemory(long limit) {
    this(limit, null);
  }

  PaimonBufferMemory(long limit, MemoryManager manager) {
    this.limit = limit;
    this.manager = manager;
  }

  /** A failed reservation asks the writer to spill and retry with its remaining buffers. */
  boolean update(long bytes) {
    if (bytes > limit) {
      return false;
    }
    if (manager != null) {
      if (bytes > reserved) {
        try {
          manager.reserveMemory(this, bytes - reserved);
        } catch (MemoryReservationException exhausted) {
          return false;
        }
      } else if (bytes < reserved) {
        manager.releaseMemory(this, reserved - bytes);
      }
    }
    reserved = bytes;
    return true;
  }

  long reservedBytes() {
    return reserved;
  }

  @Override
  public void close() {
    update(0);
  }
}
