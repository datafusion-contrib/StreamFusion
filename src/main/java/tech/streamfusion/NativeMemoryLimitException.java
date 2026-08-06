package tech.streamfusion;

/**
 * Thrown when a StreamFusion consumer cannot reserve from the TaskManager-wide task off-heap pool.
 * This is deliberately distinct from a generic {@link RuntimeException}: callers and tests can
 * identify a configured resource limit rather than an allocator leak. The message names the
 * consumer and Flink's {@code taskmanager.memory.task.off-heap.size} remedy.
 */
public class NativeMemoryLimitException extends RuntimeException {

  public NativeMemoryLimitException(String message) {
    super(message);
  }
}
