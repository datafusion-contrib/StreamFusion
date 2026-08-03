package tech.streamfusion;

/**
 * Thrown by a native call when an operator's state would exceed the managed-memory budget the host
 * reserved for it. This is the accounted replacement for the failure it prevents: without a budget
 * the native side allocates invisibly to Flink and the container is OOM-killed with no attribution;
 * with one, the task fails with the operator and the remedy (raise the task manager's managed memory
 * or the operator's managed-memory weight) in the message.
 */
public class NativeMemoryLimitException extends RuntimeException {

  public NativeMemoryLimitException(String message) {
    super(message);
  }
}
