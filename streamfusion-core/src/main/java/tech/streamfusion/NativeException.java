package tech.streamfusion;

/**
 * Thrown when a native call cannot complete safely, including an invalid physical batch layout or a
 * Rust panic caught at the JNI boundary.
 *
 * <p>Without this conversion a panic unwinds out of a native frame, which the Rust ABI turns into
 * an immediate process abort: the whole TaskManager dies, taking every other task in the process
 * with it, and Flink's restart strategy never sees a failure it could act on. Converting the panic
 * into an ordinary exception on the calling task thread makes it a normal task failure instead, so
 * one bad record fails and restarts its own task rather than the container.
 *
 * <p>Unchecked by design: native calls sit on the hot path of operators that declare no checked
 * exceptions, and a panic is an internal engine fault rather than a condition callers handle.
 */
public class NativeException extends RuntimeException {

  public NativeException(String message) {
    super(message);
  }
}
