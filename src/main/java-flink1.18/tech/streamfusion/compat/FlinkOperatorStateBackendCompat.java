package tech.streamfusion.compat;

import org.apache.flink.runtime.state.OperatorStateBackend;

/** Keeps released operator-state APIs behind the common snapshot lifecycle. */
public abstract class FlinkOperatorStateBackendCompat implements OperatorStateBackend {
  protected final OperatorStateBackend delegate;

  protected FlinkOperatorStateBackendCompat(OperatorStateBackend delegate) {
    this.delegate = delegate;
  }
}
