package tech.streamfusion.compat;

import org.apache.flink.streaming.api.operators.AbstractStreamOperator;

/** Keep columnar operators in the upstream task chain when Flink's topology permits it. */
public abstract class FlinkStreamOperator<OUT> extends AbstractStreamOperator<OUT> {
  private static final long serialVersionUID = 1L;
  private final tech.streamfusion.configuration.NativeRuntimeOptions runtimeOptions =
      tech.streamfusion.configuration.NativeRuntimeOptions.capture();

  protected final tech.streamfusion.configuration.NativeRuntimeOptions runtimeOptions() {
    // Operators serialized before per-job runtime options retain their legacy JVM defaults.
    return runtimeOptions == null
        ? tech.streamfusion.configuration.NativeRuntimeOptions.capture()
        : runtimeOptions;
  }

  protected FlinkStreamOperator() {}
}
