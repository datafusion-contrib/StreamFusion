package io.github.jordepic.streamfusion.state;

import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.runtime.state.StateBackendFactory;

/**
 * Factory hook for Flink's state-backend selection: set {@code state.backend.type} to this class
 * name to run native operator state on local Paimon tables with incremental checkpoints.
 */
public class PaimonStateBackendFactory implements StateBackendFactory<PaimonStateBackend> {

  @Override
  public PaimonStateBackend createFromConfig(ReadableConfig config, ClassLoader classLoader) {
    return new PaimonStateBackend();
  }
}
