package tech.streamfusion.state;

import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.runtime.state.StateBackendFactory;

/** Factory used by {@code state.backend.type}. */
public final class RocksDBNativeStateBackendFactory
    implements StateBackendFactory<RocksDBNativeStateBackend> {

  @Override
  public RocksDBNativeStateBackend createFromConfig(
      ReadableConfig config, ClassLoader classLoader) {
    return new RocksDBNativeStateBackend(config, classLoader);
  }
}
