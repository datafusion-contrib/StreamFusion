package tech.streamfusion.compat;

import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.runtime.state.*;
import org.apache.flink.state.rocksdb.EmbeddedRocksDBStateBackend;

/** Adapts the host backend factory signature without owning native state or snapshot bytes. */
public abstract class FlinkStateBackendCompat implements StateBackend {
  protected final StateBackend delegate;

  public static String prepareNativeStateBackend(
      org.apache.flink.streaming.api.environment.StreamExecutionEnvironment environment,
      ReadableConfig tableConfig) {
    boolean configuredChangelog =
        tableConfig
            .getOptional(org.apache.flink.configuration.StateChangelogOptions.ENABLE_STATE_CHANGE_LOG)
            .orElse(false);
    boolean changelog =
        environment == null
            ? configuredChangelog
            : environment.isChangelogStateBackendEnabled().getOrDefault(configuredChangelog);
    return changelog
        ? "state backend: Flink 2.2 changelog state is not verified for native keyed state"
        : null;
  }

  protected static StateBackend configuredDelegate(ReadableConfig config, ClassLoader classLoader) {
    return new EmbeddedRocksDBStateBackend().configure(config, classLoader);
  }

  protected FlinkStateBackendCompat(StateBackend delegate) {
    this.delegate = delegate;
  }

  protected abstract <K> CheckpointableKeyedStateBackend<K> createNativeKeyedBackend(
      KeyedBackendContext<K> context) throws Exception;

  @Override
  public boolean useManagedMemory() {
    return delegate.useManagedMemory();
  }

  @Override
  public final <K> CheckpointableKeyedStateBackend<K> createKeyedStateBackend(
      KeyedStateBackendParameters<K> parameters) throws Exception {
    return createNativeKeyedBackend(
        new KeyedBackendContext<>(
            parameters.getEnv(),
            parameters.getJobID(),
            parameters.getOperatorIdentifier(),
            parameters.getKeySerializer(),
            parameters.getNumberOfKeyGroups(),
            parameters.getKeyGroupRange(),
            parameters.getStateHandles(),
            parameters.getManagedMemoryFraction(),
            handles ->
                delegate.createKeyedStateBackend(
                    new KeyedStateBackendParametersImpl<>(parameters).setStateHandles(handles)),
            null));
  }

  @Override
  public final OperatorStateBackend createOperatorStateBackend(
      OperatorStateBackendParameters parameters) throws Exception {
    return delegate.createOperatorStateBackend(parameters);
  }
}
