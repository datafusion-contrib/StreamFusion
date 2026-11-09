package tech.streamfusion.compat;

import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.runtime.state.*;
import org.apache.flink.state.rocksdb.EmbeddedRocksDBStateBackend;

/** Adapts the host backend factory signature without owning native state or snapshot bytes. */
public abstract class FlinkStateBackendCompat implements StateBackend {
  public static StateBackend explicitStateBackend(
      org.apache.flink.streaming.api.environment.StreamExecutionEnvironment environment) {
    return null;
  }

  public static void explicitStateBackend(
      org.apache.flink.streaming.api.environment.StreamExecutionEnvironment environment,
      StateBackend backend) {
    throw new UnsupportedOperationException(
        "Flink 2.2 configures state backends through job configuration");
  }

  public static int keyGroupPrefixBytes(CheckpointableKeyedStateBackend<?> backend) {
    return org.apache.flink.runtime.state.CompositeKeySerializationUtils
        .computeRequiredBytesInKeyGroupPrefix(
            ((org.apache.flink.runtime.state.InternalKeyContext<?>) backend)
                .getNumberOfKeyGroups());
  }

  protected final StateBackend delegate;

  public static org.apache.flink.configuration.Configuration configuration(ReadableConfig config) {
    return org.apache.flink.configuration.Configuration.fromMap(config.toMap());
  }

  public static String prepareNativeStateBackend(
      org.apache.flink.streaming.api.environment.StreamExecutionEnvironment environment,
      ReadableConfig tableConfig) {
    boolean configuredChangelog =
        tableConfig
            .getOptional(
                org.apache.flink.configuration.StateChangelogOptions.ENABLE_STATE_CHANGE_LOG)
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
                null)
            .withStockBackendFactory(
                (serializer, handles) ->
                    stockProjectionBackend(parameters.getEnv())
                        .createKeyedStateBackend(
                            new KeyedStateBackendParametersImpl<>(
                                parameters.getEnv(),
                                parameters.getJobID(),
                                parameters.getOperatorIdentifier(),
                                serializer,
                                parameters.getNumberOfKeyGroups(),
                                parameters.getKeyGroupRange(),
                                parameters.getKvStateRegistry(),
                                parameters.getTtlTimeProvider(),
                                parameters.getMetricGroup(),
                                parameters.getCustomInitializationMetrics(),
                                handles,
                                parameters.getCancelStreamRegistry(),
                                parameters.getManagedMemoryFraction()))));
  }

  private static EmbeddedRocksDBStateBackend stockProjectionBackend(
      org.apache.flink.runtime.execution.Environment environment) {
    var backend =
        new EmbeddedRocksDBStateBackend()
            .configure(
                environment.getTaskManagerInfo().getConfiguration(),
                environment.getUserCodeClassLoader().asClassLoader());
    backend.setPriorityQueueStateType(EmbeddedRocksDBStateBackend.PriorityQueueStateType.ROCKSDB);
    return backend;
  }

  @Override
  public final OperatorStateBackend createOperatorStateBackend(
      OperatorStateBackendParameters parameters) throws Exception {
    return delegate.createOperatorStateBackend(parameters);
  }
}
