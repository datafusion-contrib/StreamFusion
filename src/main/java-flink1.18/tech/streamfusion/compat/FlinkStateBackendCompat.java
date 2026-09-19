package tech.streamfusion.compat;

import java.util.Collection;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.*;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;

/** Adapts the host backend factory signature without owning native state or snapshot bytes. */
public abstract class FlinkStateBackendCompat implements StateBackend {
  protected final StateBackend delegate;

  public static String unsupportedNativeStateReason(
      org.apache.flink.streaming.api.environment.StreamExecutionEnvironment environment,
      ReadableConfig tableConfig) {
    boolean changelog =
        environment == null
            ? tableConfig
                .getOptional(
                    org.apache.flink.configuration.StateChangelogOptions.ENABLE_STATE_CHANGE_LOG)
                .orElse(false)
            : environment.isChangelogStateBackendEnabled()
                == org.apache.flink.util.TernaryBoolean.TRUE;
    if (changelog) {
      return "state backend: Flink 1.18 changelog state is not verified for native keyed state";
    }
    StateBackend backend = environment == null ? null : environment.getStateBackend();
    if (backend instanceof org.apache.flink.runtime.state.hashmap.HashMapStateBackend
        || backend instanceof org.apache.flink.runtime.state.memory.MemoryStateBackend
        || backend instanceof org.apache.flink.runtime.state.filesystem.FsStateBackend
        // The deployed planner and host load the same final backend in separate classloaders.
        || (backend != null
            && backend
                .getClass()
                .getName()
                .equals(tech.streamfusion.state.RocksDBNativeStateBackend.class.getName()))) {
      return null;
    }
    var option = org.apache.flink.configuration.StateBackendOptions.STATE_BACKEND;
    String configured =
        environment == null
            ? tableConfig.getOptional(option).orElse("hashmap")
            : environment
                .getConfiguration()
                .getOptional(option)
                .orElse(tableConfig.getOptional(option).orElse("hashmap"));
    if (backend == null
        && java.util.Set.of(
                "hashmap",
                "jobmanager",
                "filesystem",
                "memory",
                "tech.streamfusion.state.RocksDBNativeStateBackendFactory")
            .contains(configured)) {
      return null;
    }
    return "state backend: Flink 1.18 native keyed state requires heap state or the StreamFusion"
        + " RocksDB backend";
  }

  protected FlinkStateBackendCompat(ReadableConfig config, ClassLoader classLoader) {
    delegate = new EmbeddedRocksDBStateBackend().configure(config, classLoader);
  }

  protected abstract <K> CheckpointableKeyedStateBackend<K> createNativeKeyedBackend(
      KeyedBackendContext<K> context) throws Exception;

  @Override
  public boolean useManagedMemory() {
    return delegate.useManagedMemory();
  }

  @Override
  public final <K> CheckpointableKeyedStateBackend<K> createKeyedStateBackend(
      Environment env,
      JobID jobId,
      String operatorIdentifier,
      TypeSerializer<K> keySerializer,
      int numberOfKeyGroups,
      KeyGroupRange keyGroupRange,
      TaskKvStateRegistry kvStateRegistry,
      TtlTimeProvider ttlTimeProvider,
      MetricGroup metricGroup,
      Collection<KeyedStateHandle> stateHandles,
      CloseableRegistry cancelStreamRegistry)
      throws Exception {
    return createKeyedStateBackend(
        env,
        jobId,
        operatorIdentifier,
        keySerializer,
        numberOfKeyGroups,
        keyGroupRange,
        kvStateRegistry,
        ttlTimeProvider,
        metricGroup,
        stateHandles,
        cancelStreamRegistry,
        0.0);
  }

  @Override
  public final <K> CheckpointableKeyedStateBackend<K> createKeyedStateBackend(
      Environment env,
      JobID jobId,
      String operatorIdentifier,
      TypeSerializer<K> keySerializer,
      int numberOfKeyGroups,
      KeyGroupRange keyGroupRange,
      TaskKvStateRegistry kvStateRegistry,
      TtlTimeProvider ttlTimeProvider,
      MetricGroup metricGroup,
      Collection<KeyedStateHandle> stateHandles,
      CloseableRegistry cancelStreamRegistry,
      double managedMemoryFraction)
      throws Exception {
    return createNativeKeyedBackend(
        new KeyedBackendContext<>(
            env,
            jobId,
            operatorIdentifier,
            keySerializer,
            numberOfKeyGroups,
            keyGroupRange,
            stateHandles,
            managedMemoryFraction,
            handles ->
                delegate.createKeyedStateBackend(
                    env,
                    jobId,
                    operatorIdentifier,
                    keySerializer,
                    numberOfKeyGroups,
                    keyGroupRange,
                    kvStateRegistry,
                    ttlTimeProvider,
                    metricGroup,
                    handles,
                    cancelStreamRegistry,
                    managedMemoryFraction),
            handles ->
                new org.apache.flink.runtime.state.hashmap.HashMapStateBackend()
                    .createKeyedStateBackend(
                        env,
                        jobId,
                        operatorIdentifier,
                        keySerializer,
                        numberOfKeyGroups,
                        keyGroupRange,
                        kvStateRegistry,
                        ttlTimeProvider,
                        metricGroup,
                        handles,
                        cancelStreamRegistry)));
  }

  @Override
  public final OperatorStateBackend createOperatorStateBackend(
      Environment env,
      String operatorIdentifier,
      Collection<OperatorStateHandle> stateHandles,
      CloseableRegistry cancelStreamRegistry)
      throws Exception {
    return delegate.createOperatorStateBackend(
        env, operatorIdentifier, stateHandles, cancelStreamRegistry);
  }
}
