package tech.streamfusion.compat;

import java.lang.reflect.Field;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.configuration.StateBackendOptions;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.contrib.streaming.state.RocksDBMemoryControllerUtils.RocksDBMemoryFactory;
import org.apache.flink.contrib.streaming.state.RocksDBOptions;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.CheckpointStorage;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableConfig;
import tech.streamfusion.state.RocksDBNativeStateBackend;

/** Preserves stock RocksDB configuration while selecting the native keyed-state implementation. */
final class RocksDBBackendSelection {
  private RocksDBBackendSelection() {}

  static String install(StreamExecutionEnvironment environment, ReadableConfig tableConfig) {
    if (environment == null) return null;
    StateBackend original = environment.getStateBackend();
    if (original != null
        && !original
            .getClass()
            .getName()
            .equals("org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend")
        && !original
            .getClass()
            .getName()
            .equals("org.apache.flink.contrib.streaming.state.RocksDBStateBackend")) return null;
    Configuration config = new Configuration();
    if (tableConfig instanceof TableConfig table) config.addAll(table.getConfiguration());
    config.addAll((Configuration) environment.getConfiguration());
    if (original == null) {
      String configured = config.get(StateBackendOptions.STATE_BACKEND);
      if (!"rocksdb".equalsIgnoreCase(configured)
          && !"org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackendFactory"
              .equals(configured)) return null;
      original = new EmbeddedRocksDBStateBackend();
    }
    if (original.getClass() != EmbeddedRocksDBStateBackend.class
        && original.getClass() != RocksDBStateBackend.class) return null;

    try {
      ClassLoader loader = Thread.currentThread().getContextClassLoader();
      CheckpointStorage storage = null;
      StateBackend delegate;
      EmbeddedRocksDBStateBackend rocks;
      if (original instanceof RocksDBStateBackend legacy) {
        RocksDBStateBackend configured = legacy.configure(config, loader);
        delegate = configured;
        storage = (CheckpointStorage) configured.getCheckpointBackend();
        rocks = (EmbeddedRocksDBStateBackend) field(configured, "rocksDBStateBackend");
      } else {
        rocks = ((EmbeddedRocksDBStateBackend) original).configure(config, loader);
        delegate = rocks;
      }
      if (rocks.getRocksDBOptions() != null) {
        return "state backend: RocksDB options factories cannot configure native RocksDB";
      }
      if (field(rocks, "rocksDBMemoryFactory").getClass()
          != RocksDBMemoryFactory.DEFAULT.getClass()) {
        return "state backend: custom RocksDB memory factories cannot configure native RocksDB";
      }

      // Flink exposes these resolved options only inside the released backend. Keep this access
      // in the version adapter, and fall back if their shape changes instead of losing settings.
      ReadableConfig resolved = (ReadableConfig) field(rocks, "configurableOptions");
      Configuration nativeConfig = new Configuration(config);
      nativeConfig.addAll((Configuration) resolved);
      nativeConfig.set(
          CheckpointingOptions.INCREMENTAL_CHECKPOINTS, rocks.isIncrementalCheckpointsEnabled());
      nativeConfig.set(RocksDBOptions.PREDEFINED_OPTIONS, rocks.getPredefinedOptions().name());
      nativeConfig.set(
          RocksDBOptionsCompat.WRITE_BATCH_SIZE, new MemorySize(rocks.getWriteBatchSize()));
      String[] paths = rocks.getDbStoragePaths();
      if (paths != null)
        nativeConfig.set(RocksDBOptions.LOCAL_DIRECTORIES, String.join(",", paths));
      var memory = rocks.getMemoryConfiguration();
      nativeConfig.set(RocksDBOptions.USE_MANAGED_MEMORY, memory.isUsingManagedMemory());
      nativeConfig.set(RocksDBOptions.WRITE_BUFFER_RATIO, memory.getWriteBufferRatio());
      if (memory.getFixedMemoryPerSlot() != null) {
        nativeConfig.set(RocksDBOptions.FIX_PER_SLOT_MEMORY_SIZE, memory.getFixedMemoryPerSlot());
      }
      RocksDBNativeStateBackend replacement = new RocksDBNativeStateBackend(nativeConfig, delegate);
      if (storage != null) environment.getCheckpointConfig().setCheckpointStorage(storage);
      environment.setStateBackend(replacement);
      return null;
    } catch (ReflectiveOperationException | ClassCastException | SecurityException failure) {
      return "state backend: cannot preserve the configured Flink 1.18 RocksDB backend ("
          + failure.getClass().getSimpleName()
          + ")";
    }
  }

  private static Object field(Object backend, String name) throws ReflectiveOperationException {
    Field field = backend.getClass().getDeclaredField(name);
    if (!field.trySetAccessible()) throw new IllegalAccessException(name);
    return field.get(backend);
  }
}
