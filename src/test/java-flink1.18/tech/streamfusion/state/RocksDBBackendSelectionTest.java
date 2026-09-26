package tech.streamfusion.state;

import static org.junit.jupiter.api.Assertions.*;

import java.lang.reflect.Field;
import java.nio.file.Path;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.contrib.streaming.state.DefaultConfigurableOptionsFactory;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.contrib.streaming.state.PredefinedOptions;
import org.apache.flink.contrib.streaming.state.RocksDBConfigurableOptions;
import org.apache.flink.contrib.streaming.state.RocksDBOptions;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import tech.streamfusion.compat.FlinkStateBackendCompat;
import tech.streamfusion.planner.NativePlanner;

class RocksDBBackendSelectionTest {
  @TempDir Path directory;

  @Test
  void preservesProgrammaticAndPreviouslyConfiguredOptionsWithFlinkPrecedence() throws Exception {
    Configuration earlier = new Configuration();
    earlier.set(RocksDBConfigurableOptions.MAX_OPEN_FILES, 23);
    earlier.set(RocksDBConfigurableOptions.WRITE_BUFFER_SIZE, MemorySize.parse("4mb"));
    EmbeddedRocksDBStateBackend stock =
        new EmbeddedRocksDBStateBackend(true).configure(earlier, getClass().getClassLoader());
    stock.setDbStoragePath(directory.toString());
    stock.setPredefinedOptions(PredefinedOptions.SPINNING_DISK_OPTIMIZED_HIGH_MEM);
    stock.setWriteBatchSize(4096);
    stock.setNumberOfTransferThreads(3);
    stock.setPriorityQueueStateType(EmbeddedRocksDBStateBackend.PriorityQueueStateType.HEAP);
    stock.getMemoryConfiguration().setUseManagedMemory(false);
    stock.getMemoryConfiguration().setFixedMemoryPerSlot("32mb");
    stock.getMemoryConfiguration().setWriteBufferRatio(0.3);
    Configuration current = new Configuration();
    current.set(CheckpointingOptions.INCREMENTAL_CHECKPOINTS, false);
    current.set(RocksDBConfigurableOptions.MAX_OPEN_FILES, 41);
    current.set(RocksDBOptions.USE_MANAGED_MEMORY, true);
    current.set(RocksDBOptions.WRITE_BUFFER_RATIO, 0.5);
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(current);
    env.setStateBackend(stock);

    assertNull(FlinkStateBackendCompat.prepareNativeStateBackend(env, current));
    RocksDBNativeStateBackend nativeBackend =
        assertInstanceOf(RocksDBNativeStateBackend.class, env.getStateBackend());
    FlinkRocksDBOptions options = (FlinkRocksDBOptions) field(nativeBackend, "nativeOptions");
    assertTrue(options.json().contains("\"maxOpenFiles\":41"));
    assertTrue(options.json().contains("\"writeBufferSize\":4194304"));
    assertTrue(options.json().contains("\"writeBatchSize\":4096"));
    assertEquals(true, field(nativeBackend, "incrementalCheckpoints"));
    assertEquals(directory.toString(), field(nativeBackend, "localDirectories"));
    assertEquals(false, field(nativeBackend, "useManagedMemory"));
    assertEquals(32L * 1024 * 1024, field(nativeBackend, "fixedPerSlotBytes"));
    assertEquals(0.3, field(nativeBackend, "writeBufferRatio"));
    Field delegateField = FlinkStateBackendCompat.class.getDeclaredField("delegate");
    delegateField.setAccessible(true);
    EmbeddedRocksDBStateBackend delegate =
        assertInstanceOf(EmbeddedRocksDBStateBackend.class, delegateField.get(nativeBackend));
    assertEquals(3, delegate.getNumberOfTransferThreads());
    assertEquals(
        EmbeddedRocksDBStateBackend.PriorityQueueStateType.HEAP,
        delegate.getPriorityQueueStateType());
    assertNull(FlinkStateBackendCompat.prepareNativeStateBackend(env, current));
    assertSame(nativeBackend, env.getStateBackend());
  }

  @Test
  void preservesLegacyCheckpointStoragePrecedence() throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.getCheckpointConfig().setCheckpointStorage(directory.toUri().toString());
    env.setStateBackend(new RocksDBStateBackend(new MemoryStateBackend(1234567)));
    assertNull(FlinkStateBackendCompat.prepareNativeStateBackend(env, new Configuration()));
    MemoryStateBackend storage =
        assertInstanceOf(
            MemoryStateBackend.class, env.getCheckpointConfig().getCheckpointStorage());
    assertEquals(1234567, storage.getMaxStateSize());
  }

  @Test
  void customOptionsFactoryKeepsStockBackendAndExplainsFallback() {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    EmbeddedRocksDBStateBackend stock = new EmbeddedRocksDBStateBackend();
    stock.setRocksDBOptions(new DefaultConfigurableOptionsFactory());
    env.setStateBackend(stock);
    assertTrue(
        FlinkStateBackendCompat.prepareNativeStateBackend(env, new Configuration())
            .contains("options factories"));
    assertSame(stock, env.getStateBackend());
  }

  @Test
  void serializedStockBackendRetainsDefaultMemoryFactoryAdmission() throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setStateBackend(
        org.apache.flink.util.InstantiationUtil.clone(
            new EmbeddedRocksDBStateBackend(), getClass().getClassLoader()));
    assertNull(FlinkStateBackendCompat.prepareNativeStateBackend(env, new Configuration()));
    assertInstanceOf(RocksDBNativeStateBackend.class, env.getStateBackend());
  }

  @Test
  void customBackendSubclassIsNotReplaced() {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    EmbeddedRocksDBStateBackend stock = new EmbeddedRocksDBStateBackend() {};
    env.setStateBackend(stock);
    assertNotNull(FlinkStateBackendCompat.prepareNativeStateBackend(env, new Configuration()));
    assertSame(stock, env.getStateBackend());
  }

  @Test
  void customMemoryFactoryIsNotIgnored() {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    EmbeddedRocksDBStateBackend stock = new EmbeddedRocksDBStateBackend();
    stock.setRocksDBMemoryFactory(
        new org.apache.flink.contrib.streaming.state.RocksDBMemoryControllerUtils
            .RocksDBMemoryFactory() {
          @Override
          public org.rocksdb.Cache createCache(long capacity, double ratio) {
            throw new AssertionError("admission must not allocate a cache");
          }

          @Override
          public org.rocksdb.WriteBufferManager createWriteBufferManager(
              long capacity, org.rocksdb.Cache cache) {
            throw new AssertionError("admission must not allocate write buffers");
          }
        });
    env.setStateBackend(stock);
    assertTrue(
        FlinkStateBackendCompat.prepareNativeStateBackend(env, new Configuration())
            .contains("memory factories"));
    assertSame(stock, env.getStateBackend());
  }

  @Test
  void disabledAccelerationLeavesStockBackendUntouched() {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    EmbeddedRocksDBStateBackend stock = new EmbeddedRocksDBStateBackend();
    env.setStateBackend(stock);
    StreamTableEnvironment table = StreamTableEnvironment.create(env);
    table.getConfig().set("streamfusion.native.enabled", "false");
    NativePlanner.explain(table, "SELECT SUM(v) FROM (VALUES (1), (2)) AS t(v)");
    assertSame(stock, env.getStateBackend());
  }

  private static Object field(Object target, String name) throws Exception {
    Field field = target.getClass().getDeclaredField(name);
    field.setAccessible(true);
    return field.get(target);
  }
}
