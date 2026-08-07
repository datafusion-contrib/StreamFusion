package tech.streamfusion.state;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.state.rocksdb.RocksDBConfigurableOptions;
import org.apache.flink.state.rocksdb.RocksDBOptions;
import org.junit.jupiter.api.Test;

class FlinkRocksDBOptionsTest {

  @Test
  void resolvesFlinkDefaultsAndExplicitOverrides() {
    Configuration configuration = new Configuration();
    configuration.set(
        RocksDBConfigurableOptions.WRITE_BUFFER_SIZE, MemorySize.parse("17mb"));
    configuration.set(RocksDBConfigurableOptions.USE_DYNAMIC_LEVEL_SIZE, true);

    String json = FlinkRocksDBOptions.from(configuration).json();

    assertTrue(json.contains("\"writeBufferSize\":17825792"));
    assertTrue(json.contains("\"useDynamicLevelSize\":true"));
    assertTrue(json.contains("\"compactionStyle\":\"LEVEL\""));
    assertTrue(json.contains("\"compressionPerLevel\":[\"SNAPPY_COMPRESSION\"]"));
  }

  @Test
  void appliesPredefinedProfileBeforeExplicitOverrides() {
    Configuration configuration = new Configuration();
    configuration.set(RocksDBOptions.PREDEFINED_OPTIONS, "SPINNING_DISK_OPTIMIZED_HIGH_MEM");
    configuration.set(RocksDBConfigurableOptions.MAX_BACKGROUND_THREADS, 7);

    String json = FlinkRocksDBOptions.from(configuration).json();

    assertTrue(json.contains("\"maxBackgroundThreads\":7"));
    assertTrue(json.contains("\"blockCacheSize\":268435456"));
    assertTrue(json.contains("\"blockSize\":131072"));
    assertTrue(json.contains("\"maxSizeLevelBase\":1073741824"));
  }

  @Test
  void rejectsJavaOptionsFactoryInsteadOfSilentlyDiverging() {
    Configuration configuration = new Configuration();
    configuration.set(RocksDBOptions.OPTIONS_FACTORY, "example.OptionsFactory");

    assertThrows(IllegalArgumentException.class, () -> FlinkRocksDBOptions.from(configuration));
  }
}
