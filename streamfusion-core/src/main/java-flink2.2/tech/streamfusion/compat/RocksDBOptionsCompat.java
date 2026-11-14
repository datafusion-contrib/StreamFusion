package tech.streamfusion.compat;

import java.util.List;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.state.rocksdb.RocksDBConfigurableOptions;
import org.apache.flink.state.rocksdb.RocksDBOptions;
import org.rocksdb.CompressionType;

/** The selected Flink line owns option keys and defaults. */
public final class RocksDBOptionsCompat extends RocksDBConfigurableOptions {
  private RocksDBOptionsCompat() {}

  public static final ConfigOption<String> PREDEFINED_OPTIONS = RocksDBOptions.PREDEFINED_OPTIONS;
  public static final ConfigOption<String> OPTIONS_FACTORY = RocksDBOptions.OPTIONS_FACTORY;
  public static final ConfigOption<String> LOCAL_DIRECTORIES = RocksDBOptions.LOCAL_DIRECTORIES;
  public static final ConfigOption<Boolean> USE_MANAGED_MEMORY = RocksDBOptions.USE_MANAGED_MEMORY;
  public static final ConfigOption<MemorySize> FIX_PER_SLOT_MEMORY_SIZE =
      RocksDBOptions.FIX_PER_SLOT_MEMORY_SIZE;
  public static final ConfigOption<MemorySize> FIX_PER_TM_MEMORY_SIZE =
      RocksDBOptions.FIX_PER_TM_MEMORY_SIZE;
  public static final ConfigOption<Double> WRITE_BUFFER_RATIO = RocksDBOptions.WRITE_BUFFER_RATIO;

  public static List<CompressionType> compressionPerLevel(ReadableConfig config) {
    return config.get(COMPRESSION_PER_LEVEL);
  }

  public static long compactionQueryEntries(ReadableConfig config) {
    return config.get(COMPACT_FILTER_QUERY_TIME_AFTER_NUM_ENTRIES);
  }

  public static long periodicCompactionSeconds(ReadableConfig config) {
    return config.get(COMPACT_FILTER_PERIODIC_COMPACTION_TIME).getSeconds();
  }
}
