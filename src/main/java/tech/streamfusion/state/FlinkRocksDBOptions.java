package tech.streamfusion.state;

import static org.apache.flink.state.rocksdb.RocksDBConfigurableOptions.*;
import static org.apache.flink.state.rocksdb.RocksDBOptions.OPTIONS_FACTORY;
import static org.apache.flink.state.rocksdb.RocksDBOptions.PREDEFINED_OPTIONS;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.ReadableConfig;
import org.rocksdb.CompactionStyle;
import org.rocksdb.CompressionType;

import java.io.Serializable;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

/**
 * Flink's resolved public RocksDB options, serialized once at backend construction and handed to
 * Rust when a native state store opens. Explicit configuration wins over the predefined profile,
 * matching {@code RocksDBResourceContainer}'s precedence.
 */
public final class FlinkRocksDBOptions implements Serializable {

  private static final long serialVersionUID = 1L;

  private final String json;

  private FlinkRocksDBOptions(String json) {
    this.json = json;
  }

  public static FlinkRocksDBOptions from(ReadableConfig config) {
    if (config.getOptional(OPTIONS_FACTORY).filter(value -> !value.isBlank()).isPresent()) {
      throw new IllegalArgumentException(
          "state.backend.rocksdb.options-factory cannot configure StreamFusion's Rust-owned "
              + "RocksDB instance; use Flink's state.backend.rocksdb.* options instead");
    }
    String profile = config.get(PREDEFINED_OPTIONS).toUpperCase(Locale.ROOT);

    int threads = profileValue(config, MAX_BACKGROUND_THREADS, profile, 4);
    int openFiles = profileValue(config, MAX_OPEN_FILES, profile, -1);
    boolean dynamic =
        profileValue(
            config,
            USE_DYNAMIC_LEVEL_SIZE,
            profile,
            profile.equals("SPINNING_DISK_OPTIMIZED")
                || profile.equals("SPINNING_DISK_OPTIMIZED_HIGH_MEM"));
    long maxLevel =
        profileValue(
                config,
                MAX_SIZE_LEVEL_BASE,
                profile,
                MemorySize.parse("1gb"))
            .getBytes();
    long targetFile =
        profileValue(
                config,
                TARGET_FILE_SIZE_BASE,
                profile,
                MemorySize.parse("256mb"))
            .getBytes();
    long writeBuffer =
        profileValue(config, WRITE_BUFFER_SIZE, profile, MemorySize.parse("64mb")).getBytes();
    int buffers = profileValue(config, MAX_WRITE_BUFFER_NUMBER, profile, 4);
    int mergeBuffers = profileValue(config, MIN_WRITE_BUFFER_NUMBER_TO_MERGE, profile, 3);
    long blockSize =
        profileValue(config, BLOCK_SIZE, profile, MemorySize.parse("128kb")).getBytes();
    long cacheSize =
        profileValue(config, BLOCK_CACHE_SIZE, profile, MemorySize.parse("256mb")).getBytes();

    String compression =
        config.get(COMPRESSION_PER_LEVEL).stream()
            .map(CompressionType::name)
            .map(FlinkRocksDBOptions::quote)
            .collect(Collectors.joining(","));
    String json =
        "{"
            + field("maxBackgroundThreads", threads)
            + field("maxOpenFiles", openFiles)
            + field("logMaxFileSize", config.get(LOG_MAX_FILE_SIZE).getBytes())
            + field("logFileNum", config.get(LOG_FILE_NUM))
            + field(
                "logDirectory",
                config.getOptional(LOG_DIR).map(FlinkRocksDBOptions::quote).orElse("null"))
            + field("logLevel", quote(config.get(LOG_LEVEL).name()))
            + field("compactionStyle", quote(config.get(COMPACTION_STYLE).name()))
            + "\"compressionPerLevel\":["
            + compression
            + "],"
            + field("useDynamicLevelSize", dynamic)
            + field("targetFileSizeBase", targetFile)
            + field("maxSizeLevelBase", maxLevel)
            + field("writeBufferSize", writeBuffer)
            + field("maxWriteBufferNumber", buffers)
            + field("minWriteBufferNumberToMerge", mergeBuffers)
            + field("writeBatchSize", config.get(WRITE_BATCH_SIZE).getBytes())
            + field(
                "compactionFilterQueryTimeAfterNumEntries",
                config.get(COMPACT_FILTER_QUERY_TIME_AFTER_NUM_ENTRIES))
            + field(
                "periodicCompactionSeconds",
                config.get(COMPACT_FILTER_PERIODIC_COMPACTION_TIME).getSeconds())
            + field("blockSize", blockSize)
            + field("metadataBlockSize", config.get(METADATA_BLOCK_SIZE).getBytes())
            + field("blockCacheSize", cacheSize)
            + field("useBloomFilter", config.get(USE_BLOOM_FILTER))
            + field("bloomFilterBitsPerKey", config.get(BLOOM_FILTER_BITS_PER_KEY))
            + "\"bloomFilterBlockBasedMode\":"
            + config.get(BLOOM_FILTER_BLOCK_BASED_MODE)
            + "}";
    return new FlinkRocksDBOptions(json);
  }

  public String json() {
    return json;
  }

  private static int profileValue(
      ReadableConfig config, ConfigOption<Integer> option, String profile, int highMemoryValue) {
    if (config.getOptional(option).isPresent()) {
      return config.get(option);
    }
    if (profile.equals("SPINNING_DISK_OPTIMIZED_HIGH_MEM")) {
      return highMemoryValue;
    }
    if ((option == MAX_BACKGROUND_THREADS || option == MAX_OPEN_FILES)
        && (profile.equals("SPINNING_DISK_OPTIMIZED")
            || profile.equals("FLASH_SSD_OPTIMIZED"))) {
      return highMemoryValue;
    }
    return option.defaultValue();
  }

  private static boolean profileValue(
      ReadableConfig config,
      ConfigOption<Boolean> option,
      String profile,
      boolean profileValue) {
    return config.getOptional(option).orElse(profileValue);
  }

  private static MemorySize profileValue(
      ReadableConfig config,
      ConfigOption<MemorySize> option,
      String profile,
      MemorySize highMemoryValue) {
    if (config.getOptional(option).isPresent()) {
      return config.get(option);
    }
    return profile.equals("SPINNING_DISK_OPTIMIZED_HIGH_MEM")
        ? highMemoryValue
        : option.defaultValue();
  }

  private static String field(String name, Object value) {
    return quote(name) + ":" + value + ",";
  }

  private static String quote(String value) {
    return "\"" + value.replace("\\", "\\\\").replace("\"", "\\\"") + "\"";
  }
}
