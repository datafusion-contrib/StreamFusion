package tech.streamfusion.delta;

import io.delta.flink.table.HadoopTable;
import io.delta.kernel.DataWriteContext;
import io.delta.kernel.Transaction;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.expressions.Literal;
import io.delta.kernel.internal.data.TransactionStateRow;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.DataFileStatus;
import java.net.URI;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;

/** Published Delta Hadoop table with only its data-file Parquet service replaced. */
public final class NativeDeltaHadoopTable extends HadoopTable {
  private final Map<String, String> hadoopOptions;

  public NativeDeltaHadoopTable(
      URI tablePath,
      Map<String, String> configurations,
      StructType schema,
      List<String> partitionColumns) {
    super(tablePath, configurations, schema, partitionColumns);
    hadoopOptions = Map.copyOf(configurations);
  }

  @Override
  public CloseableIterator<Row> writeParquet(
      String pathSuffix,
      CloseableIterator<FilteredColumnarBatch> data,
      Map<String, Literal> partitionValues) {
    return withRetry(
        () -> {
          Engine engine = getEngine();
          Row writeState = getWriteState();
          Configuration configuration = new Configuration();
          hadoopOptions.forEach(configuration::set);
          EncoderConfig encoderConfig =
              encoderConfig(
                  configuration,
                  hadoopOptions.keySet(),
                  TransactionStateRow.getConfiguration(writeState));
          if (encoderConfig.fallbackReason != null) {
            return super.writeParquet(pathSuffix, data, partitionValues);
          }
          CloseableIterator<FilteredColumnarBatch> physicalData =
              Transaction.transformLogicalData(engine, writeState, data, partitionValues);
          DataWriteContext writeContext =
              Transaction.getWriteContext(engine, writeState, partitionValues);
          CloseableIterator<DataFileStatus> dataFiles =
              new NativeDeltaParquetHandler(
                      engine.getParquetHandler(),
                      configuration,
                      encoderConfig.keys,
                      encoderConfig.values)
                  .writeParquetFiles(
                      getTablePath().resolve(pathSuffix).toString(),
                      physicalData,
                      writeContext.getStatisticsColumns());
          return Transaction.generateAppendActions(engine, writeState, dataFiles, writeContext);
        });
  }

  /** Resolves every Parquet setting consumed by Delta Kernel's default data-file writer. */
  private static EncoderConfig encoderConfig(
      Configuration configuration,
      Set<String> explicitHadoopOptions,
      Map<String, String> tableProperties) {
    Map<String, String> translated = new java.util.LinkedHashMap<>();
    String compression =
        tableProperties.getOrDefault(
            "delta.parquet.compression.codec", configuration.get("parquet.compression", "SNAPPY"));
    compression = compression.toUpperCase(Locale.ROOT);
    if ("NONE".equals(compression)) {
      compression = "UNCOMPRESSED";
    }
    switch (compression) {
      case "UNCOMPRESSED", "SNAPPY" -> {}
      case "GZIP" -> {
        String zlibLevel = configuration.get("zlib.compress.level");
        if (zlibLevel != null && !"DEFAULT_COMPRESSION".equalsIgnoreCase(zlibLevel)) {
          return EncoderConfig.fallback(
              "custom zlib.compress.level is not supported by the native writer");
        }
        translated.put("compression.gzip.level", "6");
      }
      case "ZSTD" -> {
        String workers = configuration.get("parquet.compression.codec.zstd.workers", "0").trim();
        if (!"0".equals(workers)) {
          return EncoderConfig.fallback("multithreaded ZSTD is not supported by the native writer");
        }
        String level = configuration.get("parquet.compression.codec.zstd.level", "3").trim();
        try {
          int parsed = Integer.parseInt(level);
          if (parsed < -131_072 || parsed > 22) {
            return EncoderConfig.fallback("invalid ZSTD compression level " + level);
          }
        } catch (NumberFormatException invalidLevel) {
          return EncoderConfig.fallback("invalid ZSTD compression level " + level);
        }
        translated.put("compression.zstd.level", level);
      }
      default -> {
        return EncoderConfig.fallback(
            "Delta compression codec " + compression + " is not supported by the native writer");
      }
    }
    translated.put("compression", compression);

    String sizeFailure =
        copyPositiveInt(
            configuration, "parquet.block.size", "block.size", 128 * 1024 * 1024, translated);
    if (sizeFailure == null) {
      sizeFailure =
          copyPositiveInt(configuration, "parquet.page.size", "page.size", 1024 * 1024, translated);
    }
    if (sizeFailure == null) {
      sizeFailure =
          copyPositiveInt(
              configuration,
              "parquet.dictionary.page.size",
              "dictionary.page.size",
              1024 * 1024,
              translated);
    }
    if (sizeFailure != null) {
      return EncoderConfig.fallback(sizeFailure);
    }

    String dictionary = configuration.get("parquet.enable.dictionary", "true");
    if (!isBoolean(dictionary)) {
      return EncoderConfig.fallback("invalid parquet.enable.dictionary " + dictionary);
    }
    translated.put("enable.dictionary", dictionary.toLowerCase(Locale.ROOT));

    String version = configuration.get("parquet.writer.version", "PARQUET_1_0");
    switch (version.toUpperCase(Locale.ROOT)) {
      case "V1", "PARQUET_1_0" -> translated.put("writer.version", "1");
      case "V2", "PARQUET_2_0" -> translated.put("writer.version", "2");
      default -> {
        return EncoderConfig.fallback("unsupported parquet.writer.version " + version);
      }
    }
    translated.put("timestamp.unit", "micros");

    String validation = configuration.get("parquet.validation", "false");
    if (!isBoolean(validation) || Boolean.parseBoolean(validation)) {
      return EncoderConfig.fallback(
          "parquet.validation is invalid or unsupported by the native writer");
    }
    String maxPadding = configuration.get("parquet.writer.max-padding");
    if (maxPadding != null) {
      return EncoderConfig.fallback(
          "parquet.writer.max-padding is not supported by the native writer");
    }
    String bufferPool =
        configuration.get("parquet.compression.codec.zstd.bufferPool.enabled", "true");
    if (!isBoolean(bufferPool) || !Boolean.parseBoolean(bufferPool)) {
      return EncoderConfig.fallback(
          "disabled ZSTD buffer pooling is not supported by the native writer");
    }
    if (configuration.get("delta.kernel.default.parquet.writer.targetMaxFileSize") != null) {
      return EncoderConfig.fallback(
          "custom Delta Kernel targetMaxFileSize is not reproduced by the native writer");
    }

    Set<String> recognized =
        Set.of(
            "parquet.compression",
            "parquet.compression.codec.zstd.level",
            "parquet.compression.codec.zstd.workers",
            "parquet.compression.codec.zstd.bufferPool.enabled",
            "parquet.block.size",
            "parquet.page.size",
            "parquet.dictionary.page.size",
            "parquet.enable.dictionary",
            "parquet.writer.version",
            "parquet.validation",
            "parquet.writer.max-padding");
    Set<String> explicit = new HashSet<>();
    for (String key : explicitHadoopOptions) {
      if (key.startsWith("parquet.")) {
        explicit.add(key);
      }
    }
    explicit.removeAll(recognized);
    if (!explicit.isEmpty()) {
      return EncoderConfig.fallback("unrecognized Delta Parquet settings " + explicit);
    }
    return EncoderConfig.translated(translated);
  }

  private static String copyPositiveInt(
      Configuration configuration,
      String sourceKey,
      String targetKey,
      int defaultValue,
      Map<String, String> translated) {
    String value = configuration.get(sourceKey, Integer.toString(defaultValue)).trim();
    try {
      int parsed = Integer.parseInt(value);
      if (parsed <= 0) {
        return sourceKey + " must be a positive 32-bit integer";
      }
      translated.put(targetKey, Integer.toString(parsed));
      return null;
    } catch (NumberFormatException invalidSize) {
      return sourceKey + " must be a positive 32-bit integer";
    }
  }

  private static boolean isBoolean(String value) {
    return "true".equalsIgnoreCase(value) || "false".equalsIgnoreCase(value);
  }

  private static final class EncoderConfig {
    private final String[] keys;
    private final String[] values;
    private final String fallbackReason;

    private EncoderConfig(String[] keys, String[] values, String fallbackReason) {
      this.keys = keys;
      this.values = values;
      this.fallbackReason = fallbackReason;
    }

    private static EncoderConfig translated(Map<String, String> options) {
      return new EncoderConfig(
          options.keySet().toArray(new String[0]), options.values().toArray(new String[0]), null);
    }

    private static EncoderConfig fallback(String reason) {
      return new EncoderConfig(null, null, reason);
    }
  }
}
