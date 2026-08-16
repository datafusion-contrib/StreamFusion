package tech.streamfusion.planner;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

/**
 * Decides whether the native Parquet sink can honor a filesystem table's configuration exactly, and
 * resolves the effective writer settings the Rust encoder applies. The contract is 1:1 with the
 * host: every option is either honored (mapped to the encoder or consumed by the Flink writer and
 * committer classes the native sink reuses), ignored because stock Flink itself ignores it, or a
 * fallback so the host runs the sink instead of the native path silently diverging.
 *
 * <p>The ignore column is larger than it looks: Flink builds its Parquet writer through {@code
 * ParquetWriter.Builder}, which never feeds the Hadoop configuration into {@code
 * ParquetProperties}, so of all the {@code parquet.*} pass-through keys only the eight the builder
 * reads — plus the codec-level keys the {@code CodecFactory} consumes — have any effect. Keys like
 * {@code parquet.bloom.filter.enabled} are dead in stock Flink, and ignoring them here is exactly
 * the host behavior.
 *
 * <p>Timestamps gate hard: Flink's default encoding is INT96 ({@code write.int64.timestamp=false}),
 * which the Rust parquet writer cannot produce, and with {@code utc-timezone=false} even INT64
 * values are shifted through the JVM's local timezone — semantics not safely reproducible off-JVM.
 * A table with timestamp columns therefore accelerates only when both flags are explicitly true.
 */
final class ParquetSinkTranslator {

  private ParquetSinkTranslator() {}

  @FunctionalInterface
  interface HadoopConfigLookup {
    String get(String key);
  }

  static final class Result {
    private final Map<String, String> encoderConfig;
    final String fallbackReason;

    private Result(Map<String, String> encoderConfig, String fallbackReason) {
      this.encoderConfig = encoderConfig;
      this.fallbackReason = fallbackReason;
    }

    static Result translated(Map<String, String> encoderConfig) {
      return new Result(encoderConfig, null);
    }

    static Result fallback(String reason) {
      return new Result(null, reason);
    }

    String[] encoderKeys() {
      return encoderConfig.keySet().toArray(new String[0]);
    }

    String[] encoderValues() {
      return encoderConfig.values().toArray(new String[0]);
    }
  }

  /** Consumed by the reused Flink writer/committer classes, so honored without translation. */
  private static final Set<String> HOST_HONORED =
      Set.of(
          "connector",
          "format",
          "path",
          "partition.default-name",
          "sink.rolling-policy.file-size",
          "sink.rolling-policy.rollover-interval",
          "sink.rolling-policy.inactivity-interval",
          "sink.rolling-policy.check-interval",
          "partition.time-extractor.kind",
          "partition.time-extractor.class",
          "partition.time-extractor.timestamp-formatter",
          "partition.time-extractor.timestamp-pattern",
          "sink.partition-commit.trigger",
          "sink.partition-commit.delay",
          "sink.partition-commit.watermark-time-zone",
          "sink.partition-commit.policy.kind",
          "sink.partition-commit.policy.class",
          "sink.partition-commit.policy.class.parameters",
          "sink.partition-commit.success-file.name",
          "sink.parallelism");

  /**
   * No effect on this sink in stock Flink: shuffle-by-partition is registered but consumed nowhere
   * in the streaming filesystem sink, compaction sizing only applies when auto-compaction is on,
   * and source options never reach a sink.
   */
  private static final Set<String> HOST_IGNORED =
      Set.of(
          "sink.shuffle-by-partition.enable",
          "compaction.file-size",
          "compaction.parallelism",
          "source.monitor-interval",
          "source.report-statistics",
          "source.path.regex-pattern");

  /** Writer keys whose effective values are either translated or explicitly declined. */
  private static final Set<String> PARQUET_HANDLED =
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
          "parquet.write.int64.timestamp",
          "parquet.utc-timezone",
          "parquet.timestamp.time.unit");

  /**
   * Proven no-ops for Flink's streaming Parquet sink. The source batch size never reaches a sink;
   * the remaining properties are not copied from Hadoop configuration into the
   * {@code ParquetWriter.Builder}; max padding is inert because Flink's stream-backed
   * {@code OutputFile.supportsBlockSize()} is false.
   */
  private static final Set<String> PARQUET_HOST_IGNORED =
      Set.of(
          "parquet.batch-size",
          "parquet.bloom.filter.enabled",
          "parquet.page.row.count.limit",
          "parquet.statistics.truncate.length",
          "parquet.writer.max-padding");

  static Result translate(
      Map<String, String> options, RowType physicalRowType, List<String> partitionKeys) {
    return translate(
        options,
        physicalRowType,
        partitionKeys,
        ParquetSinkTranslator::clusterHadoopValue);
  }

  static Result translate(
      Map<String, String> options,
      RowType physicalRowType,
      List<String> partitionKeys,
      HadoopConfigLookup hadoopConfig) {
    if ("true".equalsIgnoreCase(options.get("auto-compaction"))) {
      return Result.fallback(
          "auto-compaction inserts Flink's compaction topology, which has no native counterpart");
    }
    for (String key : options.keySet()) {
      if (!PARQUET_HANDLED.contains(key)
          && !PARQUET_HOST_IGNORED.contains(key)
          && !HOST_HONORED.contains(key)
          && !HOST_IGNORED.contains(key)
          && !key.equals("auto-compaction")) {
        return Result.fallback(
            key.startsWith("parquet.")
                ? "unrecognized Parquet writer option " + key
                : "unrecognized filesystem sink option " + key);
      }
    }

    String typeReason = unsupportedColumnReason(physicalRowType, partitionKeys, options);
    if (typeReason != null) {
      return Result.fallback(typeReason);
    }
    return encoderConfig(options, hadoopConfig);
  }

  /**
   * Gates the written (non-partition) columns to the types whose Parquet encoding is verified
   * byte-compatible with the host writer. Partition columns never reach the encoder — their values
   * are stringified into the directory path by Flink's own partition-path code — so any type the
   * host accepts as a partition key is parity by construction.
   */
  private static String unsupportedColumnReason(
      RowType rowType, List<String> partitionKeys, Map<String, String> options) {
    boolean hasTimestamp = false;
    int writtenColumns = 0;
    for (int index = 0; index < rowType.getFieldCount(); index++) {
      if (partitionKeys.contains(rowType.getFieldNames().get(index))) {
        continue;
      }
      writtenColumns++;
      LogicalType type = rowType.getTypeAt(index);
      switch (type.getTypeRoot()) {
        case BOOLEAN:
        case TINYINT:
        case SMALLINT:
        case INTEGER:
        case BIGINT:
        case FLOAT:
        case DOUBLE:
        case CHAR:
        case VARCHAR:
        case BINARY:
        case VARBINARY:
        case DECIMAL:
        case DATE:
        case TIME_WITHOUT_TIME_ZONE:
          break;
        case TIMESTAMP_WITHOUT_TIME_ZONE:
        case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
          hasTimestamp = true;
          break;
        default:
          return "column type "
              + type.asSummaryString()
              + " is not yet verified against the host Parquet writer";
      }
    }
    if (writtenColumns == 0) {
      return "all columns are partition keys, leaving a zero-column Parquet file schema whose row"
          + " count the native writer cannot preserve";
    }
    if (hasTimestamp) {
      if (!"true".equalsIgnoreCase(options.get("parquet.write.int64.timestamp"))) {
        return "Flink writes timestamps as INT96 unless parquet.write.int64.timestamp=true, and the"
            + " native writer cannot produce INT96";
      }
      if (!"true".equalsIgnoreCase(options.get("parquet.utc-timezone"))) {
        return "parquet.utc-timezone=false shifts timestamp values through the JVM local timezone,"
            + " which the native writer does not reproduce";
      }
    }
    return null;
  }

  /**
   * Resolves the effective writer settings the way the host does: DDL {@code parquet.*} options are
   * laid over a fresh Hadoop configuration, so cluster {@code core-site.xml} values show through
   * where the DDL is silent. The eight builder-consumed keys map to the encoder; the codec-level
   * keys the {@code CodecFactory} reads resolve to concrete levels; everything else under {@code
   * parquet.} is dead in the host writer and ignored identically.
   */
  private static Result encoderConfig(
      Map<String, String> options, HadoopConfigLookup hadoopConfig) {
    Map<String, String> config = new LinkedHashMap<>();

    for (String booleanOption :
        List.of("parquet.write.int64.timestamp", "parquet.utc-timezone")) {
      String value = options.get(booleanOption);
      if (value != null && !isBoolean(value)) {
        return Result.fallback("invalid " + booleanOption + " " + value);
      }
    }

    String compression =
        effectiveValue(options, "parquet.compression", hadoopConfig, "SNAPPY")
            .toUpperCase(Locale.ROOT);
    switch (compression) {
      case "UNCOMPRESSED":
      case "SNAPPY":
        break;
      case "GZIP":
        String zlibLevel = hadoopConfig.get("zlib.compress.level");
        if (zlibLevel != null && !zlibLevel.equalsIgnoreCase("DEFAULT_COMPRESSION")) {
          return Result.fallback(
              "the cluster Hadoop configuration sets zlib.compress.level="
                  + zlibLevel
                  + ", which the native gzip codec does not mirror");
        }
        config.put("compression.gzip.level", "6");
        break;
      case "ZSTD":
        String workers =
            effectiveValue(
                options, "parquet.compression.codec.zstd.workers", hadoopConfig, "0");
        String parsedWorkers = validInt(workers);
        if (parsedWorkers == null || Long.parseLong(parsedWorkers) != 0) {
          return Result.fallback(
              "multithreaded zstd (parquet.compression.codec.zstd.workers) changes the compressed"
                  + " frame layout, which the native writer does not reproduce");
        }
        String level =
            effectiveValue(options, "parquet.compression.codec.zstd.level", hadoopConfig, "3");
        String parsedLevel = validInt(level);
        if (parsedLevel == null
            || Long.parseLong(parsedLevel) < -131_072
            || Long.parseLong(parsedLevel) > 22) {
          return Result.fallback("invalid parquet.compression.codec.zstd.level " + level);
        }
        config.put("compression.zstd.level", parsedLevel);
        break;
      default:
        return Result.fallback(
            "parquet.compression " + compression + " is not supported by the native writer");
    }
    config.put("compression", compression);

    if (!copyEffectiveSize(
            options,
            "parquet.block.size",
            "block.size",
            128 * 1024 * 1024,
            hadoopConfig,
            config)
        || !copyEffectiveSize(
            options,
            "parquet.page.size",
            "page.size",
            1024 * 1024,
            hadoopConfig,
            config)
        || !copyEffectiveSize(
            options,
            "parquet.dictionary.page.size",
            "dictionary.page.size",
            1024 * 1024,
            hadoopConfig,
            config)) {
      return Result.fallback("a parquet size option is not a positive 32-bit integer");
    }

    String dictionary =
        effectiveValue(options, "parquet.enable.dictionary", hadoopConfig, "true");
    if (!isBoolean(dictionary)) {
      return Result.fallback("invalid parquet.enable.dictionary " + dictionary);
    }
    config.put("enable.dictionary", dictionary.toLowerCase(Locale.ROOT));

    String version =
        effectiveValue(options, "parquet.writer.version", hadoopConfig, "PARQUET_1_0");
    switch (version.toUpperCase(Locale.ROOT)) {
      case "V1":
      case "PARQUET_1_0":
        config.put("writer.version", "1");
        break;
      case "V2":
      case "PARQUET_2_0":
        config.put("writer.version", "2");
        break;
      default:
        return Result.fallback("unrecognized parquet.writer.version " + version);
    }

    String unit =
        effectiveValue(options, "parquet.timestamp.time.unit", hadoopConfig, "micros");
    switch (unit.toLowerCase(Locale.ROOT)) {
      case "millis":
      case "micros":
      case "nanos":
        config.put("timestamp.unit", unit.toLowerCase(Locale.ROOT));
        break;
      default:
        return Result.fallback("unrecognized parquet.timestamp.time.unit " + unit);
    }

    String validation =
        effectiveValue(options, "parquet.validation", hadoopConfig, "false");
    if (!isBoolean(validation)) {
      return Result.fallback("invalid parquet.validation " + validation);
    }
    if (Boolean.parseBoolean(validation)) {
      return Result.fallback(
          "parquet.validation=true enables parquet-mr's record validation, which the native writer"
              + " does not replicate");
    }

    String bufferPool =
        effectiveValue(
            options,
            "parquet.compression.codec.zstd.bufferPool.enabled",
            hadoopConfig,
            "true");
    if (!isBoolean(bufferPool)) {
      return Result.fallback(
          "invalid parquet.compression.codec.zstd.bufferPool.enabled " + bufferPool);
    }

    return Result.translated(config);
  }

  private static boolean copyEffectiveSize(
      Map<String, String> options,
      String option,
      String encoderKey,
      int defaultValue,
      HadoopConfigLookup hadoopConfig,
      Map<String, String> config) {
    String value = effectiveValue(options, option, hadoopConfig, Integer.toString(defaultValue));
    String parsed = validInt(value);
    if (parsed == null) {
      return false;
    }
    long numeric = Long.parseLong(parsed);
    if (numeric <= 0 || numeric > Integer.MAX_VALUE) {
      return false;
    }
    config.put(encoderKey, parsed);
    return true;
  }

  private static boolean isBoolean(String value) {
    return "true".equalsIgnoreCase(value) || "false".equalsIgnoreCase(value);
  }

  private static String validInt(String value) {
    try {
      return Long.toString(Long.parseLong(value.trim()));
    } catch (NumberFormatException e) {
      return null;
    }
  }

  /** DDL over cluster Hadoop configuration — the same precedence the host writer resolves with. */
  private static String effectiveValue(
      Map<String, String> options,
      String key,
      HadoopConfigLookup hadoopConfig,
      String defaultValue) {
    String ddl = options.get(key);
    if (ddl != null) {
      return ddl;
    }
    String cluster = hadoopConfig.get(key);
    return cluster != null ? cluster : defaultValue;
  }

  /**
   * The value a fresh Hadoop configuration (core-default + any core-site.xml on the classpath)
   * carries for {@code key} — the base layer under the DDL options in the host's own config
   * construction. Reflective because hadoop-common is not a compile-time dependency; when it is
   * absent the host Parquet writer cannot run at all, so an absent-hadoop default can never diverge
   * from a runnable host path.
   */
  private static String clusterHadoopValue(String key) {
    try {
      Class<?> configuration = Class.forName("org.apache.hadoop.conf.Configuration");
      Object instance = configuration.getDeclaredConstructor().newInstance();
      return (String) configuration.getMethod("get", String.class).invoke(instance, key);
    } catch (ReflectiveOperationException | LinkageError e) {
      return null;
    }
  }
}
