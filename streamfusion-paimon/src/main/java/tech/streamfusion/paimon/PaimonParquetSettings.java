package tech.streamfusion.paimon;

import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.paimon.options.Options;

/**
 * Translates the Parquet writer settings Paimon's own writer honours into native encoder options.
 * Every key parquet-mr would read is either reproduced or declines the native writer; an unknown
 * {@code parquet.*} key declines too, so a setting the native writer does not understand can never be
 * silently dropped.
 */
public final class PaimonParquetSettings {
  static final String ZSTD_LEVEL_KEY = "parquet.compression.codec.zstd.level";
  static final String BLOCK_SIZE_KEY = "parquet.block.size";

  private static final Set<String> RECOGNIZED =
      Set.of(
          "parquet.compression",
          ZSTD_LEVEL_KEY,
          "parquet.compression.codec.zstd.workers",
          "parquet.compression.codec.zstd.bufferPool.enabled",
          BLOCK_SIZE_KEY,
          "parquet.page.size",
          "parquet.dictionary.page.size",
          "parquet.enable.dictionary",
          "parquet.writer.version",
          "parquet.validation",
          "parquet.writer.max-padding",
          "parquet.bloom.filter.enabled",
          "parquet.page.row.count.limit",
          "parquet.statistics.truncate.length",
          "parquet.columnindex.truncate.length",
          "parquet.page.size.row.check.min",
          "parquet.page.size.row.check.max");

  private final String[] keys;
  private final String[] values;
  @Nullable private final String fallbackReason;

  private PaimonParquetSettings(Map<String, String> translated, @Nullable String fallbackReason) {
    this.keys = translated.keySet().toArray(new String[0]);
    this.values = translated.values().toArray(new String[0]);
    this.fallbackReason = fallbackReason;
  }

  private static PaimonParquetSettings fallback(String reason) {
    return new PaimonParquetSettings(Map.of(), reason);
  }

  /**
   * @param parquetOptions the format's {@code parquet.*} options, as Paimon's writer builder sees them
   * @param compression the per-file compression Paimon passes to the writer factory
   */
  public static PaimonParquetSettings translate(Options parquetOptions, String compression) {
    Map<String, String> translated = new LinkedHashMap<>();
    translated.put("schema.shape", "paimon");
    translated.put("timestamp.unit", "micros");

    String codec = parquetOptions.getString("parquet.compression", compression);
    codec = codec == null ? "UNCOMPRESSED" : codec.toUpperCase(Locale.ROOT);
    if ("NONE".equals(codec)) {
      codec = "UNCOMPRESSED";
    }
    switch (codec) {
      case "UNCOMPRESSED":
      case "SNAPPY":
        {
          {
          }
          break;
        }
      case "GZIP":
        translated.put("compression.gzip.level", "6");
        break;
      case "ZSTD":
        {
          {
        String workers = parquetOptions.getString("parquet.compression.codec.zstd.workers", "0");
        if (!"0".equals(workers.trim())) {
          return fallback("multithreaded ZSTD is not supported by the native writer");
        }
        String level = parquetOptions.getString(ZSTD_LEVEL_KEY, "1").trim();
        try {
          int parsed = Integer.parseInt(level);
          if (parsed < -131_072 || parsed > 22) {
            return fallback("invalid ZSTD compression level " + level);
          }
        } catch (NumberFormatException invalidLevel) {
          return fallback("invalid ZSTD compression level " + level);
        }
        translated.put("compression.zstd.level", level);
      }
          break;
        }
      default:
        {
          {
        return fallback("Parquet compression " + codec + " is not supported by the native writer");
      }
        }
    }
    translated.put("compression", codec);

    String sizeFailure =
        copyPositiveInt(
            parquetOptions, BLOCK_SIZE_KEY, "block.size", 128 * 1024 * 1024, translated);
    if (sizeFailure == null) {
      sizeFailure =
          copyPositiveInt(
              parquetOptions, "parquet.page.size", "page.size", 1024 * 1024, translated);
    }
    if (sizeFailure == null) {
      sizeFailure =
          copyPositiveInt(
              parquetOptions,
              "parquet.dictionary.page.size",
              "dictionary.page.size",
              1024 * 1024,
              translated);
    }
    if (sizeFailure != null) {
      return fallback(sizeFailure);
    }

    String dictionary = parquetOptions.getString("parquet.enable.dictionary", "true");
    if (!isBoolean(dictionary)) {
      return fallback("invalid parquet.enable.dictionary " + dictionary);
    }
    translated.put("enable.dictionary", dictionary.toLowerCase(Locale.ROOT));

    String version = parquetOptions.getString("parquet.writer.version", "PARQUET_1_0");
    switch (version.toUpperCase(Locale.ROOT)) {
      case "V1":
      case "PARQUET_1_0":
        translated.put("writer.version", "1");
        break;
      case "V2":
      case "PARQUET_2_0":
        translated.put("writer.version", "2");
        break;
      default:
        {
          {
        return fallback("unsupported parquet.writer.version " + version);
      }
        }
    }

    String validation = parquetOptions.getString("parquet.validation", "false");
    if (!isBoolean(validation) || Boolean.parseBoolean(validation)) {
      return fallback("parquet.validation is invalid or unsupported by the native writer");
    }
    String bloomFilter = parquetOptions.getString("parquet.bloom.filter.enabled", "false");
    if (!isBoolean(bloomFilter) || Boolean.parseBoolean(bloomFilter)) {
      return fallback("Parquet bloom filters are not supported by the native writer");
    }
    String bufferPool =
        parquetOptions.getString("parquet.compression.codec.zstd.bufferPool.enabled", "true");
    if (!isBoolean(bufferPool) || !Boolean.parseBoolean(bufferPool)) {
      return fallback("disabled ZSTD buffer pooling is not supported by the native writer");
    }
    for (String key :
        new String[] {
          "parquet.writer.max-padding",
          "parquet.page.row.count.limit",
          "parquet.statistics.truncate.length",
          "parquet.columnindex.truncate.length",
          "parquet.page.size.row.check.min",
          "parquet.page.size.row.check.max"
        }) {
      if (parquetOptions.containsKey(key)) {
        return fallback(key + " is not reproduced by the native writer");
      }
    }
    for (String key : parquetOptions.keySet()) {
      if (!RECOGNIZED.contains(key)) {
        return fallback("unrecognized Parquet writer setting " + key);
      }
    }
    return new PaimonParquetSettings(translated, null);
  }

  private static String copyPositiveInt(
      Options options,
      String sourceKey,
      String targetKey,
      int defaultValue,
      Map<String, String> translated) {
    String value = options.getString(sourceKey, Integer.toString(defaultValue)).trim();
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

  @Nullable
  public String fallbackReason() {
    return fallbackReason;
  }

  String[] keys() {
    return keys;
  }

  String[] values() {
    return values;
  }
}
