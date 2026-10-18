package tech.streamfusion.iceberg;

import static org.apache.iceberg.TableProperties.*;

import com.fasterxml.jackson.core.JsonProcessingException;
import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/** Effective host writer properties admitted before any native writer opens. */
final class IcebergParquetSettings implements Serializable {
  private static final Set<String> TRANSLATED =
      Set.of(
          PARQUET_COMPRESSION,
          PARQUET_COMPRESSION_LEVEL,
          PARQUET_ROW_GROUP_SIZE_BYTES,
          PARQUET_PAGE_SIZE_BYTES,
          PARQUET_PAGE_ROW_LIMIT,
          PARQUET_DICT_SIZE_BYTES,
          PARQUET_PAGE_VERSION,
          "parquet.enable.dictionary");
  private static final Map<String, String> DEFAULT_ONLY =
      Map.of(
          PARQUET_ROW_GROUP_CHECK_MIN_RECORD_COUNT,
          "100",
          PARQUET_ROW_GROUP_CHECK_MAX_RECORD_COUNT,
          "10000",
          PARQUET_ROW_GROUP_SIZE_TRACK_UNCOMPRESSED,
          "false",
          PARQUET_SHRED_VARIANTS,
          "false",
          PARQUET_VARIANT_BUFFER_SIZE,
          "100");

  private final String json;

  private IcebergParquetSettings(String json) {
    this.json = json;
  }

  static IcebergParquetSettings translate(Map<String, String> properties) {
    for (var entry : properties.entrySet()) {
      String key = entry.getKey();
      if (!key.startsWith("write.parquet.") && !key.startsWith("parquet.")) continue;
      if (TRANSLATED.contains(key)) continue;
      if (entry.getValue().equalsIgnoreCase(DEFAULT_ONLY.get(key))) continue;
      throw new IllegalArgumentException("Unverified Iceberg Parquet setting: " + key);
    }
    String codec =
        properties.getOrDefault(PARQUET_COMPRESSION, PARQUET_COMPRESSION_DEFAULT_SINCE_1_4_0);
    if (!Set.of("uncompressed", "snappy", "zstd", "gzip").contains(codec)) {
      throw new IllegalArgumentException("Unverified Iceberg Parquet codec: " + codec);
    }
    String version = properties.getOrDefault(PARQUET_PAGE_VERSION, PARQUET_PAGE_VERSION_DEFAULT);
    if (!Set.of("v1", "v2").contains(version)) {
      throw new IllegalArgumentException("Unverified Iceberg Parquet page version: " + version);
    }
    Map<String, Object> settings = new LinkedHashMap<>();
    settings.put("compression", codec);
    if (properties.containsKey(PARQUET_COMPRESSION_LEVEL)) {
      if (!Set.of("zstd", "gzip").contains(codec)) {
        throw new IllegalArgumentException("Unverified compression level for " + codec);
      }
      int level = Integer.parseInt(properties.get(PARQUET_COMPRESSION_LEVEL));
      if ((codec.equals("zstd") && (level < -131072 || level > 22))
          || (codec.equals("gzip") && (level < 0 || level > 9))) {
        throw new IllegalArgumentException("Invalid Iceberg compression level");
      }
      settings.put("compression_level", level);
    }
    settings.put(
        "row_group_bytes",
        positive(properties, PARQUET_ROW_GROUP_SIZE_BYTES, PARQUET_ROW_GROUP_SIZE_BYTES_DEFAULT));
    settings.put(
        "page_bytes",
        positive(properties, PARQUET_PAGE_SIZE_BYTES, PARQUET_PAGE_SIZE_BYTES_DEFAULT));
    settings.put(
        "page_rows", positive(properties, PARQUET_PAGE_ROW_LIMIT, PARQUET_PAGE_ROW_LIMIT_DEFAULT));
    settings.put(
        "dictionary_bytes",
        positive(properties, PARQUET_DICT_SIZE_BYTES, PARQUET_DICT_SIZE_BYTES_DEFAULT));
    String dictionary = properties.getOrDefault("parquet.enable.dictionary", "true");
    if (!Set.of("true", "false").contains(dictionary))
      throw new IllegalArgumentException("Invalid dictionary flag");
    settings.put("dictionary_enabled", Boolean.parseBoolean(dictionary));
    settings.put("version", version.equals("v1") ? 1 : 2);
    try {
      return new IcebergParquetSettings(IcebergJson.mapper().writeValueAsString(settings));
    } catch (JsonProcessingException e) {
      throw new IllegalStateException(e);
    }
  }

  private static int positive(Map<String, String> properties, String key, int defaultValue) {
    int value = Integer.parseInt(properties.getOrDefault(key, Integer.toString(defaultValue)));
    if (value <= 0) throw new IllegalArgumentException("Iceberg setting must be positive: " + key);
    return value;
  }

  String json() {
    return json;
  }
}
