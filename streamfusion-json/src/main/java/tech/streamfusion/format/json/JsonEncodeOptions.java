package tech.streamfusion.format.json;

import java.util.Locale;
import java.util.Map;
import java.util.Set;

/** Validated Flink Json serializer options carried to this format's native library. */
public final class JsonEncodeOptions {
  private JsonEncodeOptions() {}

  /** JSON encode options resolved with Flink's json format factory defaults. */
  public static String encode(Map<String, String> options) {
    StringBuilder encoded = new StringBuilder();
    if (!tech.streamfusion.compat.JsonRuntimeCompat.PRESERVES_DECIMAL_SCALE) {
      encoded.append("legacy-decimal-nodes=true\n");
    }
    String timestampFormat = options.getOrDefault("timestamp-format.standard", "SQL");
    if ("ISO-8601".equals(timestampFormat)) {
      encoded.append("timestamp-format=ISO-8601\n");
    } else if (!"SQL".equals(timestampFormat)) {
      return null;
    }
    if (!appendBoolean(encoded, "encode.ignore-null-fields", options)
        || !appendBoolean(encoded, "encode.decimal-as-plain-number", options)) {
      return null;
    }
    String nullKeyMode = options.getOrDefault("map-null-key.mode", "FAIL").toUpperCase(Locale.ROOT);
    if (!Set.of("FAIL", "DROP", "LITERAL").contains(nullKeyMode)) {
      return null;
    }
    if (!"FAIL".equals(nullKeyMode)) {
      encoded.append("map-null-key.mode=").append(nullKeyMode).append('\n');
    }
    String nullKeyLiteral = options.get("map-null-key.literal");
    if (nullKeyLiteral != null && !"null".equals(nullKeyLiteral)) {
      // The carrier is line-encoded; a literal that cannot ride it stays on Flink.
      if (nullKeyLiteral.contains("\n") || nullKeyLiteral.contains("\r")) {
        return null;
      }
      encoded.append("map-null-key.literal=").append(nullKeyLiteral).append('\n');
    }
    return encoded.toString();
  }

  /**
   * A boolean option the way Flink's configuration reads it: absent or exactly true/false
   * (case-insensitive). Anything else is not appended and fails the resolution.
   */
  private static boolean appendBoolean(
      StringBuilder encoded, String key, Map<String, String> options) {
    String value = options.get(key);
    if (value == null) {
      return true;
    }
    if (!"true".equalsIgnoreCase(value) && !"false".equalsIgnoreCase(value)) {
      return false;
    }
    if (Boolean.parseBoolean(value)) {
      encoded.append(key).append("=true\n");
    }
    return true;
  }
}
