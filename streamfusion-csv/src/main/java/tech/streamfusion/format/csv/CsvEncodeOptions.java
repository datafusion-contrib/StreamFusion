package tech.streamfusion.format.csv;

import java.util.Map;
import tech.streamfusion.format.NativeFormatOptions;

/** Validated Flink Csv serializer options carried to this format's native library. */
public final class CsvEncodeOptions {
  private CsvEncodeOptions() {}

  /**
   * CSV encode options resolved the way Flink's csv format factory configures its serializer. The
   * character options must be single ASCII characters that survive the line-encoded carrier —
   * anything else (including the option combinations Flink's own validation refuses, whose error
   * must stay Flink's) resolves to null and falls back. The factory reads every option through
   * {@code getOptional}, which never yields a ConfigOption default: notably, an UNSET {@code
   * write-bigdecimal-in-scientific-notation} leaves the serializer in plain-string mode despite the
   * option's declared default of true.
   */
  public static String encode(Map<String, String> options) {
    // Flink's factory validation parses the deser-only booleans eagerly even on the write path;
    // a malformed value must stay on Flink so its factory raises the error.
    if (!validBooleanWhenPresent(options, "allow-comments")
        || !validBooleanWhenPresent(options, "ignore-parse-errors")
        || !validBooleanWhenPresent(options, "disable-quote-character")) {
      return null;
    }
    boolean quoteDisabled = Boolean.parseBoolean(options.get("disable-quote-character"));
    String quote = options.get("quote-character");
    if (quoteDisabled && quote != null) {
      // "Format cannot define a quote character and disabled quote character at the same time."
      return null;
    }
    StringBuilder encoded = new StringBuilder();
    if (!tech.streamfusion.compat.JsonRuntimeCompat.PRESERVES_DECIMAL_SCALE) {
      encoded.append("legacy-decimal-nodes=true\n");
    }
    String delimiter = options.get("field-delimiter");
    if (delimiter != null) {
      Character unescaped = NativeFormatOptions.unescapedDelimiter(delimiter);
      if (unescaped == null
          || !NativeFormatOptions.appendChar(encoded, "field-delimiter", unescaped)) {
        return null;
      }
    }
    if (quoteDisabled) {
      encoded.append("disable-quote-character=true\n");
    }
    if (!appendSingleChar(encoded, "quote-character", quote)
        || !appendSingleChar(
            encoded, "array-element-delimiter", options.get("array-element-delimiter"))
        || !appendSingleChar(encoded, "escape-character", options.get("escape-character"))) {
      return null;
    }
    String nullLiteral = options.get("null-literal");
    if (nullLiteral != null) {
      if (nullLiteral.contains("\n") || nullLiteral.contains("\r")) {
        return null;
      }
      encoded.append("null-literal=").append(nullLiteral).append('\n');
    }
    if (!appendBoolean(encoded, "write-bigdecimal-in-scientific-notation", options)) {
      return null;
    }
    return encoded.toString();
  }

  /** A single-character option as Flink validates it; null when absent, false when unusable. */
  private static boolean appendSingleChar(StringBuilder encoded, String key, String value) {
    if (value == null) {
      return true;
    }
    return value.length() == 1 && NativeFormatOptions.appendChar(encoded, key, value.charAt(0));
  }

  private static boolean validBooleanWhenPresent(Map<String, String> options, String key) {
    String value = options.get(key);
    return value == null || "true".equalsIgnoreCase(value) || "false".equalsIgnoreCase(value);
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
