package io.github.jordepic.streamfusion.format;

import java.io.IOException;
import java.io.Serializable;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import org.apache.flink.table.types.logical.RowType;

/**
 * One native sink format instance: the wire-format code and its encode-affecting options rendered
 * as the {@code key=value} lines the native encoder parses — the encode-side counterpart of the
 * decode path's {@link NativeFormatOptions} carrier. The planner resolves a format instance once
 * per sink (value, and upsert key) and every layer below carries this pair instead of
 * format-specific parameters, so additional sink formats plug in at {@link #of} without touching
 * the exec node, operators, or the JNI surface.
 */
public final class EncodeFormat implements Serializable {

  private static final long serialVersionUID = 1L;

  /** Work a format instance defers to sink open, completing the plan-time option lines. */
  public interface OpenCompletion extends Serializable {
    String complete(String options) throws IOException;
  }

  public final int format;
  public final String options;
  private final OpenCompletion completion;

  private EncodeFormat(int format, String options, OpenCompletion completion) {
    this.format = format;
    this.options = options;
    this.completion = completion;
  }

  /**
   * Resolves one format instance from its identifier, prefix-stripped table options, and the row
   * type it will serialize, or null when the format (or one of its option values, or a row type its
   * mapping cannot carry) is not natively encoded — the planner's fallback gate. An out-of-range
   * option value also returns null so the query stays on Flink, whose own format factory raises its
   * ValidationException; the native path never runs a validation Flink would have failed. JSON is
   * part of the connector itself; other formats resolve through their installed provider artifact.
   */
  public static EncodeFormat of(String identifier, Map<String, String> options, RowType rowType) {
    int code = FormatCodes.forIdentifier(identifier);
    if (code == FormatCodes.CSV) {
      return csv(options);
    }
    if (FormatCodes.isJsonFamily(identifier)) {
      // Flink's debezium-json factory rejects schema-include on the serialization side; declining
      // keeps that ValidationException on Flink. The CDC dialects otherwise forward the shared
      // json.* option set to their nested row serializer (canal's database/table filters are
      // deserialization-only and ignored on write, as in Flink).
      if (code == FormatCodes.DEBEZIUM_JSON
          && Boolean.parseBoolean(options.get("schema-include"))) {
        return null;
      }
      EncodeFormat json = json(options);
      return json == null ? null : new EncodeFormat(code, json.options, null);
    }
    return NativeFormatProviders.forIdentifier(identifier)
        .map(
            provider ->
                provider.encodeFormat(new NativeFormatContext(rowType, rowType, options, false)))
        .orElse(null);
  }

  /** A provider-resolved format instance; a non-null completion runs once at sink open. */
  public static EncodeFormat resolved(int format, String options, OpenCompletion completion) {
    return new EncodeFormat(format, options, completion);
  }

  /**
   * The option lines the encode calls use, running any open-time completion (for example schema
   * registration, whose returned id the native framing needs). Called once per sink open; a
   * completion failure fails the job exactly like Flink's serializer failing its first record.
   */
  public String openOptions() throws IOException {
    return completion == null ? options : completion.complete(options);
  }

  /** JSON encode options resolved with Flink's json format factory defaults. */
  public static EncodeFormat json(Map<String, String> options) {
    StringBuilder encoded = new StringBuilder();
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
    String nullKeyMode =
        options.getOrDefault("map-null-key.mode", "FAIL").toUpperCase(Locale.ROOT);
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
    return new EncodeFormat(FormatCodes.JSON, encoded.toString(), null);
  }

  /**
   * CSV encode options resolved the way Flink's csv format factory configures its serializer.
   * The character options must be single ASCII characters that survive the line-encoded carrier —
   * anything else (including the option combinations Flink's own validation refuses, whose error
   * must stay Flink's) resolves to null and falls back. The factory reads every option through
   * {@code getOptional}, which never yields a ConfigOption default: notably, an UNSET
   * {@code write-bigdecimal-in-scientific-notation} leaves the serializer in plain-string mode
   * despite the option's declared default of true.
   */
  public static EncodeFormat csv(Map<String, String> options) {
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
    return new EncodeFormat(FormatCodes.CSV, encoded.toString(), null);
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
