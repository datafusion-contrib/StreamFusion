package io.github.jordepic.streamfusion.format;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Map;

/** Native decoder option encoding shared by planner gates and format artifacts. */
public final class NativeFormatOptions {

  private NativeFormatOptions() {}

  /** A value-format option, resolved with Flink's own prefixing ({@code FactoryUtil.getFormatPrefix}):
   * {@code csv.field-delimiter} when the table uses {@code format = 'csv'}, but
   * {@code value.csv.field-delimiter} when it uses {@code value.format = 'csv'}. */
  public static String option(Map<String, String> options, String suffix) {
    String valueFormat = options.get("value.format");
    return valueFormat != null
        ? options.get("value." + valueFormat + "." + suffix)
        : options.get(options.get("format") + "." + suffix);
  }

  /**
   * The decode-relevant format options rendered for the native decoder as {@code key=value} lines,
   * or null when an option value the native decode can't reproduce is present — behavior that must
   * stay on Flink (the fallback gate). CSV carries the Jackson {@code CsvSchema} knobs; the JSON
   * family (plain {@code json} and the CDC envelopes) carries {@code timestamp-format.standard} and
   * gates {@code fail-on-missing-field}; raw carries {@code raw.endianness} and gates a non-UTF-8
   * {@code raw.charset}. The CSV delimiter is Java-unescaped and truncated to its
   * first character exactly as {@code CsvFormatFactory} does; quote is a literal single character
   * (the factory validates the length). Each must be ASCII — csv-core splits on bytes — and a null
   * literal must fit the line encoding.
   */
  public static String encode(Map<String, String> options) {
    String format = NativeFormatProviders.formatIdentifier(options);
    if ("raw".equals(format)) {
      return encodeRaw(options);
    }
    StringBuilder encoded = new StringBuilder();
    if (FormatCodes.isJsonFamily(format)) {
      // A missing field is null natively (Flink's default); the fail mode isn't modeled.
      if ("true".equalsIgnoreCase(option(options, "fail-on-missing-field"))) {
        return null;
      }
      // json.decode.json-parser.enabled = false switches Flink to its tree deserializer, whose
      // coercion envelope differs from the parser path the native decode mirrors.
      if ("false".equalsIgnoreCase(option(options, "decode.json-parser.enabled"))) {
        return null;
      }
      String timestampFormat = option(options, "timestamp-format.standard");
      if (timestampFormat == null || "SQL".equals(timestampFormat)) {
        return encoded.toString();
      }
      // The factory validates the value, so returning null for anything else is defensive.
      return "ISO-8601".equals(timestampFormat) ? "timestamp-format=ISO-8601\n" : null;
    }
    if (!"csv".equals(format)) {
      return encoded.toString();
    }
    String delimiter = option(options, "field-delimiter");
    if (delimiter != null) {
      Character c = unescapedDelimiter(delimiter);
      if (c == null || !appendChar(encoded, "csv.field-delimiter", c)) {
        return null;
      }
    }
    String quote = option(options, "quote-character");
    if (quote != null && !appendChar(encoded, "csv.quote-character", quote.charAt(0))) {
      return null;
    }
    if ("true".equalsIgnoreCase(option(options, "disable-quote-character"))) {
      encoded.append("csv.disable-quote-character=true\n");
    }
    if (option(options, "escape-character") != null) {
      // Jackson's escape applies in unquoted fields too (parity-pinned: "esc\;aped" unescapes);
      // csv-core's escape is quoted-context only, so the option can't be reproduced — fall back.
      return null;
    }
    if ("true".equalsIgnoreCase(option(options, "allow-comments"))) {
      encoded.append("csv.allow-comments=true\n");
    }
    String nullLiteral = option(options, "null-literal");
    if (nullLiteral != null) {
      if (nullLiteral.contains("\n") || nullLiteral.contains("\r")) {
        return null;
      }
      encoded.append("csv.null-literal=").append(nullLiteral).append('\n');
    }
    return encoded.toString();
  }

  /**
   * Raw's two options the way {@code RawFormatFactory} reads them. {@code raw.charset} accepts any
   * name resolving to UTF-8 (the underlying bytes of {@code StringData} are UTF-8, so Flink's
   * decode is a passthrough then — exactly what the native decode does); any other valid charset
   * decodes through Java's charset machinery and stays on Flink. Endianness is case-insensitive
   * {@code big-endian}/{@code little-endian}; only the non-default value is plumbed through. An
   * invalid charset or endianness value falls back so Flink raises its own ValidationException.
   */
  private static String encodeRaw(Map<String, String> options) {
    String charset = option(options, "charset");
    if (charset != null && !isUtf8(charset)) {
      return null;
    }
    String endianness = option(options, "endianness");
    if (endianness == null || "big-endian".equalsIgnoreCase(endianness)) {
      return "";
    }
    return "little-endian".equalsIgnoreCase(endianness) ? "raw.endianness=little-endian\n" : null;
  }

  public static boolean isUtf8(String charsetName) {
    try {
      return StandardCharsets.UTF_8.equals(Charset.forName(charsetName));
    } catch (Exception e) {
      return false;
    }
  }

  static boolean appendChar(StringBuilder encoded, String key, char c) {
    if (c > 127 || c == '\n' || c == '\r') {
      return false;
    }
    encoded.append(key).append('=').append(c).append('\n');
    return true;
  }

  /**
   * {@code field-delimiter} the way {@code CsvFormatFactory} reads it — Java-unescaped, first char
   * ({@code '\t'} arrives as the two characters backslash-t). Handles the escape forms that render
   * a single character; null (fall back) for anything else rather than risking a mis-read
   * delimiter.
   */
  static Character unescapedDelimiter(String raw) {
    if (raw.length() == 1) {
      return raw.charAt(0);
    }
    if (raw.length() == 2 && raw.charAt(0) == '\\') {
      switch (raw.charAt(1)) {
        case 't':
          return '\t';
        case 'b':
          return '\b';
        case 'f':
          return '\f';
        case '\\':
          return '\\';
        case '\'':
          return '\'';
        case '"':
          return '"';
        default:
          return null;
      }
    }
    if (raw.length() == 6 && raw.startsWith("\\u")) {
      try {
        return (char) Integer.parseInt(raw.substring(2), 16);
      } catch (NumberFormatException e) {
        return null;
      }
    }
    return null;
  }
}
