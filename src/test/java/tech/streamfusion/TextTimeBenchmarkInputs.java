package tech.streamfusion;

import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/** Source fixtures shared by each independent text/time benchmark and its identity control. */
final class TextTimeBenchmarkInputs {
  private TextTimeBenchmarkInputs() {}

  static String baselineExpression(String input) {
    return switch (input) {
      case "tt_bytes", "tt_utf16", "tt_utf16be", "tt_utf16le" -> "b";
      case "tt_boolean" -> "b";
      case "tt_decimal", "tt_unix_time" -> "n";
      case "tt_decimal_array" -> "a";
      case "tt_timestamp" -> "ts";
      default -> "s";
    };
  }

  static String baselineType(String input) {
    return switch (input) {
      case "tt_bytes", "tt_utf16", "tt_utf16be", "tt_utf16le" -> "BYTES";
      case "tt_boolean" -> "BOOLEAN";
      case "tt_decimal" -> "DECIMAL(38,9)";
      case "tt_unix_time" -> "BIGINT";
      case "tt_decimal_array" -> "ARRAY<DECIMAL(38,9)>";
      case "tt_timestamp" -> "TIMESTAMP(9)";
      default -> "STRING";
    };
  }

  static TableEnvironment environment(
      String input, long rows, int bytes, boolean unicode, int nullEvery) {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    StreamTableEnvironment tables = StreamTableEnvironment.create(env);
    String[] text = {
      payload(unicode ? " |\u4e2daB\ud83d\ude00| " : " |abCd| efGh| ", bytes),
      payload(unicode ? " |\u00e9dE\ud83d\ude42| " : " |deFg| abCd| ", bytes)
    };
    if (input.equals("tt_unix_time")) {
      tables.getConfig().setLocalTimeZone(java.time.ZoneOffset.UTC);
      tables.createTemporaryView(
          "inputs",
          env.fromSequence(0, rows - 1)
              .map(
                  i ->
                      Row.of(
                          isNull(i, nullEvery) ? null : 1_700_000_000L + i % 86400, "yyyyMMddHHmm"))
              .returns(Types.ROW_NAMED(new String[] {"n", "p"}, Types.LONG, Types.STRING)));
    } else if (input.equals("tt_udf_decimal")) {
      String[] values = {"999.995", "-999.995", "1.235", "-1.235", "0", "9.99E+8"};
      tables.createTemporaryView("inputs", env.fromSequence(0, rows - 1)
          .map(i -> Row.of(isNull(i, nullEvery) ? null : values[(int) (i % values.length)]))
          .returns(Types.ROW_NAMED(new String[] {"s"}, Types.STRING)));
    } else if (input.equals("tt_decimal_array")) {
      java.math.BigDecimal[] values = {
        new java.math.BigDecimal("12345678901234567890.123456700"),
        null,
        new java.math.BigDecimal("-0.000000100")
      };
      tables.createTemporaryView("inputs", env.fromSequence(0, rows - 1)
          .map(i -> Row.of((Object) (isNull(i, nullEvery) ? null : values)))
          .returns(Types.ROW_NAMED(new String[] {"a"}, Types.OBJECT_ARRAY(Types.BIG_DEC))),
          Schema.newBuilder().column("a", DataTypes.ARRAY(DataTypes.DECIMAL(38, 9))).build());
    } else if (input.equals("tt_decimal")) {
      java.math.BigDecimal[] values = {
        new java.math.BigDecimal("12345678901234567890.123456700"),
        new java.math.BigDecimal("-0.000000100")
      };
      tables.createTemporaryView(
          "inputs",
          env.fromSequence(0, rows - 1)
              .map(i -> Row.of(isNull(i, nullEvery) ? null : values[(int) (i % 2)]))
              .returns(Types.ROW_NAMED(new String[] {"n"}, Types.BIG_DEC)),
          Schema.newBuilder().column("n", DataTypes.DECIMAL(38, 9)).build());
    } else if (input.equals("tt_json_object")) {
      tables.createTemporaryView(
          "inputs",
          env.fromSequence(0, rows - 1)
              .map(
                  i -> Row.of(
                      isNull(i, nullEvery) ? null : text[(int) (i % 2)],
                      isNull(i + 1, nullEvery) ? null : i,
                      isNull(i + 2, nullEvery) ? null : i % 2 == 0))
              .returns(
                  Types.ROW_NAMED(
                      new String[] {"s", "n", "b"}, Types.STRING, Types.LONG, Types.BOOLEAN)),
          Schema.newBuilder()
              .column("s", DataTypes.STRING())
              .column("n", DataTypes.BIGINT())
              .column("b", DataTypes.BOOLEAN())
              .build());
    } else if (input.equals("tt_boolean")) {
      tables.createTemporaryView(
          "inputs",
          env.fromSequence(0, rows - 1)
              .map(i -> Row.of(isNull(i, nullEvery) ? null : i % 2 == 0))
              .returns(Types.ROW_NAMED(new String[] {"b"}, Types.BOOLEAN)),
          Schema.newBuilder().column("b", DataTypes.BOOLEAN()).build());
    } else if (input.equals("tt_timestamp")) {
      LocalDateTime[] values = {
        LocalDateTime.of(1969, 12, 31, 23, 59, 59, 987654321),
        LocalDateTime.of(2000, 2, 29, 12, 34, 56, 123456789),
        LocalDateTime.of(2021, 1, 1, 0, 0)
      };
      tables.createTemporaryView(
          "inputs",
          env.fromSequence(0, rows - 1)
              .map(i -> Row.of(isNull(i, nullEvery) ? null : values[(int) (i % values.length)]))
              .returns(Types.ROW_NAMED(new String[] {"ts"}, Types.LOCAL_DATE_TIME)),
          Schema.newBuilder().column("ts", DataTypes.TIMESTAMP(9)).build());
    } else if (input.equals("tt_bytes") || input.startsWith("tt_utf16")) {
      java.nio.charset.Charset charset =
          switch (input) {
            case "tt_utf16" -> StandardCharsets.UTF_16;
            case "tt_utf16be" -> StandardCharsets.UTF_16BE;
            case "tt_utf16le" -> StandardCharsets.UTF_16LE;
            default -> StandardCharsets.UTF_8;
          };
      byte[][] values = {
        text[0].getBytes(charset), text[1].getBytes(charset)
      };
      tables.createTemporaryView(
          "inputs",
          env.fromSequence(0, rows - 1)
              .map(i -> Row.of(isNull(i, nullEvery) ? null : values[(int) (i % 2)]))
              .returns(Types.ROW_NAMED(new String[] {"b"}, Types.PRIMITIVE_ARRAY(Types.BYTE))),
          Schema.newBuilder().column("b", DataTypes.BYTES()).build());
    } else if (input.equals("tt_substring")) {
      tables.createTemporaryView(
          "inputs",
          env.fromSequence(0, rows - 1)
              .map(
                  i ->
                      Row.of(
                          isNull(i, nullEvery) ? null : text[(int) (i % 2)],
                          i % 2 == 0 ? 3 : -16,
                          (int) (i % 3) * 8))
              .returns(
                  Types.ROW_NAMED(
                      new String[] {"s", "n", "len"}, Types.STRING, Types.INT, Types.INT)),
          Schema.newBuilder()
              .column("s", DataTypes.STRING())
              .column("n", DataTypes.INT())
              .column("len", DataTypes.INT())
              .build());
    } else if (input.equals("tt_counted")) {
      tables.createTemporaryView(
          "inputs",
          env.fromSequence(0, rows - 1)
              .map(
                  i -> Row.of(isNull(i, nullEvery) ? null : text[(int) (i % 2)], (int) (i % 3) * 8))
              .returns(Types.ROW_NAMED(new String[] {"s", "n"}, Types.STRING, Types.INT)),
          Schema.newBuilder().column("s", DataTypes.STRING()).column("n", DataTypes.INT()).build());
    } else if (input.equals("tt_pad") || input.equals("tt_split")) {
      String[] pads =
          input.equals("tt_split")
              ? new String[] {"|", " "}
              : unicode
                  ? new String[] {"\u4e2d\ud83d\ude00", "\u00e9x"}
                  : new String[] {"xy", "ab"};
      tables.createTemporaryView(
          "inputs",
          env.fromSequence(0, rows - 1)
              .map(
                  i ->
                      Row.of(
                          isNull(i, nullEvery) ? null : text[(int) (i % 2)],
                          input.equals("tt_pad")
                              ? text[(int) (i % 2)].length() + 8 + (int) (i % 3)
                              : (int) (i % 3),
                          pads[(int) (i / 2 % 2)]))
              .returns(
                  Types.ROW_NAMED(
                      new String[] {"s", "n", "p"}, Types.STRING, Types.INT, Types.STRING)),
          Schema.newBuilder()
              .column("s", DataTypes.STRING())
              .column("n", DataTypes.INT())
              .column("p", DataTypes.STRING())
              .build());
    } else if (input.equals("tt_trim")) {
      tables.createTemporaryView(
          "inputs",
          env.fromSequence(0, rows - 1)
              .map(
                  i ->
                      Row.of(
                          isNull(i, nullEvery) ? null : text[(int) (i % 2)],
                          i % 3 == 0 ? " |ab" : " |de"))
              .returns(Types.ROW_NAMED(new String[] {"s", "p"}, Types.STRING, Types.STRING)),
          Schema.newBuilder()
              .column("s", DataTypes.STRING())
              .column("p", DataTypes.STRING())
              .build());
    } else {
      String quotePattern = unicode ? "\u4e2d\\n\\t\\\"\\\\" : "ab\\n\\t\\\"\\\\";
      String quoted =
          "\""
              + quotePattern.repeat(bytes / quotePattern.getBytes(StandardCharsets.UTF_8).length)
              + "\"";
      String[] values =
          switch (input) {
            case "tt_text" -> text;
            case "tt_json_predicate" ->
                new String[] {
                  "{\"padding\":\"" + text[0] + "\"}",
                  "[\"" + text[1] + "\"]",
                  "\"" + text[0] + "\"",
                  "{\"invalid\":\"" + text[1] + "\",}"
                };
            case "tt_quoted" -> new String[] {quoted, quoted};
            case "tt_json_boolean", "tt_json_integer", "tt_json_double" -> {
              String[] selected =
                  switch (input) {
                    case "tt_json_boolean" -> new String[] {"true", "false"};
                    case "tt_json_integer" -> new String[] {"123456789", "-234567890"};
                    default -> new String[] {"1.23456789", "-2.3456789e12"};
                  };
              yield new String[] {
                "{\"v\":" + selected[0] + ",\"padding\":\"" + text[0] + "\"}",
                "{\"v\":" + selected[1] + ",\"padding\":\"" + text[1] + "\"}"
              };
            }
            case "tt_ascii" ->
                new String[] {
                  payload(unicode ? "\u4e2da" : "ab", bytes),
                  payload(unicode ? "\u00e9b" : "cd", bytes)
                };
            case "tt_json_empty_member" ->
                new String[] {
                  "{\"\":\"Alice\",\"padding\":\"" + text[0] + "\"}",
                  "{\" \":\"space\",\"padding\":\"" + text[1] + "\"}"
                };
            case "tt_json_surrogate" -> new String[] {"\"\\uD800\"", "\"?\""};
            case "tt_json_escaped" ->
                new String[] {
                  "{\"a\\\\b\":{\"a\\nb\":\"Alice\"},\"padding\":\"" + text[0] + "\"}",
                  "{\"a\\\\b\":{\"a\\nb\":\"Bob\"},\"padding\":\"" + text[1] + "\"}"
                };
            case "tt_json_dot_member" ->
                new String[] {
                  "{\"order-id\":{\"123\":{\"a\\tb\":\"Alice\"}},\"padding\":\"" + text[0] + "\"}",
                  "{\"order-id\":{\"123\":{\"a\\tb\":\"Bob\"}},\"padding\":\"" + text[1] + "\"}"
                };
            case "tt_json_negative" -> {
              StringBuilder elements = new StringBuilder();
              for (int i = 0; i < 31; i++) elements.append("\"value").append(i).append("\",");
              yield new String[] {
                "{\"a\":[" + elements + "\"Alice\"],\"padding\":\"" + text[0] + "\"}",
                "{\"a\":[" + elements + "\"Bob\"],\"padding\":\"" + text[1] + "\"}"
              };
            }
            case "tt_json_member" ->
                new String[] {
                  "{\"\u7528\u6237\":{\"\u59d3.\u540d\":\""
                      + (unicode ? "\u4e2d\\n\ud83d\ude00" : "Alice")
                      + "\"},\"padding\":\""
                      + text[0]
                      + "\"}",
                  "{\"\u7528\u6237\":{},\"padding\":\"" + text[1] + "\"}"
                };
            case "tt_json" -> {
              int fields = Integer.getInteger("scalar.json.fields", 0);
              if (fields < 0) {
                throw new IllegalArgumentException("scalar.json.fields must be nonnegative");
              }
              StringBuilder members = new StringBuilder();
              for (int i = 0; i < fields; i++) {
                members.append(",\"field").append(i).append("\":\"value").append(i).append("\"");
              }
              yield new String[] {
                "{\"user\":{\"name\":\""
                    + (unicode ? "\u4e2d\\n\ud83d\ude00" : "Alice")
                    + "\",\"active\":true}"
                    + members
                    + ",\"padding\":\""
                    + text[0]
                    + "\"}",
                "{\"user\":{\"active\":false}" + members + ",\"padding\":\"" + text[1] + "\"}"
              };
            }
            case "tt_date_text" -> new String[] {"2000-02-29", "1969-12-31"};
            case "tt_timestamp_text" -> new String[] {"2000-02-29 12:34:56", "1969-12-31 23:59:59"};
            default -> throw new IllegalArgumentException("Unknown text/time input: " + input);
          };
      tables.createTemporaryView(
          "inputs",
          env.fromSequence(0, rows - 1)
              .map(i -> Row.of(isNull(i, nullEvery) ? null : values[(int) (i % values.length)]))
              .returns(Types.ROW_NAMED(new String[] {"s"}, Types.STRING)),
          Schema.newBuilder().column("s", DataTypes.STRING()).build());
    }
    return tables;
  }

  private static boolean isNull(long row, int nullEvery) {
    return nullEvery > 0 && row % nullEvery == 0;
  }

  private static String payload(String pattern, int bytes) {
    int size = pattern.getBytes(StandardCharsets.UTF_8).length;
    return pattern.repeat(bytes / size) + "x".repeat(bytes % size);
  }
}
