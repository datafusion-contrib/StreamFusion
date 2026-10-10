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

  static int fixedBinaryWidth() {
    int width = Integer.getInteger("scalar.binary.width", 16);
    if (width < 1) throw new IllegalArgumentException("scalar.binary.width must be positive");
    return width;
  }

  static String fixedBinaryLiteral() {
    return fixedBinaryLiteral(fixedBinaryWidth());
  }

  static String fixedBinaryLiteral(int width) {
    byte[] value = new byte[width];
    for (int i = 0; i < value.length; i++) value[i] = (byte) (i * 17);
    return "X'"
        + org.apache.flink.util.StringUtils.byteToHexString(value)
            .toUpperCase(java.util.Locale.ROOT)
        + "'";
  }

  static String fixedBinarySelection() {
    return "ELT(n,b," + fixedBinaryLiteral() + ")";
  }

  static String baselineExpression(String input) {
    switch (input) {
      case "tt_bytes":
      case "tt_fixed_bytes":
      case "tt_utf16":
      case "tt_utf16be":
      case "tt_utf16le":
        return "b";
      case "tt_boolean":
        return "b";
      case "tt_decimal":
      case "tt_decimal_scale":
      case "tt_unix_time":
      case "tt_double_bounded":
      case "tt_double_boundary":
      case "tt_double_ambiguous":
      case "tt_double_outside":
      case "tt_double_large":
        return "n";
      case "tt_decimal_array":
        return "a";
      case "tt_json_array":
        return "a";
      case "tt_timestamp":
      case "tt_timestamp_ltz":
        return "ts";
      default:
        return "s";
    }
  }

  static String baselineType(String input) {
    switch (input) {
      case "tt_bytes":
      case "tt_utf16":
      case "tt_utf16be":
      case "tt_utf16le":
        return "BYTES";
      case "tt_boolean":
        return "BOOLEAN";
      case "tt_fixed_bytes":
        return "BINARY(" + fixedBinaryWidth() + ")";
      case "tt_decimal":
      case "tt_decimal_scale":
        return "DECIMAL(38,9)";
      case "tt_unix_time":
        return "BIGINT";
      case "tt_double_bounded":
      case "tt_double_boundary":
      case "tt_double_ambiguous":
      case "tt_double_outside":
      case "tt_double_large":
        return "DOUBLE";
      case "tt_decimal_array":
        return "ARRAY<DECIMAL(38,9)>";
      case "tt_json_array":
        return "ARRAY<STRING>";
      case "tt_timestamp":
        return "TIMESTAMP(9)";
      case "tt_timestamp_ltz":
        return "TIMESTAMP_LTZ(9)";
      default:
        return "STRING";
    }
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
    if (input.equals("tt_charset")) {
      String[] charsets = {"UTF-8", "UTF-16LE", "windows-1252"};
      byte[][] encoded = new byte[charsets.length][];
      for (int i = 0; i < encoded.length; i++) {
        encoded[i] = text[0].getBytes(java.nio.charset.Charset.forName(charsets[i]));
      }
      tables.createTemporaryView("inputs", env.fromSequence(0, rows - 1)
          .map(i -> Row.of(isNull(i, nullEvery) ? null : text[0],
              isNull(i, nullEvery) ? null : encoded[(int) (i % encoded.length)],
              charsets[(int) (i % charsets.length)]))
          .returns(Types.ROW_NAMED(new String[] {"s", "b", "c"},
              Types.STRING, Types.PRIMITIVE_ARRAY(Types.BYTE), Types.STRING)));
    } else if (input.equals("tt_json_array")) {
      tables.createTemporaryView(
          "inputs",
          env.fromSequence(0, rows - 1)
              .map(i -> Row.of(
                  isNull(i, nullEvery) ? null : new String[] {text[(int) (i % 2)], null, "tail"},
                  "{\"items\":[1,2,3]}"))
              .returns(Types.ROW_NAMED(
                  new String[] {"a", "s"}, Types.OBJECT_ARRAY(Types.STRING), Types.STRING)));
    } else if (input.equals("tt_unix_time")) {
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
    } else if (input.equals("tt_double_bounded") || input.equals("tt_double_boundary")
        || input.equals("tt_double_ambiguous") || input.equals("tt_double_outside")
        || input.equals("tt_double_large")) {
      int nullScaleEvery = Integer.getInteger("scalar.scaleNullEvery", 0);
      tables.createTemporaryView(
          "inputs",
          env.fromSequence(0, rows - 1)
              .map(
                  i -> {
                    double magnitude;
                    switch (input) {
                      case "tt_double_boundary":
                        magnitude = 1000.5 + i % 1024;
                        break;
                      case "tt_double_ambiguous":
                        magnitude = 1000.1 + i % 1024;
                        break;
                      case "tt_double_outside":
                        magnitude = 0.46;
                        break;
                      case "tt_double_large":
                        magnitude = 1e12 + 0.12345 + i % 1024;
                        break;
                      default:
                        magnitude = 1000.12345 + i % 1024;
                        break;
                    }
                    Double value =
                        isNull(i, nullEvery) ? null : (i % 2 == 0 ? magnitude : -magnitude);
                    Integer scale =
                        nullScaleEvery > 0 && i % nullScaleEvery == 0
                            ? null
                            : (input.equals("tt_double_boundary")
                                    || input.equals("tt_double_ambiguous"))
                                ? 1
                                : (int) (i % 7) - 3;
                    return Row.of(value, scale);
                  })
              .returns(Types.ROW_NAMED(new String[] {"n", "s"}, Types.DOUBLE, Types.INT)));
    } else if (input.equals("tt_decimal_scale")) {
      java.math.BigDecimal[] values = {
        new java.math.BigDecimal("12345678901234567890.123456700"),
        new java.math.BigDecimal("-0.000000100")
      };
      Integer[] scales = {-3, 0, 2, 9, 12, null};
      tables.createTemporaryView(
          "inputs",
          env.fromSequence(0, rows - 1)
              .map(i -> Row.of(isNull(i, nullEvery) ? null : values[(int) (i % 2)],
                  scales[(int) ((i / 2) % scales.length)]))
              .returns(Types.ROW_NAMED(new String[] {"n", "s"}, Types.BIG_DEC, Types.INT)),
          Schema.newBuilder().column("n", DataTypes.DECIMAL(38, 9))
              .column("s", DataTypes.INT()).build());
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
    } else if (input.equals("tt_timestamp_ltz")) {
      tables.createTemporaryView(
          "inputs",
          env.fromSequence(0, rows - 1)
              .map(i -> Row.of(isNull(i, nullEvery) ? null
                  : java.time.Instant.ofEpochSecond(i - rows / 2, 123456789)))
              .returns(Types.ROW_NAMED(new String[] {"ts"}, Types.INSTANT)),
          Schema.newBuilder().column("ts", DataTypes.TIMESTAMP_LTZ(9)).build());
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
    } else if (input.equals("tt_fixed_bytes")) {
      int width = fixedBinaryWidth();
      byte[][] values = {new byte[width], new byte[width]};
      for (int i = 0; i < width; i++) {
        values[0][i] = (byte) (i * 17);
        values[1][i] = (byte) (255 - i * 17);
      }
      tables.createTemporaryView(
          "inputs",
          env.fromSequence(0, rows - 1)
              .map(i -> Row.of(isNull(i, nullEvery) ? null : values[(int) (i % 2)], (int) (i % 4)))
              .returns(
                  Types.ROW_NAMED(
                      new String[] {"b", "n"}, Types.PRIMITIVE_ARRAY(Types.BYTE), Types.INT)),
          Schema.newBuilder()
              .column("b", DataTypes.BINARY(width))
              .column("n", DataTypes.INT())
              .build());
    } else if (input.equals("tt_bytes") || input.startsWith("tt_utf16")) {
      java.nio.charset.Charset charset;
      switch (input) {
        case "tt_utf16":
          charset = StandardCharsets.UTF_16;
          break;
        case "tt_utf16be":
          charset = StandardCharsets.UTF_16BE;
          break;
        case "tt_utf16le":
          charset = StandardCharsets.UTF_16LE;
          break;
        default:
          charset = StandardCharsets.UTF_8;
          break;
      }
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
      String[] values;
      switch (input) {
        case "tt_text":
          values = text;
          break;
        case "tt_json_predicate":
          values =
              new String[] {
                "{\"padding\":\"" + text[0] + "\"}",
                "[\"" + text[1] + "\"]",
                "\"" + text[0] + "\"",
                "{\"invalid\":\"" + text[1] + "\",}"
              };
          break;
        case "tt_quoted":
          values = new String[] {quoted, quoted};
          break;
        case "tt_json_boolean":
        case "tt_json_integer":
        case "tt_json_double":
          {
            {
              String[] selected;
              switch (input) {
                case "tt_json_boolean":
                  selected = new String[] {"true", "false"};
                  break;
                case "tt_json_integer":
                  selected = new String[] {"123456789", "-234567890"};
                  break;
                default:
                  selected = new String[] {"1.23456789", "-2.3456789e12"};
                  break;
              }
              values =
                  new String[] {
                    "{\"v\":" + selected[0] + ",\"padding\":\"" + text[0] + "\"}",
                    "{\"v\":" + selected[1] + ",\"padding\":\"" + text[1] + "\"}"
                  };
            }
            break;
          }
        case "tt_ascii":
          values =
              new String[] {
                payload(unicode ? "\u4e2da" : "ab", bytes),
                payload(unicode ? "\u00e9b" : "cd", bytes)
              };
          break;
        case "tt_json_empty_member":
          values =
              new String[] {
                "{\"\":\"Alice\",\"padding\":\"" + text[0] + "\"}",
                "{\" \":\"space\",\"padding\":\"" + text[1] + "\"}"
              };
          break;
        case "tt_json_surrogate":
          values = new String[] {"\"\\uD800\"", "\"?\""};
          break;
        case "tt_json_escaped":
          values =
              new String[] {
                "{\"a\\\\b\":{\"a\\nb\":\"Alice\"},\"padding\":\"" + text[0] + "\"}",
                "{\"a\\\\b\":{\"a\\nb\":\"Bob\"},\"padding\":\"" + text[1] + "\"}"
              };
          break;
        case "tt_json_dot_member":
          values =
              new String[] {
                "{\"order-id\":{\"123\":{\"a\\tb\":\"Alice\"}},\"padding\":\"" + text[0] + "\"}",
                "{\"order-id\":{\"123\":{\"a\\tb\":\"Bob\"}},\"padding\":\"" + text[1] + "\"}"
              };
          break;
        case "tt_json_negative":
          {
            {
              StringBuilder elements = new StringBuilder();
              for (int i = 0; i < 31; i++) elements.append("\"value").append(i).append("\",");
              values =
                  new String[] {
                    "{\"a\":[" + elements + "\"Alice\"],\"padding\":\"" + text[0] + "\"}",
                    "{\"a\":[" + elements + "\"Bob\"],\"padding\":\"" + text[1] + "\"}"
                  };
            }
            break;
          }
        case "tt_json_member":
          values =
              new String[] {
                "{\"\u7528\u6237\":{\"\u59d3.\u540d\":\""
                    + (unicode ? "\u4e2d\\n\ud83d\ude00" : "Alice")
                    + "\"},\"padding\":\""
                    + text[0]
                    + "\"}",
                "{\"\u7528\u6237\":{},\"padding\":\"" + text[1] + "\"}"
              };
          break;
        case "tt_json":
          {
            {
              int fields = Integer.getInteger("scalar.json.fields", 0);
              if (fields < 0) {
                throw new IllegalArgumentException("scalar.json.fields must be nonnegative");
              }
              StringBuilder members = new StringBuilder();
              for (int i = 0; i < fields; i++) {
                members.append(",\"field").append(i).append("\":\"value").append(i).append("\"");
              }
              values =
                  new String[] {
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
            break;
          }
        case "tt_date_noncanonical":
          values = new String[] {"2000-2-29", "1969-1-2"};
          break;
        case "tt_time_noncanonical":
          values = new String[] {"12:34:56.1", "23:59:59.12"};
          break;
        case "tt_timestamp_noncanonical":
          values = new String[] {"2024-02-30 00:00:00", "2024-02-29 24:00:00"};
          break;
        case "tt_date_text":
          values = new String[] {"2000-02-29", "1969-12-31"};
          break;
        case "tt_time_text":
          values = new String[] {"12:34:56.789", "23:59:59.001"};
          break;
        case "tt_timestamp_text":
          values = new String[] {"2000-02-29 12:34:56", "1969-12-31 23:59:59"};
          break;
        default:
          throw new IllegalArgumentException("Unknown text/time input: " + input);
      }
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
