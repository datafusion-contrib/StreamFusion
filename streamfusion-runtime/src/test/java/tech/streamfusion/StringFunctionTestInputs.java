package tech.streamfusion;

import static tech.streamfusion.compat.FlinkTestSources.fromData;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

final class StringFunctionTestInputs {
  private StringFunctionTestInputs() {}

  static TableEnvironment search() {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
    tEnv.createTemporaryView(
        "searches",
        fromData(
            env,
            Types.ROW_NAMED(
                new String[] {"id", "s", "needle", "start_pos", "binary_value"},
                Types.INT,
                Types.STRING,
                Types.STRING,
                Types.INT,
                Types.PRIMITIVE_ARRAY(Types.BYTE)),
            Row.of(0, "abcabc", "bc", 3, new byte[] {(byte) 0xff, 0}),
            Row.of(1, "", "", -1, new byte[0]),
            Row.of(2, "abc", "", Integer.MAX_VALUE, null),
            Row.of(3, null, "a", 1, new byte[] {1}),
            Row.of(4, "abc", null, 1, null),
            Row.of(5, "abc", "abc", null, new byte[] {0}),
            Row.of(6, "a\ud83d\ude00\u4e2db\ud83d\ude00", "\ud83d\ude00", 3, new byte[0]),
            Row.of(7, "\u00e9e\u0301abc", "abc", 2, new byte[0]),
            Row.of(8, "a\u0000b\u0000", "\u0000", 3, new byte[] {0}),
            Row.of(9, "%_abc_%", "_%", 1, new byte[0]),
            Row.of(10, "\\abc\\", "\\", 0, new byte[0]),
            Row.of(11, "abc", "abcabc", Integer.MIN_VALUE, new byte[0]),
            Row.of(12, "abcabc", "bc", Integer.MIN_VALUE, new byte[0]),
            Row.of(13, "\u00e9".repeat(4097) + "abc", "abc", 4097, new byte[0])),
        Schema.newBuilder()
            .column("id", DataTypes.INT())
            .column("s", DataTypes.STRING())
            .column("needle", DataTypes.STRING())
            .column("start_pos", DataTypes.INT())
            .column("binary_value", DataTypes.BYTES())
            .build());
    return tEnv;
  }

  static TableEnvironment encodings() {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
    long[] numbers = {
      0,
      1,
      -1,
      Long.MIN_VALUE,
      Long.MAX_VALUE,
      Integer.MIN_VALUE,
      Integer.MAX_VALUE,
      Short.MIN_VALUE,
      Short.MAX_VALUE,
      Byte.MIN_VALUE,
      Byte.MAX_VALUE,
      255,
      256,
      4294967297L,
      -4294967297L,
      17,
      31,
      32
    };
    String[] strings = {
      "",
      "a",
      "ab",
      "abc",
      "abcd",
      "\u4e2d\ud83d\ude00",
      "\u00e9e\u0301",
      "a\u0000b",
      "line\r\nnext",
      " ",
      "\t",
      "%_\\",
      "x".repeat(57),
      "x".repeat(58),
      "\u4e2d".repeat(4097),
      null,
      "abc ",
      "0123456789"
    };
    String[] hexStrings = {
      "",
      "A",
      "AB",
      "ABC",
      "fF00",
      "12345",
      "G",
      "0G",
      "G12",
      " 12",
      "12\n",
      "0x12",
      "\uff11\uff12",
      "\u0000",
      "aF".repeat(4097),
      null,
      "f",
      "0123456789aBcDeF"
    };
    List<Row> rows = new ArrayList<>();
    for (int id = 0; id < numbers.length; id++) {
      long value = numbers[id];
      boolean isNull = id == 15;
      rows.add(
          Row.of(
              id,
              strings[id],
              hexStrings[id],
              isNull ? null : value,
              isNull ? null : (int) value,
              isNull ? null : (short) value,
              isNull ? null : (byte) value));
    }
    tEnv.createTemporaryView(
        "encodings",
        fromData(
            env,
            rows,
            Types.ROW_NAMED(
                new String[] {"id", "s", "hex_text", "n", "i", "sh", "t"},
                Types.INT,
                Types.STRING,
                Types.STRING,
                Types.LONG,
                Types.INT,
                Types.SHORT,
                Types.BYTE)),
        Schema.newBuilder()
            .column("id", DataTypes.INT())
            .column("s", DataTypes.STRING())
            .column("hex_text", DataTypes.STRING())
            .column("n", DataTypes.BIGINT())
            .column("i", DataTypes.INT())
            .column("sh", DataTypes.SMALLINT())
            .column("t", DataTypes.TINYINT())
            .build());
    return tEnv;
  }

  static TableEnvironment text() {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    StreamTableEnvironment tables = StreamTableEnvironment.create(env);
    String[] strings = {
      "",
      "aBC dEF",
      "a_B-C.9ABC",
      "\u00e9aBC\u4e2dDEF",
      "a\ud83d\ude00BC",
      "e\u0301ABC",
      "a\u0000b",
      " abba ",
      "\tab\r\n",
      "~*+% -_.",
      "%",
      "%GG",
      "%A",
      "%C3%28",
      "%ED%A0%80",
      "%F0%9F%98%80",
      "%00+%2B",
      "abc".repeat(4097),
      null,
      "abba"
    };
    String[] from = {
      "",
      "ab",
      "aab",
      "\u00e9\u4e2d",
      "\ud83d\ude00a",
      "e\u0301",
      "\u0000",
      " ab",
      "\t\n",
      null,
      "QUERY",
      "HOST",
      "PATH",
      "REF",
      "PROTOCOL",
      "FILE",
      "AUTHORITY",
      "USERINFO",
      null,
      "ab"
    };
    String[] to = {
      "",
      "x",
      "123",
      "\ud83d\ude00x",
      "xy",
      "X",
      "Z",
      "z",
      "",
      "a",
      "a.b",
      "a",
      "missing",
      "[",
      "",
      "a",
      "a",
      "a",
      null,
      null
    };
    long[] positions = {
      0,
      1,
      2,
      3,
      3,
      -1,
      1,
      2,
      1,
      2,
      Integer.MIN_VALUE,
      Integer.MAX_VALUE,
      Long.MIN_VALUE,
      Long.MAX_VALUE,
      4294967297L,
      1,
      2,
      3,
      1,
      2
    };
    List<Row> rows = new ArrayList<>();
    for (int id = 0; id < strings.length; id++) {
      Long n =
          id == 18 ? null : id == 0 ? Long.MIN_VALUE : id == 1 ? Long.MAX_VALUE : (long) (id - 5);
      Integer i =
          id == 18 ? null : id == 0 ? Integer.MIN_VALUE : id == 1 ? Integer.MAX_VALUE : id % 6 - 1;
      String url =
          id == 18
              ? null
              : id % 4 == 0
                  ? "invalid"
                  : id % 4 == 1
                      ? "file:/tmp/a?x=1"
                      : "https://user:pass@example.com:8080/a%20b?a=1&a=2&a.b=z#anchor";
      rows.add(
          Row.of(
              id,
              strings[id],
              from[id],
              to[id],
              n,
              i,
              id == 18 ? null : positions[id],
              id == 18 ? null : id % 2 == 0,
              id == 18 ? null : new BigDecimal(id + ".125"),
              url));
    }
    tables.createTemporaryView(
        "texts",
        fromData(
            env,
            rows,
            Types.ROW_NAMED(
                new String[] {"id", "s", "f", "t", "n", "i", "p", "b", "d", "u"},
                Types.INT,
                Types.STRING,
                Types.STRING,
                Types.STRING,
                Types.LONG,
                Types.INT,
                Types.LONG,
                Types.BOOLEAN,
                Types.BIG_DEC,
                Types.STRING)),
        Schema.newBuilder()
            .column("id", DataTypes.INT())
            .column("s", DataTypes.STRING())
            .column("f", DataTypes.STRING())
            .column("t", DataTypes.STRING())
            .column("n", DataTypes.BIGINT())
            .column("i", DataTypes.INT())
            .column("p", DataTypes.BIGINT())
            .column("b", DataTypes.BOOLEAN())
            .column("d", DataTypes.DECIMAL(20, 3))
            .column("u", DataTypes.STRING())
            .build());
    return tables;
  }

  static TableEnvironment urlBoundaries() {
    List<Row> rows = new ArrayList<>();
    String[] hex = new String[256];
    for (int i = 0; i < hex.length; i++) {
      hex[i] = String.format(java.util.Locale.ROOT, "%%%02X", i);
    }
    for (int a = 0; a < 256; a++) {
      for (int b = 0; b < 256; b++) {
        rows.add(Row.of(rows.size(), hex[a] + hex[b]));
      }
    }
    int[] boundaries = {0, 0x28, 0x7f, 0x80, 0x8f, 0x90, 0x9f, 0xa0, 0xbf, 0xc0, 0xff};
    for (int a = 0xe0; a <= 0xf7; a++) {
      for (int b : boundaries) {
        for (int c : boundaries) {
          String prefix = hex[a] + hex[b] + hex[c];
          rows.add(Row.of(rows.size(), prefix));
          for (int d : boundaries) {
            rows.add(Row.of(rows.size(), prefix + hex[d]));
          }
        }
      }
    }
    for (char c = 0; c < Character.MAX_VALUE; c++) {
      if (Character.digit(c, 16) >= 0) {
        rows.add(Row.of(rows.size(), "%0" + c));
        rows.add(Row.of(rows.size(), "%" + c + "0"));
      }
    }
    for (String value :
        new String[] {
          "%+A",
          "%-0",
          "%-1",
          "%++",
          "%\uff11\uff12",
          "%+\uff26",
          "%\ud835\udfd8",
          "%C3x%A9",
          "%C3+%A9",
          "%E2%82%",
          "%E2%82%G0",
          "%F0%9F%98%80"
        }) {
      rows.add(Row.of(rows.size(), value));
    }
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    StreamTableEnvironment tables = StreamTableEnvironment.create(env);
    tables.createTemporaryView(
        "urls",
        fromData(env, rows, Types.ROW_NAMED(new String[] {"id", "s"}, Types.INT, Types.STRING)),
        Schema.newBuilder().column("id", DataTypes.INT()).column("s", DataTypes.STRING()).build());
    return tables;
  }
}
