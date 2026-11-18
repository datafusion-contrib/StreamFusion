package tech.streamfusion;

import static tech.streamfusion.compat.FlinkTestSources.fromData;

import java.util.ArrayList;
import java.util.List;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

final class TextTimeFunctionTestInputs {
  private TextTimeFunctionTestInputs() {}

  static TableEnvironment strings() {
    String[] values = {
      null,
      "",
      "a",
      "|a||",
      ".*x.*",
      "\u4e2d\ud83d\ude00\u4e2d",
      "a\u0000b",
      "/\"\\\b\f\n\r\t",
      "\"ok\"",
      "\"\\ud83d\\ude00\"",
      "\"\\ud800\"",
      "\"\\udc00\"",
      "\"bad\\q\"",
      "\"a\" \"b\"",
      " \"ok\" ",
      "null",
      "[]",
      "{}",
      "\"a\u0000b\"",
      "\"\\u0000\"",
      "\"\\u00e9\"",
      "\"\\u1f600\\ude00\"",
      "\"x\" garbage\"",
      "\u00e9\ud83d\ude00".repeat(4097)
    };
    List<Row> rows = new ArrayList<>();
    for (int i = 0; i < values.length; i++) {
      rows.add(Row.of(i, values[i]));
    }
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    StreamTableEnvironment tables = StreamTableEnvironment.create(env);
    tables.createTemporaryView(
        "inputs",
        fromData(
            env,
            Types.ROW_NAMED(new String[] {"id", "s"}, Types.INT, Types.STRING),
            rows.toArray(Row[]::new)),
        Schema.newBuilder().column("id", DataTypes.INT()).column("s", DataTypes.STRING()).build());
    return tables;
  }

  static TableEnvironment parameters() {
    String[] text = {
      null, "", "abc", "a\ud83d\ude00\u4e2db", "\u00e9e\u0301", "a|b||", " a  b ", "a\u0000b"
    };
    String[] pads = {null, "", "xy", "|", "\ud83d\ude00x", " "};
    Integer[] counts = {null, -7, -1, 0, 1, 2, 3, 7, 19};
    List<Row> rows = new ArrayList<>();
    for (String value : text) {
      for (Integer count : counts) {
        for (String pad : pads) {
          rows.add(Row.of(rows.size(), value, count, pad));
        }
      }
    }
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    StreamTableEnvironment tables = StreamTableEnvironment.create(env);
    tables.createTemporaryView(
        "inputs",
        fromData(
            env,
            Types.ROW_NAMED(
                new String[] {"id", "s", "n", "p"},
                Types.INT,
                Types.STRING,
                Types.INT,
                Types.STRING),
            rows.toArray(Row[]::new)),
        Schema.newBuilder()
            .column("id", DataTypes.INT())
            .column("s", DataTypes.STRING())
            .column("n", DataTypes.INT())
            .column("p", DataTypes.STRING())
            .build());
    return tables;
  }

  static TableEnvironment dates() {
    String[] text = {
      null,
      "",
      "1970",
      "1970-2",
      "1970-1-2",
      "1970-01-02 12:34:56",
      "2020-02-29",
      "1900-02-29",
      "2000-02-29",
      "2021-02-30",
      "0000-1-1",
      "9999-12-31",
      "10000-1-1",
      "-1-1-1",
      "+1970-1-1",
      " 1970-1-2 ",
      "1970- 1-2",
      "1970-1-2junk",
      "1970--2",
      "1970-1",
      "1970\t",
      "1970-1-2\t",
      "1970-1-2T00:00:00",
      "\uff11\uff19\uff17\uff10"
    };
    return textRows(text);
  }

  static TableEnvironment textRows(String... text) {
    List<Row> rows = new ArrayList<>();
    for (int i = 0; i < text.length; i++) {
      rows.add(Row.of(i, text[i]));
    }
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    StreamTableEnvironment tables = StreamTableEnvironment.create(env);
    tables.createTemporaryView(
        "inputs",
        fromData(
            env,
            Types.ROW_NAMED(new String[] {"id", "s"}, Types.INT, Types.STRING),
            rows.toArray(Row[]::new)),
        Schema.newBuilder().column("id", DataTypes.INT()).column("s", DataTypes.STRING()).build());
    return tables;
  }

  static TableEnvironment calendar() {
    return calendar(9);
  }

  static TableEnvironment calendar(int precision) {
    List<Row> rows = new ArrayList<>();
    java.time.LocalDate start = java.time.LocalDate.of(1968, 1, 1);
    for (int i = 0; i < 1462; i++) {
      java.time.LocalDate date = start.plusDays(i);
      rows.add(Row.of(i, date, date.atTime(23, 59, 59, 999999999)));
    }
    for (String value :
        new String[] {
          "1900-01-01",
          "2000-02-29",
          "2015-12-31",
          "2016-01-01",
          "2020-12-31",
          "2021-01-01",
          "2024-03-31"
        }) {
      java.time.LocalDate date = java.time.LocalDate.parse(value);
      rows.add(Row.of(rows.size(), date, date.atStartOfDay()));
    }
    rows.add(Row.of(rows.size(), null, null));
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    StreamTableEnvironment tables = StreamTableEnvironment.create(env);
    tables.createTemporaryView(
        "inputs",
        fromData(
            env,
            Types.ROW_NAMED(
                new String[] {"id", "d", "ts"}, Types.INT, Types.LOCAL_DATE, Types.LOCAL_DATE_TIME),
            rows.toArray(Row[]::new)),
        Schema.newBuilder()
            .column("id", DataTypes.INT())
            .column("d", DataTypes.DATE())
            .column("ts", DataTypes.TIMESTAMP(precision))
            .build());
    return tables;
  }

  static TableEnvironment bytes() {
    List<Row> rows = new ArrayList<>();
    rows.add(Row.of(0, null));
    rows.add(Row.of(1, new byte[0]));
    for (int i = 0; i < 256; i++) {
      rows.add(Row.of(rows.size(), new byte[] {(byte) i}));
      rows.add(Row.of(rows.size(), new byte[] {(byte) 0xed, (byte) i, (byte) 0x80}));
      rows.add(Row.of(rows.size(), new byte[] {(byte) 0xf0, (byte) i, (byte) 0x80}));
    }
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    StreamTableEnvironment tables = StreamTableEnvironment.create(env);
    tables.createTemporaryView(
        "inputs",
        fromData(
            env,
            Types.ROW_NAMED(new String[] {"id", "b"}, Types.INT, Types.PRIMITIVE_ARRAY(Types.BYTE)),
            rows.toArray(Row[]::new)),
        Schema.newBuilder().column("id", DataTypes.INT()).column("b", DataTypes.BYTES()).build());
    return tables;
  }
}
