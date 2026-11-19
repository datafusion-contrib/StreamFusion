package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static tech.streamfusion.compat.FlinkTestSources.fromData;

import java.util.List;
import java.util.stream.IntStream;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.planner.NativePlanner;

class FlinkIntegerStringCastSqlHarnessTest {
  @ParameterizedTest
  @ValueSource(strings = {"TINYINT", "SMALLINT", "INT", "BIGINT"})
  void validValuesMatchAcrossBatchesAndWidths(String type) throws Exception {
    String[] values = {"  +00123.99  ", "-128.99", ".", "+.", "-.9", "0", "-0", null};
    String[] repeated = IntStream.range(0, 5003).mapToObj(i -> values[i % values.length])
        .toArray(String[]::new);
    NativeParity.assertParity(() -> input(false, DataTypes.STRING(), repeated),
        "SELECT id, CAST(s AS " + type + "), TRY_CAST(s AS " + type + ") FROM src");
    NativeParity.assertParity(() -> input(false, DataTypes.CHAR(16), values),
        "SELECT id FROM src WHERE CAST(s AS " + type + ") < 1");
  }

  @ParameterizedTest
  @ValueSource(strings = {"TINYINT", "SMALLINT", "INT", "BIGINT"})
  void tryCastAndLegacyHandleBoundariesMalformedTextAndNulls(String type) throws Exception {
    String[] values = {
      "-128",
      "127",
      "128",
      "-129",
      "-32768",
      "32767",
      "32768",
      "-32769",
      "-2147483648.9",
      "2147483647.9",
      "2147483648",
      "-2147483649",
      "-9223372036854775808.9",
      "9223372036854775807.9",
      "9223372036854775808",
      "-9223372036854775809",
      "",
      " ",
      "+",
      "-",
      "1e2",
      "1.2.3",
      "1.2x",
      "\t1",
      "1\n",
      "\u00a01",
      "1\u2003",
      "\u0661",
      "\uff11",
      "1\u0000",
      " 42 ",
      "1 2",
      null
    };
    NativeParity.assertParity(() -> input(false, DataTypes.STRING(), values),
        "SELECT id, TRY_CAST(s AS " + type + ") FROM src");
    NativeParity.assertParity(() -> input(true, DataTypes.STRING(), values),
        "SELECT id, CAST(s AS " + type + ") FROM src");
  }

  @ParameterizedTest
  @ValueSource(strings = {"", "+", "1e2", "\t1", "1.2x", "\u0661", "2147483648", "-2147483649"})
  void malformedOrdinaryCastFailsInBothEngines(String value) {
    for (boolean nativeRun : new boolean[] {false, true}) {
      TableEnvironment table = input(false, DataTypes.STRING(), value);
      var scan = nativeRun ? NativePlanner.install(table) : null;
      Exception failure = assertThrows(Exception.class, () -> collect(table,
          "SELECT CAST(s AS INT) FROM src"));
      StringBuilder messages = new StringBuilder();
      boolean typedFailure = false;
      for (Throwable cause = failure; cause != null; cause = cause.getCause()) {
        messages.append(cause.getMessage());
        typedFailure |= cause instanceof NumberFormatException;
      }
      assertTrue(typedFailure, messages.toString());
      assertTrue(messages.toString().contains("For input string:"), messages.toString());
      if (scan != null) assertTrue(scan.substitutions() > 0, scan::explainSummary);
    }
  }

  @Test
  void nonNullableInputAndCaseRetainHostBehavior() throws Exception {
    NativeParity.assertParity(
        () -> input(false, DataTypes.VARCHAR(12).notNull(), "1", "-2147483648"),
        "SELECT id, CAST(s AS INT), CAST(CAST(s AS INT) AS STRING) FROM src");
    NativeParity.assertParity(() -> input(false, DataTypes.STRING(), "skip", "42", null),
        "SELECT id, CASE WHEN s = 'skip' THEN 0 ELSE CAST(s AS INT) END FROM src");
    NativeParity.assertParity(() -> input(false, DataTypes.STRING(), "42", "invalid", null),
        "SELECT id FROM src WHERE TRY_CAST(s AS INT) > 0");
  }

  @Test
  void fallibleConjunctionsKeepHostShortCircuiting() throws Exception {
    for (String predicate :
        List.of("s = 'skip' OR CAST(s AS INT) > 0", "s <> 'skip' AND CAST(s AS INT) > 0")) {
      NativeParity.assertFallbackReasonContains(
          () -> input(false, DataTypes.STRING(), "skip", "42", "-1"),
          "SELECT id, " + predicate + " FROM src", "short-circuit");
      NativeParity.assertParity(() -> input(true, DataTypes.STRING(), "skip", "42", "-1"),
          "SELECT id, " + predicate + " FROM src");
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void integerFormattingAndLengthsMatchHost(boolean legacy) throws Exception {
    NativeParity.assertParity(() -> numbers(legacy),
        "SELECT CAST(b AS STRING), CAST(s AS STRING), CAST(i AS STRING), CAST(l AS STRING), "
            + "CAST(i AS VARCHAR(1)), CAST(i AS VARCHAR(5)), CAST(l AS CHAR(22)), "
            + "CAST(l AS CHAR(3)), TRY_CAST(i AS VARCHAR(4)) FROM nums");
    NativeParity.assertParity(() -> numbers(legacy),
        "SELECT i FROM nums WHERE CAST(i AS STRING) = '0'");
  }

  @Test
  void legacyFailureOnNonNullableInputRetainsSinkEnforcement() throws Exception {
    for (String enforcement : List.of("ERROR", "DROP")) {
      for (boolean nativeRun : new boolean[] {false, true}) {
        TableEnvironment table = input(true, DataTypes.STRING().notNull(), "invalid");
        table
            .getConfig()
            .getConfiguration()
            .setString("table.exec.sink.not-null-enforcer", enforcement);
        var scan = nativeRun ? NativePlanner.install(table) : null;
        if (enforcement.equals("ERROR")) {
          assertThrows(Exception.class, () -> collect(table, "SELECT CAST(s AS INT) FROM src"));
        } else {
          assertEquals(List.of(), collect(table, "SELECT CAST(s AS INT) FROM src"));
        }
        if (scan != null) assertTrue(scan.substitutions() > 0, scan::explainSummary);
      }
    }
  }

  private static List<Row> collect(TableEnvironment table, String sql) throws Exception {
    try (var rows = table.executeSql(sql).collect()) {
      var result = new java.util.ArrayList<Row>();
      rows.forEachRemaining(result::add);
      return result;
    }
  }

  private static TableEnvironment numbers(boolean legacy) {
    var env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    var table = StreamTableEnvironment.create(env);
    table
        .getConfig()
        .getConfiguration()
        .setString("table.exec.legacy-cast-behaviour", legacy ? "ENABLED" : "DISABLED");
    table.createTemporaryView(
        "nums",
        fromData(
            env,
            Types.ROW_NAMED(
                new String[] {"b", "s", "i", "l"}, Types.BYTE, Types.SHORT, Types.INT, Types.LONG),
            Row.of(Byte.MIN_VALUE, Short.MIN_VALUE, Integer.MIN_VALUE, Long.MIN_VALUE),
            Row.of(Byte.MAX_VALUE, Short.MAX_VALUE, Integer.MAX_VALUE, Long.MAX_VALUE),
            Row.of((byte) 0, (short) 0, 0, 0L),
            Row.of(null, null, null, null)));
    return table;
  }

  private static TableEnvironment input(boolean legacy, DataType type, String... values) {
    var env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    var table = StreamTableEnvironment.create(env);
    table
        .getConfig()
        .getConfiguration()
        .setString("table.exec.legacy-cast-behaviour", legacy ? "ENABLED" : "DISABLED");
    Row[] rows = IntStream.range(0, values.length).mapToObj(i -> Row.of(i, values[i])).toArray(Row[]::new);
    table.createTemporaryView(
        "src",
        fromData(env, Types.ROW_NAMED(new String[] {"id", "s"}, Types.INT, Types.STRING), rows),
        Schema.newBuilder().column("id", DataTypes.INT().notNull()).column("s", type).build());
    return table;
  }
}
