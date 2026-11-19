package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.planner.NativePlanner;

class FlinkJsonStringIdentitySqlHarnessTest {
  @ParameterizedTest
  @ValueSource(strings = {"JSON_VALUE(s, '$')", "JSON_UNQUOTE(s)"})
  void consumersKeepSurrogatesDistinctFromQuestionMarks(String value) throws Exception {
    tech.streamfusion.compat.FlinkTestCapabilities.requireSqlFunction("JSON_QUOTE");
    tech.streamfusion.compat.FlinkTestCapabilities.requireJsonFunctions(value);
    NativeParity.assertParity(
        () -> environment(5003),
        "SELECT id, "
            + value
            + " = '?', "
            + value
            + " <> '?', "
            + value
            + " LIKE '?', "
            + "CASE WHEN "
            + value
            + " = '?' THEN 1 ELSE 0 END, "
            + "CHAR_LENGTH("
            + value
            + "), JSON_QUOTE("
            + value
            + "), "
            + "JSON_STRING("
            + value
            + "), utf16_unit("
            + value
            + ") FROM inputs");
  }

  @ParameterizedTest
  @ValueSource(strings = {"JSON_VALUE(s, '$')", "JSON_UNQUOTE(s)"})
  void filtersKeepOnlyTheActualQuestionMark(String value) throws Exception {
    tech.streamfusion.compat.FlinkTestCapabilities.requireJsonFunctions(value);
    String sql = "SELECT id FROM inputs WHERE " + value + " = '?'";
    for (boolean nativeEnabled : new boolean[] {false, true}) {
      TableEnvironment tables = environment(4);
      var scan = nativeEnabled ? NativePlanner.install(tables) : null;
      List<Integer> ids = new ArrayList<>();
      try (var rows = tables.executeSql(sql).collect()) {
        while (rows.hasNext()) ids.add((Integer) rows.next().getField(0));
      }
      assertEquals(List.of(2), ids);
      if (scan != null) assertTrue(scan.substitutions() > 0, scan.fallbackReasons().toString());
    }
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "JSON_UNQUOTE(JSON_QUOTE(JSON_VALUE(s, '$'))) = '?'",
        "CONCAT(JSON_VALUE(s, '$'), 'x') = '?x'",
        "COALESCE(JSON_VALUE(s, '$'), 'missing') = '?'",
        "LOWER(JSON_VALUE(s, '$')) = '?'",
        "JSON_VALUE(s, '$') = JSON_UNQUOTE(s)",
        "CASE WHEN JSON_VALUE(s, '$') LIKE '?' THEN JSON_VALUE(s, '$') ELSE 'other' END",
        "JSON_VALUE(s, '$') IS NULL"
      })
  void nestedConsumersRemainInOneGeneratedExpression(String expression) throws Exception {
    tech.streamfusion.compat.FlinkTestCapabilities.requireJsonFunctions(expression);
    NativeParity.assertParity(() -> environment(5003), "SELECT id, " + expression + " FROM inputs");
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "SELECT JSON_VALUE(s, '$'), COUNT(*) FROM inputs GROUP BY JSON_VALUE(s, '$')",
        "SELECT COUNT(DISTINCT JSON_UNQUOTE(s)) FROM inputs",
        "SELECT MIN(JSON_VALUE(s, '$')) FROM inputs",
        "SELECT id, JSON_VALUE(s, '$') AS j FROM inputs ORDER BY j LIMIT 5",
        "SELECT a.id FROM (SELECT id, JSON_UNQUOTE(s) AS j FROM inputs) a "
            + "JOIN inputs b ON a.j = b.s",
        "SELECT LOWER(JSON_VALUE(s, '$')), COUNT(*) FROM inputs "
            + "GROUP BY LOWER(JSON_VALUE(s, '$'))"
      })
  void stringResultsCrossingOperatorsFallBackExplicitly(String sql) throws Exception {
    tech.streamfusion.compat.FlinkTestCapabilities.requireJsonFunctions(sql);
    NativeParity.assertFallbackReasonContains(
        () -> environment(32), sql, "JSON string identity requires a final projection");
  }

  @Test
  void scalarResultsCanStillFeedNativeAggregation() throws Exception {
    NativeParity.assertChangelogParity(
        () -> environment(5003),
        "SELECT CASE WHEN JSON_VALUE(s, '$') = '?' THEN 1 ELSE 0 END AS k, COUNT(*) "
            + "FROM inputs GROUP BY CASE WHEN JSON_VALUE(s, '$') = '?' THEN 1 ELSE 0 END");
  }

  @Test
  void fusedUnquoteConsumerPreservesTheHostException() {
    tech.streamfusion.compat.FlinkTestCapabilities.requireSqlFunction("JSON_UNQUOTE");
    NativeFailureParity.run(
            () -> TextTimeFunctionTestInputs.textRows("\"a\" \\u1\""),
            "SELECT JSON_UNQUOTE(s) = '?' FROM inputs")
        .assertFailure(
            StringIndexOutOfBoundsException.class,
            "",
            NativeFailureParity.Phase.ROW_EVALUATION,
            NativeFailureParity.Route.NATIVE);
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "JSON_VALUE(s, CASE WHEN MOD(id, 2) = 0 THEN '$[-1]' ELSE '$.a' END) = '?'",
        "JSON_VALUE(s, '$.*' DEFAULT 'empty' ON EMPTY DEFAULT 'error' ON ERROR) = 'error'",
        "JSON_VALUE(s, '$[-1]' DEFAULT '?' ON EMPTY DEFAULT '?' ON ERROR) = '?'"
      })
  void fusedConsumersUseFlinksSelectorAndPolicyRules(String expression) throws Exception {
    NativeParity.assertParity(
        () ->
            TextTimeFunctionTestInputs.textRows(
                "[\"first\",\"\\uD800\"]",
                "{\"a\":\"\\uDC00\"}",
                "[\"first\",\"?\"]",
                "{\"a\":\"?\"}",
                "[]",
                "{}",
                "invalid",
                null),
        "SELECT id, " + expression + " FROM inputs");
  }

  @ParameterizedTest
  @ValueSource(strings = {"\\uD800", "\\uDC00", "\\uD83D\\uDE00"})
  void constantFoldedJsonResultsKeepTheirIdentity(String escaped) throws Exception {
    tech.streamfusion.compat.FlinkTestCapabilities.requireSqlFunction("JSON_UNQUOTE");
    NativeParity.assertParity(
        () -> TextTimeFunctionTestInputs.textRows("?", "😀", "", null),
        "SELECT id, JSON_VALUE('\""
            + escaped
            + "\"', '$') = s, "
            + "JSON_UNQUOTE('\""
            + escaped
            + "\"') LIKE s FROM inputs");
  }

  private static TableEnvironment environment(int count) {
    String[] values = {
      "\"\\uD800\"",
      "\"\\uDC00\"",
      "\"?\"",
      "\"\\uD83D\\uDE00\"",
      null,
      "\"\\uD800x\\uDC00\"",
      "\"?x?\"",
      "null",
      "\"\"",
      "\"\\ufffd\""
    };
    String[] rows = new String[count];
    for (int i = 0; i < count; i++) rows[i] = values[i % values.length];
    TableEnvironment tables = TextTimeFunctionTestInputs.textRows(rows);
    tables.createTemporarySystemFunction("utf16_unit", new Utf16Unit());
    return tables;
  }

  public static final class Utf16Unit extends ScalarFunction {
    public Integer eval(String value) {
      return value == null || value.isEmpty() ? null : (int) value.charAt(0);
    }
  }
}
