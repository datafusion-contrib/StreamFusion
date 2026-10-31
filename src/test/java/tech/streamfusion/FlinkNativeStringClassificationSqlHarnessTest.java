package tech.streamfusion;

import static org.apache.flink.table.api.DataTypes.*;
import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.List;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.runtime.functions.SqlFunctionUtils;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.planner.NativePlanner;

class FlinkNativeStringClassificationSqlHarnessTest {
  @ParameterizedTest
  @ValueSource(strings = {"IS_ALPHA", "IS_DIGIT", "IS_DECIMAL"})
  void unicodePredicatesConsumersAndNullsMatchTheReleasedFunction(String function) throws Exception {
    for (String sql : List.of(
        "SELECT id, IS_ALPHA(s), IS_ALPHA(CAST(s AS CHAR(4))) FROM src",
        "SELECT id FROM src WHERE IS_ALPHA(s)",
        "SELECT id, CASE WHEN IS_ALPHA(s) THEN 1 ELSE 0 END FROM src",
        "SELECT id, IS_ALPHA(s) OR s IS NULL, NOT IS_ALPHA(s) FROM src",
        "SELECT IS_ALPHA(CONCAT(s,'x')), IS_ALPHA(REPLACE(s,'?','a')) FROM src",
        "SELECT IS_ALPHA(s) FROM src WHERE id<0",
        "SELECT IS_ALPHA(s), COUNT(*) FROM src GROUP BY IS_ALPHA(s)")) {
      BuiltinFunctionParity.assertParity(this::environment, sql.replace("IS_ALPHA", function));
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"IS_ALPHA", "IS_DIGIT", "IS_DECIMAL"})
  void everyBmpUnitAndSupplementaryCharacterRunsAcrossMultipleNativeBatches(String function) throws Exception {
    List<String> values = new ArrayList<>();
    for (int unit = 0; unit <= Character.MAX_VALUE; unit++) values.add(String.valueOf((char) unit));
    values.add(new String(Character.toChars(0x10400)));
    values.add(null);
    values.add("");
    values.add(new String(Character.toChars(0x1d7ce)));
    assertRuntimeValues(function, values);
  }

  @Test
  void decimalGrammarUnicodeIntegerBoundsAndSeededStringsMatchReleasedParsing() throws Exception {
    List<String> values = new ArrayList<>();
    for (int unit = 0; unit <= Character.MAX_VALUE; unit++) {
      char ch = (char) unit;
      values.add("1" + ch + "2");
      if (Character.digit(ch, 10) >= 0) {
        values.add("922337203685477580" + ch);
        values.add("-922337203685477580" + ch);
      }
    }
    for (String sign : List.of("", "+", "-")) {
      for (String padding : List.of("", "\0\t ", "\u00a0")) {
        for (String body : List.of("", "0", "1", "01", "123", "1.", ".1", "1.0", "0x1",
            "0x.1", "0x1.", "NaN", "Infinity", "\u0661", "1\u0661",
            "922337203685477580\u0667", "922337203685477580\u0668")) {
          for (String exponent : List.of("", "e0", "e+1", "e-99999999999", "p0", "p99999999999", "e", "p+")) {
            for (String suffix : List.of("", "f", "D", "F", "d", "L", " ", "\0", "\u00a0")) {
              values.add(padding + sign + body + exponent + suffix + padding);
            }
          }
        }
      }
    }
    String alphabet = "0123456789abcdefxXpPeE+-.fFdDNaInfinity \t\n\0\u00a0\u0661\uff11";
    var random = new java.util.Random(494240L);
    for (int i = 0; i < 6000; i++) {
      var value = new StringBuilder();
      int length = random.nextInt(25);
      for (int j = 0; j < length; j++) value.append(alphabet.charAt(random.nextInt(alphabet.length())));
      values.add(value.toString());
    }
    values.add("0".repeat(256) + "\u0661");
    values.add("9".repeat(256) + "\u0661");
    values.add("-922337203685477580\u0668");
    values.add("-922337203685477580\u0669");
    assertRuntimeValues("IS_DECIMAL", values);
  }

  private void assertRuntimeValues(String function, List<String> values) throws Exception {
    for (boolean nativeEnabled : new boolean[] {false, true}) {
      try (var metrics = new RecoveryExecutionMetrics()) {
        var env = metrics.environment(new org.apache.flink.configuration.Configuration(), 1, 0);
        var table = org.apache.flink.table.api.bridge.java.StreamTableEnvironment.create(env);
        var type = ROW(FIELD("id", INT().notNull()), FIELD("s", STRING()));
        List<Row> rows = new ArrayList<>();
        for (int id = 0; id < values.size(); id++) rows.add(Row.of(id, values.get(id)));
        table.createTemporaryView("src",
            tech.streamfusion.compat.FlinkTestSources.fromData(env, rows,
                org.apache.flink.table.runtime.typeutils.ExternalTypeInfo.<Row>of(type)),
            org.apache.flink.table.api.Schema.newBuilder().fromRowDataType(type).build());
        var scan = nativeEnabled ? NativePlanner.install(table) : null;
        String sql = "SELECT id, " + function + "(s) FROM src";
        String plan = table.explainSql(sql);
        if (nativeEnabled) {
          assertTrue(plan.contains("NativeCalc"), plan);
          assertTrue(scan.fallbackReasons().isEmpty(), scan.fallbackReasons().toString());
          assertFalse(scan.functionExecutions().isEmpty());
          assertTrue(scan.functionExecutions().stream().allMatch(route -> route.contains("jvm=[]")),
              scan.functionExecutions().toString());
        }
        int count = 0;
        boolean[] seen = new boolean[rows.size()];
        try (var output = table.executeSql(sql).collect()) {
          while (output.hasNext()) {
            Row row = output.next();
            int id = (Integer) row.getField(0);
            assertTrue(id >= 0 && id < seen.length);
            assertFalse(seen[id], "duplicate unit " + id);
            seen[id] = true;
            String value = values.get(id);
            boolean expected = function.equals("IS_DECIMAL") ? SqlFunctionUtils.isDecimal(value)
                : function.equals("IS_DIGIT") ? SqlFunctionUtils.isDigit(value) : SqlFunctionUtils.isAlpha(value);
            assertEquals(expected, row.getField(1), "unit " + id);
            count++;
          }
        }
        assertEquals(rows.size(), count);
        if (nativeEnabled) RecoveryExecutionMetrics.assertConsumed(metrics.nativeOperators(), "NativeCalc");
      }
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"IS_ALPHA", "IS_DIGIT", "IS_DECIMAL"})
  void nonCharacterOverloadsKeepTheirGeneratedBehavior(String function) throws Exception {
    BuiltinFunctionParity.assertParity(this::environment,
        ("SELECT IS_ALPHA(id), IS_ALPHA(CAST(id AS DECIMAL(18,2))) FROM src").replace("IS_ALPHA", function));
  }

  @ParameterizedTest
  @ValueSource(strings = {"IS_ALPHA", "IS_DIGIT", "IS_DECIMAL"})
  void conditionalFailuresAndEmptyBatchesRemainLazy(String function) throws Exception {
    BuiltinFunctionParity.assertParity(this::environment,
        ("SELECT CASE WHEN id>=0 THEN IS_ALPHA(s) ELSE IS_ALPHA(CAST(1/(id-id) AS STRING)) END FROM src")
            .replace("IS_ALPHA", function));
    BuiltinFunctionParity.assertParity(this::environment,
        ("SELECT IS_ALPHA(CAST(1/(id-id) AS STRING)) FROM src WHERE id<0").replace("IS_ALPHA", function));
    NativeFailureParity.run(this::environment,
        ("SELECT IS_ALPHA(CAST(1/(id-id) AS STRING)) FROM src").replace("IS_ALPHA", function))
        .assertCompetingFailures(NativeFailureParity::isDivisionByZero, NativeFailureParity.Phase.ROW_EVALUATION,
            NativeFailureParity.Route.NATIVE);
  }

  private TableEnvironment environment() {
    List<Row> rows = new ArrayList<>();
    String[] values = {"a", "ABCxyz", "123", "a1", "", null, "a b", "\0", "\u4e2d",
        "\u00e9", "\u0301", "e\u0301", "\u0130", "\u0131", "\u00df", "\u2160",
        "\u0661", "\uff11", "12\u0661", "\u0966\u0967", "\u00b2", "\ud835\udfce",
        "\u037f", "\u0870", "\u20bf", "\ud801\udc00",
        "\ud835\udc9c", "\ud83d\ude00", "\ud800", "\udc00", "a\ud800", "?", "\ufffd",
        "NaN", "+NaN", "-NaN", "Infinity", "-Infinity", "+Infinity", "NaNf", "infinity",
        "1f", "1D", "-1.e2", ".5", "0x1.fp+3F", "0x.8p-99999999999", "0x1p99999999999",
        "1e99999999999", "1e-99999999999", "0x1", "0x.p1", "1e", "1e+", "1_0",
        "\0\t 1 \n\0", "\u00a01", "1\u00a0", " \u0661", "\u0661 ", "\u0661.0",
        "+\u0661", "-\u0661", "922337203685477580\u0667", "922337203685477580\u0668",
        "-922337203685477580\u0668", "-922337203685477580\u0669", "9223372036854775808"};
    for (int i = 0; i < values.length; i++) rows.add(Row.of(i, values[i]));
    return BuiltinFunctionParity.environment(ROW(FIELD("id", INT()), FIELD("s", STRING())), rows);
  }
}
