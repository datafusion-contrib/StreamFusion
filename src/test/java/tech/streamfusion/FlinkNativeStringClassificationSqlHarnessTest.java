package tech.streamfusion;

import static org.apache.flink.table.api.DataTypes.*;
import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.List;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.types.Row;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.planner.NativePlanner;

class FlinkNativeStringClassificationSqlHarnessTest {
  @ParameterizedTest
  @ValueSource(strings = {"IS_ALPHA", "IS_DIGIT"})
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
  @ValueSource(strings = {"IS_ALPHA", "IS_DIGIT"})
  void everyBmpUnitAndSupplementaryCharacterRunsAcrossMultipleNativeBatches(String function) throws Exception {
    for (boolean nativeEnabled : new boolean[] {false, true}) {
      try (var metrics = new RecoveryExecutionMetrics()) {
        var env = metrics.environment(new org.apache.flink.configuration.Configuration(), 1, 0);
        var table = org.apache.flink.table.api.bridge.java.StreamTableEnvironment.create(env);
        var type = ROW(FIELD("id", INT().notNull()), FIELD("s", STRING()));
        List<Row> rows = new ArrayList<>();
        for (int unit = 0; unit <= Character.MAX_VALUE; unit++) {
          rows.add(Row.of(unit, String.valueOf((char) unit)));
        }
        rows.add(Row.of(65536, new String(Character.toChars(0x10400))));
        rows.add(Row.of(65537, null));
        rows.add(Row.of(65538, ""));
        rows.add(Row.of(65539, new String(Character.toChars(0x1d7ce))));
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
          assertTrue(scan.functionExecutions().stream().anyMatch(route -> route.contains("jvm=[]")),
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
            boolean expected = id < 65536 && (function.equals("IS_DIGIT")
                ? Character.isDigit((char) id) : Character.isLetter((char) id));
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
  @ValueSource(strings = {"IS_ALPHA", "IS_DIGIT"})
  void nonCharacterOverloadsKeepTheirGeneratedBehavior(String function) throws Exception {
    BuiltinFunctionParity.assertParity(this::environment,
        ("SELECT IS_ALPHA(id), IS_ALPHA(CAST(id AS DECIMAL(18,2))) FROM src").replace("IS_ALPHA", function));
  }

  @ParameterizedTest
  @ValueSource(strings = {"IS_ALPHA", "IS_DIGIT"})
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
        "\ud835\udc9c", "\ud83d\ude00", "\ud800", "\udc00", "a\ud800", "?", "\ufffd"};
    for (int i = 0; i < values.length; i++) rows.add(Row.of(i, values[i]));
    return BuiltinFunctionParity.environment(ROW(FIELD("id", INT()), FIELD("s", STRING())), rows);
  }
}
