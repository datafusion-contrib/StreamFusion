package tech.streamfusion;

import static org.apache.flink.table.api.DataTypes.*;
import static org.junit.jupiter.api.Assertions.*;
import static tech.streamfusion.compat.FlinkTestSources.fromData;

import java.util.ArrayList;
import java.util.List;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.runtime.typeutils.ExternalTypeInfo;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import tech.streamfusion.compat.FlinkTestCapabilities;
import tech.streamfusion.planner.NativePlanner;
import tech.streamfusion.runtime.common.NativeParity;

class FlinkNativeStringRoutingSqlHarnessTest {
  @Test
  void stringEditsAndSearchesHaveNoJvmEvaluator() throws Exception {
    assertNative(
        "s || t, CONCAT(s,t), CONCAT_WS(p,s,t), REPEAT(s,n), REPLACE(s,t,p), REVERSE(s), "
            + "SUBSTRING(s FROM n FOR 2), SUBSTR(s,n,2), LEFT(s,n), RIGHT(s,n), INITCAP(s), "
            + "CHAR_LENGTH(s), CHARACTER_LENGTH(s), POSITION(t IN s), INSTR(s,t), LOCATE(t,s,n), "
            + "ASCII(s), CHR(n), TO_BASE64(s), TRIM(s), LTRIM(s), RTRIM(s)");
  }

  @Test
  void literalComparisonsOfUtf16EditsHaveNoJvmEvaluator() throws Exception {
    assertNative(
        "LPAD(s,n,p) = '?', RPAD(s,n,p) <> 'x', LPAD(s,1,'x') = '?', "
            + "OVERLAY(s PLACING p FROM 1 FOR 1) = '?', OVERLAY(s PLACING 'x' FROM 1) <> '?'");
  }

  @Test
  void caseConversionAndComparisonsHaveNoJvmEvaluator() throws Exception {
    assertNative("UPPER(s), LOWER(s), LOWER(UPPER(s)), UPPER(LOWER(s)) = 'ABC', "
        + "LOWER(s) = UPPER(t), REPLACE(LOWER(s), 'a', 'B'), "
        + "UPPER(REPLACE(s, 'a', 'ß')), LOWER(LEFT(s,n))");
  }

  @Test
  void directUtf16EditsHaveNoJvmEvaluator() throws Exception {
    assertNative("LPAD(s,n,p), RPAD(s,n,p), OVERLAY(s PLACING p FROM n), "
        + "OVERLAY(s PLACING p FROM n FOR n + 1)");
  }

  @Test
  void runtimeSqlTrimSetsHaveNoJvmEvaluator() throws Exception {
    assertNative("TRIM(BOTH t FROM s), TRIM(LEADING t FROM s), TRIM(TRAILING t FROM s), "
        + "TRIM(BOTH LPAD(t,2,'x') FROM s), CHAR_LENGTH(TRIM(BOTH t FROM s)), "
        + "TRIM(BOTH t FROM s) = 'abc'");
  }

  @Test
  void runtimeBtrimSetsHaveNoJvmEvaluator() throws Exception {
    FlinkTestCapabilities.requireSqlFunction("BTRIM");
    assertNative("BTRIM(s), BTRIM(s,t), LTRIM(s,t), RTRIM(s,t)");
  }

  @Test
  void nativeEditChainsCanRejoinSplitSurrogates() throws Exception {
    String rejoin = "OVERLAY(s PLACING OVERLAY(s PLACING '' FROM 1 FOR 1) FROM 2 FOR 1)";
    assertNative(rejoin + ", CHAR_LENGTH(" + rejoin + "), LPAD(UPPER(s),1,'x'), "
        + "UPPER(LPAD(s,1,'x')) = '?', LPAD(s,n,p) = RPAD(s,n,p), "
        + "REPLACE(LPAD(s,1,'x'), '?', 'Z'), OVERLAY(s PLACING p FROM n FOR n + 1) = '?', "
        + "UPPER(s) IS NULL, CASE WHEN LPAD(s,1,'x') = '?' THEN 1 ELSE 0 END");
  }

  @ParameterizedTest
  @CsvSource(
      delimiter = ';',
      value = {
        "TRANSLATE3;TRANSLATE(s,t,p)",
        "STARTSWITH;STARTSWITH(s,t)",
        "ENDSWITH;ENDSWITH(s,t)",
        "URL_ENCODE;URL_ENCODE(s)",
        "URL_DECODE;URL_DECODE(s)",
        "ELT;ELT(n,s,t,p)",
        "JSON_QUOTE;JSON_QUOTE(s)"
      })
  void releaseSpecificStringFunctionsHaveNoJvmEvaluator(String function, String expression)
      throws Exception {
    FlinkTestCapabilities.requireSqlFunction(function);
    assertNative(expression);
  }

  private static void assertNative(String expressions) throws Exception {
    String sql = "SELECT " + expressions + " FROM src";
    List<Row> input =
        List.of(
            Row.of("  Ab中🙂b ", "b", "x", 2),
            Row.of("abc", "a", "-", 1),
            Row.of("A+B%23", "A", "_", 3),
            Row.of("🙂", "🙂", "x", 1),
            Row.of("?", ".", "x", 1),
            Row.of("", "", " ", 0),
            Row.of(null, "a", null, 1),
            Row.of("abc", null, "z", 1),
            Row.of("abc", "bc", ".", null));
    try (var metrics = RecoveryExecutionMetrics.countersOnly()) {
      var host = table(metrics.environment(new Configuration(), 1, 0), input);
      var enabled = table(metrics.environment(new Configuration(), 1, 0), input);
      var scan = NativePlanner.install(enabled);
      var plan = enabled.explainSql(sql);
      assertFalse(plan.contains("jvmExpressions="), plan);
      var stock = host.executeSql(sql);
      var expected = new ArrayList<List<Object>>();
      try (var rows = stock.collect()) {
        rows.forEachRemaining(row -> expected.add(fields(row)));
      }
      var result = enabled.executeSql(sql);
      var actual = new ArrayList<List<Object>>();
      try (var rows = result.collect()) {
        rows.forEachRemaining(row -> actual.add(fields(row)));
      }
      assertEquals(stock.getResolvedSchema(), result.getResolvedSchema(), sql);
      assertEquals(NativeParity.multiset(expected), NativeParity.multiset(actual), sql);
      assertTrue(scan.fallbackReasons().isEmpty(), scan::explainSummary);
      var routes = scan.functionExecutions();
      assertFalse(routes.isEmpty(), scan::explainSummary);
      assertTrue(routes.stream().allMatch(route -> route.contains("jvm=[]")), scan::explainSummary);
      var job = result.getJobClient().orElseThrow();
      job.getJobExecutionResult().get();
      assertEquals(input.size(), metrics.recordsIn(job.getJobID().toString(), "NativeCalc("), sql);
    }
  }

  private static List<Object> fields(Row row) {
    var values = new ArrayList<>();
    for (int i = 0; i < row.getArity(); i++) {
      values.add(NativeParity.comparableValue(row.getField(i)));
    }
    return values;
  }

  private static StreamTableEnvironment table(StreamExecutionEnvironment env, List<Row> input) {
    var table = StreamTableEnvironment.create(env);
    var type = ROW(FIELD("s", STRING()), FIELD("t", STRING()), FIELD("p", STRING()), FIELD("n", INT()));
    table.createTemporaryView(
        "src",
        fromData(env, input, ExternalTypeInfo.<Row>of(type)),
        Schema.newBuilder().fromRowDataType(type).build());
    return table;
  }
}
