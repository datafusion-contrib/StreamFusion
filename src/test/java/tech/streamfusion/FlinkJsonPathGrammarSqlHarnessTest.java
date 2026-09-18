package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.runtime.testutils.InMemoryReporter;
import org.apache.flink.runtime.testutils.MiniClusterResource;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.util.TestStreamEnvironment;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.planner.NativePlanner;

class FlinkJsonPathGrammarSqlHarnessTest {
  @ParameterizedTest
  @ValueSource(
      strings = {
        "$[ 1 ]",
        "$[1 ]",
        "$[ 1]",
        "$[ 0001 ]",
        "$[ -1 ]",
        "$[ 'a' ]",
        "$[ \"a\" ]",
        "$['a' ]",
        "$[ 'a']",
        "$.a[ 1 ]",
        "$[ 'a b' ][ 1 ].q",
        "$[ 'a b' ][ 1 ][ 'q' ]",
        "$[1]  ",
        "$[ 2147483647 ]",
        "$.a ",
        "$[ ' a b ' ]"
      })
  void whitespacePathsMatchFlinkAcrossPolicies(String path) throws Exception {
    String sql = select(path);
    String plan = NativePlanner.explain(documents(), sql);
    assertTrue(plan.contains("NativeCalc"), plan);
    NativeParity.assertParity(FlinkJsonPathGrammarSqlHarnessTest::documents, sql);
  }

  static Stream<String> trailingIndexControls() {
    return Stream.concat(
        IntStream.rangeClosed(0, 0x20).mapToObj(c -> "$[ 0001 " + (char) c + " ]"),
        Stream.of(
            "$[ -1\t\r\n ]",
            "$[ -2147483648\u0000]",
            "$.a[1\n]",
            "$['a b'][1\t]['q']",
            "$[1\r][0\n]",
            "$[-0\f]"));
  }

  @ParameterizedTest
  @MethodSource("trailingIndexControls")
  void trailingIndexControlsMatchReleasedFlink(String path) throws Exception {
    whitespacePathsMatchFlinkAcrossPolicies(path);
  }

  @Test
  void trailingIndexControlsPreserveTypedResultsAcrossBatches() throws Exception {
    String[] rows = new String[5003];
    String[] values = {
      null,
      "[]",
      "[0,2147483647]",
      "[0,-2147483648]",
      "[0,null]",
      "invalid",
      "[0,7,{\"bad\":1e2147483648}]",
      "[0,9] trailing",
      "[0,{}]"
    };
    for (int i = 0; i < rows.length; i++) rows[i] = values[i % values.length];
    NativeParity.assertParity(
        () -> TextTimeFunctionTestInputs.textRows(rows),
        "SELECT id, JSON_VALUE(s, '$[ 01\t\n"
            + " ]' RETURNING INTEGER DEFAULT 7 ON EMPTY DEFAULT 9 ON ERROR), JSON_VALUE(s, 'lax"
            + " $[-1\u0000]' RETURNING INTEGER DEFAULT 5 ON EMPTY DEFAULT 8 ON ERROR),"
            + " JSON_EXISTS(s, '$[0\r"
            + "]' UNKNOWN ON ERROR) FROM inputs");
  }

  @Test
  void trailingIndexControlsExecuteNativeCalcAcrossBatches() throws Exception {
    var reporter = InMemoryReporter.createWithRetainedMetrics();
    var cluster =
        new MiniClusterResource(
            new MiniClusterResourceConfiguration.Builder()
                .setConfiguration(reporter.addToConfiguration(new Configuration()))
                .setNumberTaskManagers(1)
                .setNumberSlotsPerTaskManager(2)
                .build());
    cluster.before();
    try {
      var env = new TestStreamEnvironment(cluster.getMiniCluster(), 1);
      var table = StreamTableEnvironment.create(env);
      table.createTemporaryView(
          "inputs",
          env.fromSequence(0, 5002)
              .map(id -> Row.of(id, "{\"a\":[0,\"value\"]}"))
              .returns(Types.ROW_NAMED(new String[] {"id", "s"}, Types.LONG, Types.STRING)));
      var scan = NativePlanner.install(table);
      var result =
          table.executeSql(
              "SELECT id, JSON_VALUE(s, '$.a[1\t\n ]'), "
                  + "JSON_EXISTS(s, '$.a[-1\r]') FROM inputs");
      long count = 0;
      try (var iterator = result.collect()) {
        while (iterator.hasNext()) {
          assertEquals(Row.of(count++, "value", true), iterator.next());
        }
      }
      assertEquals(5003, count);
      assertTrue(scan.substitutions() > 0, scan.explainSummary());
      assertTrue(scan.fallbackReasons().isEmpty(), scan.explainSummary());
      var groups =
          reporter.findOperatorMetricGroups(
              result.getJobClient().orElseThrow().getJobID(), "(?i)NativeCalcExecNode");
      assertEquals(1, groups.size(), "expected the JSON expressions to execute in one native Calc");
      var metrics = reporter.getMetricsByGroup(groups.iterator().next());
      assertEquals(5003, ((Counter) metrics.get("numRecordsIn")).getCount());
      assertEquals(5003, ((Counter) metrics.get("numRecordsOut")).getCount());
    } finally {
      cluster.after();
    }
  }

  @Test
  void trailingIndexControlsKeepFailurePolicies() {
    JsonFunctionTestInputs.assertFailsLikeFlink("[]", "JSON_EXISTS(s, '$[1\n]' ERROR ON ERROR)");
    JsonFunctionTestInputs.assertFails(
        "[0]", "JSON_VALUE(s, '$[1\t]' ERROR ON ERROR)", "JSON_VALUE ERROR");
    JsonFunctionTestInputs.assertFails(
        "[0]", "JSON_VALUE(s, 'lax $[-2\r]' ERROR ON EMPTY)", "JSON_VALUE EMPTY");
    NativeFailureParity.run(
            () -> TextTimeFunctionTestInputs.textRows("[0,\"bad\"]"),
            "SELECT JSON_VALUE(s, '$[1\f]' RETURNING INTEGER NULL ON ERROR) FROM inputs")
        .assertFailure(
            ClassCastException.class,
            "java.lang.String cannot be cast to class java.lang.Integer",
            NativeFailureParity.Phase.ROW_EVALUATION,
            NativeFailureParity.Route.NATIVE);
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "$[-2147483649]",
        "$[+1]",
        "$[1,2:3]",
        "$[1:2]",
        "$[*][1:2]",
        "$..a",
        "$.a[*].b.length()",
        "$[?(@.a)]",
        "$[2147483648]",
        "$[1 2]",
        "$[1\t2]",
        "$[-\t1]",
        "$[1\u007f]",
        "$[1\u0085]",
        "$[1\u00a0]",
        "$[1\u2028]",
        "$[]",
        "$[\t1\t]",
        "$[ \n1 ]",
        "$[\t'a']",
        "$['a'\n]",
        " $[1] ",
        " $ ",
        "$[1]\t",
        "$[1]\n",
        "$[1] \t",
        "$['a'] \n"
      })
  void unsupportedSelectorsRetainExplicitFallback(String path) throws Exception {
    NativeParity.assertFallbackReasonContains(
        FlinkJsonPathGrammarSqlHarnessTest::documents,
        select(path),
        "literal definite member/index path");
  }

  @Test
  void multipleIndependentPathsShareNoSelectionState() throws Exception {
    String sql =
        "SELECT id, JSON_VALUE(s, '$[ 1 ]'), JSON_VALUE(s, '$[ 0 ]'), JSON_VALUE(s, '$[ ''a b'' ]["
            + " 1 ][ ''q'' ]'), JSON_EXISTS(s, '$[ ''a'' ]') FROM inputs";
    assertTrue(NativePlanner.explain(documents(), sql).contains("NativeCalc"));
    NativeParity.assertParity(FlinkJsonPathGrammarSqlHarnessTest::documents, sql);
  }

  @Test
  void whitespaceIndexPreservesIntegerReturningAndDefaults() throws Exception {
    NativeParity.assertParity(
        () ->
            TextTimeFunctionTestInputs.textRows(
                null, "[]", "[0,2147483647]", "[0,-2147483648]", "[0,null]", "invalid"),
        "SELECT id, JSON_VALUE(s, '$[ 1 ]' RETURNING INTEGER DEFAULT 7 ON EMPTY DEFAULT 9 ON ERROR)"
            + " FROM inputs");
  }

  @Test
  void whitespaceExistsPreservesHostException() {
    JsonFunctionTestInputs.assertFailsLikeFlink("[]", "JSON_EXISTS(s, '$[ 1 ]' ERROR ON ERROR)");
  }

  @ParameterizedTest
  @ValueSource(strings = {"$['a']", "$[ 'a' ]"})
  void valueErrorPolicyFailsForCompactAndSpacedPaths(String path) {
    // ERROR-policy failures retain their native wrapper; scalar type mismatches preserve the host
    // exception.
    JsonFunctionTestInputs.assertFails(
        "{}",
        "JSON_VALUE(s, '" + path.replace("'", "''") + "' ERROR ON ERROR)",
        "JSON_VALUE ERROR");
  }

  @Test
  void invalidIntegerScalarFailsOutsideErrorPolicy() {
    var comparison =
        NativeFailureParity.run(
            () -> TextTimeFunctionTestInputs.textRows("[0,\"text\"]"),
            "SELECT JSON_VALUE(s, '$[ 1 ]' RETURNING INTEGER NULL ON ERROR) FROM inputs");
    comparison.assertFailure(
        ClassCastException.class,
        "java.lang.String cannot be cast to class java.lang.Integer",
        NativeFailureParity.Phase.ROW_EVALUATION,
        NativeFailureParity.Route.NATIVE);
    org.junit.jupiter.api.Assertions.assertEquals(
        comparison.host().rootCause().getMessage(),
        comparison.nativeRun().rootCause().getMessage());
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "lax $[1]",
        "LaX $[1]",
        " lax $[1]",
        "\tlax $[1]",
        " \tLaX $[1]",
        "lax   $[ 1 ]",
        " \tLaX $[ 1 ]  ",
        "strict $[ 'a' ] "
      })
  void explicitModeWhitespaceMatchesFlink(String path) throws Exception {
    String quoted = path.replace("'", "''");
    String sql =
        "SELECT id, JSON_VALUE(s, '"
            + quoted
            + "'), "
            + "JSON_EXISTS(s, '"
            + quoted
            + "' UNKNOWN ON ERROR) FROM inputs";
    assertTrue(NativePlanner.explain(documents(), sql).contains("NativeCalc"));
    NativeParity.assertParity(FlinkJsonPathGrammarSqlHarnessTest::documents, sql);
  }

  private static String select(String path) {
    String quoted = path.replace("'", "''");
    return "SELECT id, JSON_EXISTS(s, '"
        + quoted
        + "' UNKNOWN ON ERROR), "
        + "JSON_EXISTS(s, 'lax "
        + quoted
        + "' UNKNOWN ON ERROR), "
        + "JSON_VALUE(s, '"
        + quoted
        + "' DEFAULT 'empty' ON EMPTY DEFAULT 'error' ON ERROR), "
        + "JSON_VALUE(s, 'lax "
        + quoted
        + "' DEFAULT 'empty' ON EMPTY DEFAULT 'error' ON ERROR) FROM inputs";
  }

  private static TableEnvironment documents() {
    return TextTimeFunctionTestInputs.textRows(
        null,
        "",
        "null",
        "{}",
        "[]",
        "true",
        "42",
        "\"root\"",
        "[0,17]",
        "[0,null]",
        "[0,\"text\"]",
        "[0,[]]",
        "[0,{}]",
        "{\"a\":[0,17],\"a b\":[1,{\"q\":\"hello\"}]}",
        "{\"a\":\"value\",\"a b\":[1,{\"q\":null}]}",
        "{\"a\":false}",
        "{\"a\":null}",
        "{\"a\":\"old\",\"a\":\"new\"}",
        "{\" a b \":\"kept\",\"a b\":\"different\"}",
        "{\"a\":\"ok\",\"bad\":[}",
        "[0,17,tru]",
        "[0,17] trailing",
        "{\"a\":\"valid\",\"bad\":1e2147483648}");
  }
}
