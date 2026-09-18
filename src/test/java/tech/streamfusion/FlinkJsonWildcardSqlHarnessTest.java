package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
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
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.planner.NativePlanner;

class FlinkJsonWildcardSqlHarnessTest {
  @ParameterizedTest
  @ValueSource(
      strings = {
        "$.*",
        "$[*]",
        "$[ * ]",
        "$[  *  ]  ",
        "$.a.*",
        "$.a[*]",
        "$['a'][*]",
        "$[0][*]",
        "$[1].a[*]",
        "$[-1][*]",
        "$[-2].a[*]",
        "$[-2147483648][*]",
        "$[2147483647][*]",
        "$.a[0][*]",
        "$.a[1][*]",
        "$.a[-2].x[*]",
        "$.a.x[*]",
        "$.a[0].x[*]",
        "$[ 'a' ][ -1\t ][ * ]",
        "$['*'][*]",
        "$[''][*]",
        "$.order-id[*]",
        "$.用户[*]",
        "$['\\u0061'][*]",
        "$[*].a",
        "$.*.a",
        "$[*][0]",
        "$.*.*",
        "$.a[*].x",
        "$.a[*].x[-1]",
        "$.a[*].x[2147483647]",
        "$.a[*].x[-2147483648]",
        "$.a.*.x[0]",
        "$.a[*][*]",
        "$[*].a[*].x",
        "$[*][0][*].missing",
        "$.a[1].x[*][-1]",
        "$.a[-2].x[*][-1]",
        "$[*]['*']",
        "$[*]['']",
        "$[*]['\\u0061']",
        "$[*].order-id"
      })
  void wildcardsMatchReleasedFlinkAcrossPolicies(String path) throws Exception {
    String strict = literal("strict " + path);
    String lax = literal("lax " + path);
    NativeParity.assertParity(
        FlinkJsonWildcardSqlHarnessTest::documents,
        "SELECT id, JSON_VALUE(s, "
            + strict
            + " DEFAULT 'empty' ON EMPTY DEFAULT 'error' ON ERROR), JSON_VALUE(s, "
            + lax
            + " DEFAULT 'empty' ON EMPTY DEFAULT 'error' ON ERROR), JSON_EXISTS(s, "
            + strict
            + "), JSON_EXISTS(s, "
            + strict
            + " TRUE ON ERROR), JSON_EXISTS(s, "
            + strict
            + " UNKNOWN ON ERROR), JSON_EXISTS(s, "
            + lax
            + " UNKNOWN ON ERROR), JSON_VALUE(s, "
            + strict
            + " RETURNING INTEGER DEFAULT 7 ON EMPTY DEFAULT 9 ON ERROR), JSON_VALUE(s, "
            + lax
            + " RETURNING BOOLEAN DEFAULT TRUE ON EMPTY DEFAULT FALSE ON ERROR), "
            + "JSON_VALUE(s, "
            + lax
            + " RETURNING DOUBLE), JSON_VALUE(s, 'lax $.a'), "
            + "JSON_EXISTS(s, 'lax $.a[0]') FROM inputs");
  }

  @Test
  void errorPoliciesDistinguishMissingPropertiesFromAbsentIndexes() throws Exception {
    NativeParity.assertParity(
        () -> TextTimeFunctionTestInputs.textRows(null, "[]", "[null]", "[1]", "[[1]]"),
        "SELECT JSON_EXISTS(s, '$[1].missing[*]' ERROR ON ERROR), "
            + "JSON_EXISTS(s, '$[-2].missing[*]' ERROR ON ERROR), "
            + "JSON_EXISTS(s, '$[*]' ERROR ON ERROR) FROM inputs");
    for (String document : new String[] {"{}", "{\"a\":[]}", "{\"a\":null}", "invalid", "null"}) {
      JsonFunctionTestInputs.assertFailsLikeFlink(
          document, "JSON_EXISTS(s, '$.a.x[*]' ERROR ON ERROR)");
    }
    JsonFunctionTestInputs.assertFailsLikeFlink(
        "null", "JSON_EXISTS(s, 'lax $[*]' ERROR ON ERROR)");
    JsonFunctionTestInputs.assertFails(
        "[]", "JSON_VALUE(s, '$[*]' ERROR ON ERROR)", "JSON_VALUE ERROR");
    JsonFunctionTestInputs.assertFails(
        "{}", "JSON_VALUE(s, 'lax $.a[*]' ERROR ON EMPTY)", "JSON_VALUE EMPTY");
  }

  @Test
  void predicateDistinguishesInvalidDocumentsAndMissingPaths() throws Exception {
    NativeParity.assertParity(
        FlinkJsonWildcardSqlHarnessTest::documents,
        "SELECT id FROM inputs WHERE JSON_EXISTS(s, 'lax $.a[*]')");
  }

  @Test
  void wildcardsExecuteNativeCalcAcrossBatches() throws Exception {
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
              .map(
                  id ->
                      Row.of(
                          id,
                          switch ((int) (id % 4)) {
                            case 0 -> "{\"a\":[]}";
                            case 1 -> "{\"a\":null}";
                            case 2 -> "{}";
                            default -> "[1]";
                          }))
              .returns(Types.ROW_NAMED(new String[] {"id", "s"}, Types.LONG, Types.STRING)));
      var scan = NativePlanner.install(table);
      var result =
          table.executeSql(
              "SELECT id, JSON_EXISTS(s, '$.a[*]'), "
                  + "JSON_EXISTS(s, 'lax $.a[*]'), JSON_VALUE(s, '$.*' DEFAULT 'error' ON ERROR), "
                  + "JSON_VALUE(s, 'lax $[*]' DEFAULT 'empty' ON EMPTY) FROM inputs");
      long count = 0;
      try (var rows = result.collect()) {
        while (rows.hasNext()) {
          assertEquals(Row.of(count, count % 4 < 2, true, "error", "empty"), rows.next());
          count++;
        }
      }
      assertEquals(5003, count);
      assertTrue(scan.fallbackReasons().isEmpty(), scan.explainSummary());
      var groups =
          reporter.findOperatorMetricGroups(
              result.getJobClient().orElseThrow().getJobID(), "(?i)NativeCalcExecNode");
      assertEquals(1, groups.size());
      var metrics = reporter.getMetricsByGroup(groups.iterator().next());
      assertEquals(5003, ((Counter) metrics.get("numRecordsIn")).getCount());
      assertEquals(5003, ((Counter) metrics.get("numRecordsOut")).getCount());
    } finally {
      cluster.after();
    }
  }

  private static TableEnvironment documents() {
    var rows =
        new ArrayList<>(
            Arrays.asList(
                null,
                "",
                "null",
                "true",
                "false",
                "123",
                "\"root\"",
                "[]",
                "[null]",
                "[1]",
                "[1,2,3]",
                "[{},null]",
                "[{},{}]",
                "[[0],{\"a\":[]}]",
                "true trailing",
                "truex",
                "1,",
                "nullx",
                "invalid",
                "[".repeat(1001) + "0" + "]".repeat(1001)));
    StringBuilder padding = new StringBuilder();
    for (int i = 0; i < 16; i++) padding.append(",\"k").append(i).append("\":\"v\"");
    for (String fields :
        new String[] {
          "\"z\":0",
          "\"a\":null",
          "\"a\":false",
          "\"a\":1",
          "\"a\":\"one\"",
          "\"a\":[]",
          "\"a\":[null]",
          "\"a\":[1]",
          "\"a\":[1,2,3]",
          "\"a\":{}",
          "\"a\":{\"x\":[]}",
          "\"a\":[{\"x\":null},{}]",
          "\"a\":[{},{}]",
          "\"a\":{\"x\":[1]},\"a\":{}",
          "\"a\":{},\"a\":[1]",
          "\"a\":[{},{}],\"a\":[]",
          "\"a\":[],\"a\":[{},{}]",
          "\"a\":[],\"\\u0061\":{\"x\":1}",
          "\"a\":[],\"bad\":1e2147483648",
          "\"a\":[],\"bad\":[}",
          "\"a\":[],\"bad\":\"\\ud800\"",
          "\"a\":[],\"bad\":\"\\q\"",
          "\"*\":[1],\"\":[2],\"order-id\":[3],\"用户\":[4]"
        }) {
      rows.add("{" + fields + "}");
      rows.add("{" + fields + padding + "}");
      rows.add("[{" + fields + padding + "}]");
    }
    rows.add("{\"a\":[]} trailing");
    rows.add("{\"a\":[],\"" + "k".repeat(50001) + "\":1}");
    return TextTimeFunctionTestInputs.textRows(rows.toArray(String[]::new));
  }

  private static String literal(String path) {
    return "'" + path.replace("'", "''") + "'";
  }
}
