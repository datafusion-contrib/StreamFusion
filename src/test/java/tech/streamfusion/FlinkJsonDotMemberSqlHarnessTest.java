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
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.io.JsonStringEncoder;
import org.apache.flink.streaming.util.TestStreamEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import tech.streamfusion.planner.NativePlanner;

class FlinkJsonDotMemberSqlHarnessTest {
  static Stream<String> names() {
    return Stream.concat(
        IntStream.rangeClosed(0, 127)
            .filter(c -> c != ' ' && c != '.' && c != '[' && c != '(' && c != '*')
            .mapToObj(c -> Character.toString((char) c)),
        Stream.of(
            "order-id",
            "123",
            "a*b",
            "a]b",
            "a\\u0061",
            "\\n",
            "a\tb",
            "a\t",
            "O'Reilly",
            "用户",
            "😀",
            "\u0301",
            "\u0085",
            "\u00a0",
            "\u2028",
            "\u2029",
            "\uffff"));
  }

  @ParameterizedTest
  @MethodSource("names")
  void dotMembersMatchReleasedFlink(String name) throws Exception {
    String key = json(name);
    String[] documents = {
      null,
      "null",
      "{}",
      "[]",
      "bad JSON",
      "{" + key + ":\"first\"," + key + ":\"last\"," + json(name + "x") + ":\"neighbor\"}",
      "{" + key + ":null}",
      "{" + key + ":false}",
      "{" + key + ":7}",
      "{" + key + ":{}}",
      "{" + key + ":[]}",
      "{" + key + ":{\"x\":[null,\"nested\"]}}",
      "{" + key + ":\"okay\",\"bad\":[}",
      "{" + key + ":\"okay\",\"bad\":1e2147483648}",
      "{" + key + ":\"okay\"} trailing"
    };
    String path = "$." + name;
    String sql =
        "SELECT id, JSON_VALUE(s, "
            + literal("strict " + path)
            + " DEFAULT 'empty' ON EMPTY DEFAULT 'error' ON ERROR), JSON_VALUE(s, "
            + literal("lax " + path)
            + " DEFAULT 'empty' ON EMPTY DEFAULT 'error' ON ERROR), "
            + "JSON_EXISTS(s, "
            + literal("strict " + path)
            + " TRUE ON ERROR), JSON_EXISTS(s, "
            + literal("lax " + path)
            + " UNKNOWN ON ERROR), JSON_VALUE(s, "
            + literal("lax " + path + ".x[-1]")
            + "), JSON_EXISTS(s, "
            + literal("lax $." + name + "x")
            + ") FROM inputs";
    NativeParity.assertParity(() -> TextTimeFunctionTestInputs.textRows(documents), sql);
  }

  @Test
  void errorPoliciesAndTypedFailuresRemainNative() {
    JsonFunctionTestInputs.assertFailsLikeFlink(
        "{}", "JSON_EXISTS(s, '$.order-id' ERROR ON ERROR)");
    JsonFunctionTestInputs.assertFails(
        "{}", "JSON_VALUE(s, 'lax $.123' ERROR ON EMPTY)", "JSON_VALUE EMPTY");
    NativeFailureParity.run(
            () -> TextTimeFunctionTestInputs.textRows("{\"order-id\":\"bad\"}"),
            "SELECT JSON_VALUE(s, '$.order-id' RETURNING INTEGER NULL ON ERROR) FROM inputs")
        .assertFailure(
            ClassCastException.class,
            "java.lang.String cannot be cast to class java.lang.Integer",
            NativeFailureParity.Phase.ROW_EVALUATION,
            NativeFailureParity.Route.NATIVE);
  }

  @Test
  void dotMembersExecuteNativeCalcAcrossBatches() throws Exception {
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
              .map(id -> Row.of(id, "{\"order-id\":{\"123\":\"value\"},\"a\\tb\":true}"))
              .returns(Types.ROW_NAMED(new String[] {"id", "s"}, Types.LONG, Types.STRING)));
      var scan = NativePlanner.install(table);
      var result =
          table.executeSql(
              "SELECT id, JSON_VALUE(s, '$.order-id.123'), "
                  + "JSON_EXISTS(s, '$.a\tb') FROM inputs");
      long count = 0;
      try (var rows = result.collect()) {
        while (rows.hasNext()) assertEquals(Row.of(count++, "value", true), rows.next());
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

  private static String json(String name) {
    return "\"" + new String(JsonStringEncoder.getInstance().quoteAsString(name)) + "\"";
  }

  private static String literal(String path) {
    return "'" + path.replace("'", "''") + "'";
  }
}
