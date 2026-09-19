package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.runtime.testutils.InMemoryReporter;
import org.apache.flink.runtime.testutils.MiniClusterResource;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.util.TestStreamEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class FlinkJsonJvmSqlHarnessTest {
  @ParameterizedTest
  @ValueSource(
      strings = {
        "JSON_VALUE(s, p DEFAULT 'empty' ON EMPTY DEFAULT 'error' ON ERROR)",
        "JSON_QUERY(s, p)",
        "JSON_QUERY(s, p WITH CONDITIONAL ARRAY WRAPPER EMPTY ARRAY ON EMPTY NULL ON ERROR)",
        "JSON_QUERY(s, p WITH UNCONDITIONAL ARRAY WRAPPER EMPTY OBJECT ON EMPTY EMPTY ARRAY ON"
            + " ERROR)"
      })
  void dynamicPathsUseReleasedFlinkForSelectionAndErrors(String expression) throws Exception {
    NativeParity.assertParity(
        FlinkJsonJvmSqlHarnessTest::dynamicPaths, "SELECT id, " + expression + " FROM inputs");
  }

  @Test
  void dynamicPathsAlsoRunInsidePredicates() throws Exception {
    NativeParity.assertParity(
        FlinkJsonJvmSqlHarnessTest::dynamicPaths,
        "SELECT id FROM inputs WHERE JSON_VALUE(s, p DEFAULT 'missing' ON EMPTY DEFAULT 'missing'"
            + " ON ERROR) = 'missing'");
  }

  @Test
  void unsupportedDynamicExistsKeepsTheHostPlanningFailure() {
    NativeFailureParity.run(
            FlinkJsonJvmSqlHarnessTest::dynamicPaths,
            "SELECT JSON_EXISTS(s, p UNKNOWN ON ERROR) FROM inputs")
        .assertFailure(
            org.apache.flink.table.planner.codegen.CodeGenException.class,
            "Unsupported call: JSON_EXISTS",
            NativeFailureParity.Phase.PLANNING,
            NativeFailureParity.Route.FALLBACK);
  }

  @Test
  void filteredChangelogRowsKeepTheirKindsAndSkipInvalidProjections() throws Exception {
    NativeParity.assertOrderedKindedParity(
        () -> {
          var env = StreamExecutionEnvironment.getExecutionEnvironment();
          env.setParallelism(1);
          var table = StreamTableEnvironment.create(env);
          var source =
              env.fromData(
                  List.of(
                      Row.ofKind(RowKind.INSERT, 1, "{\"n\":1}"),
                      Row.ofKind(RowKind.INSERT, 0, "invalid"),
                      Row.ofKind(RowKind.UPDATE_BEFORE, 1, "{\"n\":1}"),
                      Row.ofKind(RowKind.UPDATE_AFTER, 2, "{\"n\":2}"),
                      Row.ofKind(RowKind.DELETE, 0, "invalid"),
                      Row.ofKind(RowKind.DELETE, 2, "{\"n\":2}")),
                  Types.ROW_NAMED(new String[] {"id", "s"}, Types.INT, Types.STRING));
          table.createTemporaryView(
              "inputs",
              table.fromChangelogStream(
                  source,
                  Schema.newBuilder()
                      .column("id", DataTypes.INT())
                      .column("s", DataTypes.STRING())
                      .build()));
          return table;
        },
        "SELECT id, JSON_VALUE(s, '$.n' RETURNING INTEGER ERROR ON ERROR), JSON_OBJECT('n' VALUE"
            + " id) FROM inputs WHERE id <> 0 AND JSON_EXISTS(s, '$.n' ERROR ON ERROR)");
  }

  @Test
  void generatedJsonResultsComposeWithNativeAggregation() throws Exception {
    NativeParity.assertParity(
        FlinkJsonJvmSqlHarnessTest::dynamicPaths,
        "SELECT JSON_EXISTS(s, '$.a'), COUNT(*) FROM inputs GROUP BY JSON_EXISTS(s, '$.a')");
  }

  @Test
  void planIdentifiesTheJvmEvaluatorInsideNativeCalc() {
    String plan =
        tech.streamfusion.planner.NativePlanner.explain(
            dynamicPaths(), "SELECT JSON_QUERY(s, p) FROM inputs");
    org.junit.jupiter.api.Assertions.assertTrue(plan.contains("NativeCalc"), plan);
    org.junit.jupiter.api.Assertions.assertTrue(plan.contains("jsonEvaluation=[JVM]"), plan);
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "JSON_VALUE(s, '$.a')",
        "JSON_EXISTS(s, '$.a')",
        "s IS JSON VALUE",
        "JSON_QUOTE(s)",
        "JSON_UNQUOTE(s)",
        "JSON_STRING(s)",
        "JSON_OBJECT('a' VALUE s)"
      })
  void verifiedFastPathsKeepTheirNativeEvaluator(String expression) throws Exception {
    String sql = "SELECT " + expression + " FROM inputs";
    String plan = tech.streamfusion.planner.NativePlanner.explain(dynamicPaths(), sql);
    org.junit.jupiter.api.Assertions.assertTrue(plan.contains("NativeCalc"), plan);
    org.junit.jupiter.api.Assertions.assertFalse(plan.contains("jsonEvaluation=[JVM]"), plan);
    NativeParity.assertParity(FlinkJsonJvmSqlHarnessTest::dynamicPaths, sql);
  }

  @Test
  void simpleJsonPredicateKeepsTheNativeFilter() throws Exception {
    String sql = "SELECT id FROM inputs WHERE JSON_EXISTS(s, '$.a')";
    String plan = tech.streamfusion.planner.NativePlanner.explain(dynamicPaths(), sql);
    org.junit.jupiter.api.Assertions.assertTrue(plan.contains("NativeFilter"), plan);
    NativeParity.assertParity(FlinkJsonJvmSqlHarnessTest::dynamicPaths, sql);
  }

  @Test
  void arrayInputUsesTheGeneratedBatchBridge() throws Exception {
    NativeParity.assertParity(
        () -> {
          var env = StreamExecutionEnvironment.getExecutionEnvironment();
          env.setParallelism(1);
          var table = StreamTableEnvironment.create(env);
          table.createTemporaryView(
              "inputs",
              env.fromData(
                  List.of(Row.of((Object) new String[] {"value", null})),
                  Types.ROW_NAMED(new String[] {"a"}, Types.OBJECT_ARRAY(Types.STRING))));
          return table;
        },
        "SELECT JSON_STRING(a) FROM inputs");
  }

  @Test
  void queryStringsCannotLoseIdentityBeforeGrouping() throws Exception {
    NativeParity.assertFallbackReasonContains(
        FlinkJsonJvmSqlHarnessTest::dynamicPaths,
        "SELECT JSON_QUERY(s, '$.a'), COUNT(*) FROM inputs GROUP BY JSON_QUERY(s, '$.a')",
        "JSON string identity requires a final projection");
  }

  @Test
  void mixedJsonCalcExecutesAcrossBatchesAndFiltersBeforeEvaluation() throws Exception {
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
                          id % 3 == 0 ? "invalid" : "{\"a\":[1,2,3],\"n\":" + id + "}",
                          new String[] {id.toString(), null}))
              .returns(
                  Types.ROW_NAMED(
                      new String[] {"id", "s", "a"},
                      Types.LONG,
                      Types.STRING,
                      Types.OBJECT_ARRAY(Types.STRING))));
      String sql =
          "SELECT id, JSON_QUERY(s, '$.a[0:2]' ERROR ON ERROR), JSON_VALUE(s, '$.n' ERROR ON"
              + " ERROR), JSON_STRING(a), a FROM inputs WHERE MOD(id, 3) <> 0";
      var scan = tech.streamfusion.planner.NativePlanner.install(table);
      assertTrue(table.explainSql(sql).contains("jsonEvaluation=[JVM]"));
      var result = table.executeSql(sql);
      try (var rows = result.collect()) {
        for (long id = 0; id < 5003; id++) {
          if (id % 3 != 0)
            assertEquals(
                Row.of(
                    id,
                    "[1,2]",
                    Long.toString(id),
                    "[\"" + id + "\",null]",
                    new String[] {Long.toString(id), null}),
                rows.next());
        }
        org.junit.jupiter.api.Assertions.assertFalse(rows.hasNext());
      }
      assertTrue(scan.fallbackReasons().isEmpty(), scan.explainSummary());
      var groups =
          reporter.findOperatorMetricGroups(
              result.getJobClient().orElseThrow().getJobID(), "(?i)NativeCalcExecNode");
      assertEquals(1, groups.size());
      var metrics = reporter.getMetricsByGroup(groups.iterator().next());
      assertEquals(5003, ((Counter) metrics.get("numRecordsIn")).getCount());
      assertEquals(3335, ((Counter) metrics.get("numRecordsOut")).getCount());
    } finally {
      cluster.after();
    }
  }

  private static TableEnvironment dynamicPaths() {
    var env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    var table = StreamTableEnvironment.create(env);
    var rows = new ArrayList<Row>();
    for (String path :
        new String[] {
          null,
          "$",
          "$.a",
          "$.a[0,2,-1]",
          "$.a[0,0]",
          "$.a[:2]",
          "$.a[-2:]",
          "$..a",
          "$['a','b']",
          "$.a[?(@.x > 0)].x",
          "$.a.length()",
          "$.a[0:2:3]",
          "$['\\uGGGG']",
          "invalid"
        }) {
      for (String mode : new String[] {"strict ", "lax "}) {
        for (String document :
            new String[] {
              null,
              "invalid",
              "null",
              "[]",
              "[1,2,3]",
              "{}",
              "{\"a\":null}",
              "{\"a\":1}",
              "{\"a\":[1,2,3],\"b\":false}",
              "{\"a\":[{\"x\":1},{\"x\":-1},null],\"b\":\"value\"}",
              "{\"a\":{\"a\":\"nested\"}}",
              "{\"a\":1,\"a\":2}",
              "{\"a\":[],\"bad\":[}"
            }) {
          rows.add(Row.of(rows.size(), document, path == null ? null : mode + path));
        }
      }
    }
    table.createTemporaryView(
        "inputs",
        env.fromData(
            rows,
            Types.ROW_NAMED(new String[] {"id", "s", "p"}, Types.INT, Types.STRING, Types.STRING)));
    return table;
  }
}
