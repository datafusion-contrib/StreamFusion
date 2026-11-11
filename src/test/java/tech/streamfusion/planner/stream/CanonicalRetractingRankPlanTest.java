package tech.streamfusion.planner.stream;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.Base64;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.streaming.api.graph.StreamGraphHasherV2;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.table.api.PlanReference;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.util.InstantiationUtil;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import tech.streamfusion.NativePlannerTestEnvironment;
import tech.streamfusion.operator.AbstractNativeStatefulOperator;
import tech.streamfusion.operator.NativeColumnarTopNOperator;
import tech.streamfusion.state.StockRetractingTopNStateContract;

class CanonicalRetractingRankPlanTest {
  static Stream<Arguments> cases() {
    var result = new ArrayList<Arguments>();
    for (String mode :
        java.util.List.of(
            "one",
            "range",
            "offset",
            "variable",
            "ttl",
            "large",
            "offset-ttl",
            "variable-ttl",
            "variable-int"))
      for (boolean compiled : new boolean[] {false, true})
        for (boolean keyless : new boolean[] {false, true})
          for (boolean rank : new boolean[] {false, true})
            for (boolean twoSorts : new boolean[] {false, true})
              result.add(Arguments.of(mode, compiled, keyless, rank, twoSorts));
    return result.stream();
  }

  @ParameterizedTest(name = "{0}, compiled={1}, keyless={2}, rank={3}, two-sorts={4}")
  @MethodSource("cases")
  void normalSubmissionBindsActualReleasedRetractingStateAndIdentities(
      String mode, boolean compiled, boolean keyless, boolean rank, boolean twoSorts)
      throws Exception {
    long limit = mode.equals("one") ? 1 : mode.equals("large") ? 2147483648L : 3;
    var config = new Configuration();
    config.setString("streamfusion.native.enabled", "true");
    config.setString("parallelism.default", "2");
    config.setString("pipeline.max-parallelism", "257");
    config.setString("table.exec.uid.generation", "DISABLED");
    config.setString("table.exec.state.ttl", mode.endsWith("ttl") ? "1 h" : "0 ms");
    var table = NativePlannerTestEnvironment.create(config);
    table.executeSql("CREATE TABLE input_table(k BIGINT,v BIGINT) WITH ('connector'='datagen')");
    table.executeSql(
        "CREATE TABLE output_table(k BIGINT,v BIGINT,n BIGINT) WITH ('connector'='blackhole')");
    table.getConfig().set("table.exec.mini-batch.enabled", "true");
    table.getConfig().set("table.exec.mini-batch.allow-latency", "1 s");
    table.getConfig().set("table.exec.mini-batch.size", "3");
    String query =
        "INSERT INTO output_table SELECT k,v,"
            + (rank ? "rn" : "CAST(0 AS BIGINT)")
            + " FROM (SELECT *,ROW_NUMBER() OVER ("
            + (keyless ? "" : "PARTITION BY k ")
            + "ORDER BY c ASC"
            + (twoSorts ? ",v DESC NULLS LAST" : "")
            + ") AS rn FROM (SELECT k,v,COUNT(*) AS c"
            + (mode.startsWith("variable")
                ? (mode.equals("variable-int")
                    ? ",CAST(COALESCE(MOD(ABS(k),3)+1,2) AS INT) AS b"
                    : ",COALESCE(MOD(ABS(k),3)+1,2) AS b")
                : "")
            + " FROM input_table GROUP BY k,v)) WHERE "
            + (mode.startsWith("offset") ? "rn>=2 AND " : "")
            + "rn<="
            + (mode.startsWith("variable") ? "b" : Long.toString(limit));
    var planner = (PlannerBase) ((TableEnvironmentImpl) table).getPlanner();
    var plan = compiled ? table.compilePlanSql(query) : null;
    String json = plan == null ? "" : plan.asJsonString();
    assertFalse(json.contains("tech.streamfusion"));
    var transformations =
        compiled
            ? planner.translatePlan(planner.loadPlan(PlanReference.fromJsonString(json)))
            : CanonicalRegularJoinPlanTest.translate(planner, query);
    var graph = CanonicalRegularJoinPlanTest.graph(planner, table, transformations);
    var owners =
        graph.getStreamNodes().stream()
            .filter(
                node ->
                    node.getOperatorFactory() instanceof SimpleOperatorFactory
                        && ((SimpleOperatorFactory<?>) node.getOperatorFactory()).getOperator()
                            instanceof NativeColumnarTopNOperator)
            .collect(java.util.stream.Collectors.toList());
    assertEquals(1, owners.size(), graph.getStreamingPlanAsJSON());
    var owner = owners.get(0);
    assertEquals(
        keyless ? 1 : 257, tech.streamfusion.compat.FlinkPlanCompat.maximumParallelism(owner));
    assertEquals(keyless ? 1 : 2, owner.getParallelism());
    assertNull(owner.getTransformationUID());
    var operator = ((SimpleOperatorFactory<?>) owner.getOperatorFactory()).getOperator();
    for (var expected :
        Map.<String, Object>of(
                "limit",
                mode.startsWith("variable") ? 100L : limit,
                "retracting",
                true,
                "netDiff",
                false,
                "offset",
                mode.startsWith("offset") ? 1L : 0L,
                "outputRankNumber",
                rank && limit != 1,
                "stateTtlMillis",
                mode.endsWith("ttl") ? 3600000L : 0L)
            .entrySet()) {
      var field = NativeColumnarTopNOperator.class.getDeclaredField(expected.getKey());
      field.setAccessible(true);
      assertEquals(expected.getValue(), field.get(operator), expected.getKey());
    }
    var field = AbstractNativeStatefulOperator.class.getDeclaredField("stockStateContract");
    field.setAccessible(true);
    var contract = assertInstanceOf(StockRetractingTopNStateContract.class, field.get(operator));
    assertEquals(
        mode.startsWith("variable")
            ? java.util.Set.of("data-state", "sorted-map", "rankEnd")
            : java.util.Set.of("data-state", "sorted-map"),
        contract.stateDescriptors().keySet());
    Map<?, ?> registrations =
        InstantiationUtil.deserializeObject(
            Base64.getDecoder()
                .decode(
                    table
                        .getConfig()
                        .getConfiguration()
                        .getString("streamfusion.canonical.state-contracts", "")),
            getClass().getClassLoader());
    CanonicalRankPlanBindings.verifyAggregateAndRank(graph, registrations);
    var hashes = new StreamGraphHasherV2().traverseStreamGraphAndGenerateHashes(graph);
    assertTrue(registrations.containsKey(new OperatorID(hashes.get(owner.getId())).toString()));
    assertTrue(
        Boolean.parseBoolean(
            table
                .getConfig()
                .getConfiguration()
                .getString("streamfusion.canonical.metadata-projection-active", "false")));
    if (compiled) assertEquals(json, plan.asJsonString());
  }
}
