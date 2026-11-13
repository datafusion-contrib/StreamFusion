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
import tech.streamfusion.state.StockUpdatingTopNStateContract;

class CanonicalUpdateRankPlanTest {
  static Stream<Arguments> cases() {
    var cases = new ArrayList<Arguments>();
    for (int limit : new int[] {1, 3})
      for (boolean compiled : new boolean[] {false, true})
        for (boolean keyless : new boolean[] {false, true})
          for (boolean rank : new boolean[] {false, true})
            for (boolean twoSorts : new boolean[] {false, true})
              cases.add(Arguments.of(limit, compiled, keyless, rank, twoSorts));
    return cases.stream();
  }

  @ParameterizedTest
  @MethodSource("cases")
  void normalSubmissionCapturesTheReleasedRankContract(
      int limit, boolean compiled, boolean keyless, boolean rank, boolean twoSorts)
      throws Exception {
    var config = new Configuration();
    config.setString("streamfusion.native.enabled", "true");
    config.setString("parallelism.default", "2");
    config.setString("pipeline.max-parallelism", "257");
    config.setString("table.exec.uid.generation", "DISABLED");
    config.setString("table.exec.state.ttl", "0 ms");
    var table = NativePlannerTestEnvironment.create(config);
    table.executeSql(
        "CREATE TABLE input_table(k BIGINT,v BIGINT) WITH" + " ('connector'='datagen')");
    table.executeSql(
        "CREATE TABLE output_table(k BIGINT,v BIGINT,n BIGINT) WITH ('connector'='blackhole')");
    table.getConfig().set("table.exec.mini-batch.enabled", "true");
    table.getConfig().set("table.exec.mini-batch.allow-latency", "1 s");
    table.getConfig().set("table.exec.mini-batch.size", "3");
    String query =
        "INSERT INTO output_table SELECT k,v,"
            + (rank ? "rn" : "CAST(0 AS BIGINT)")
            + " FROM (SELECT k,v,ROW_NUMBER() OVER ("
            + (keyless ? "" : "PARTITION BY k")
            + " ORDER BY c DESC"
            + (twoSorts ? ",v ASC NULLS FIRST" : "")
            + ") AS rn FROM (SELECT k,v,COUNT(*) AS c FROM input_table"
            + " GROUP BY k,v)) WHERE rn<="
            + limit;
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
    assertInstanceOf(NativeColumnarTopNOperator.class, operator);
    for (var expected :
        Map.<String, Object>of(
                "limit",
                (long) limit,
                "outputRankNumber",
                rank && limit != 1,
                "stateTtlMillis",
                0L,
                "netDiff",
                false)
            .entrySet()) {
      var field = NativeColumnarTopNOperator.class.getDeclaredField(expected.getKey());
      field.setAccessible(true);
      assertEquals(expected.getValue(), field.get(operator), expected.getKey());
    }
    var field = AbstractNativeStatefulOperator.class.getDeclaredField("stockStateContract");
    field.setAccessible(true);
    var contract = assertInstanceOf(StockUpdatingTopNStateContract.class, field.get(operator));
    assertEquals(
        java.util.Set.of(limit == 1 ? "Top1-Rank-State" : "data-state-with-update"),
        contract.stateDescriptors().keySet());
    var rowKeys = NativeColumnarTopNOperator.class.getDeclaredField("rowKeyColumns");
    rowKeys.setAccessible(true);
    assertArrayEquals(new int[] {0, 1}, (int[]) rowKeys.get(operator));
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
