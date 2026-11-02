package tech.streamfusion.planner;

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
import tech.streamfusion.operator.NativeColumnarFirstNOperator;
import tech.streamfusion.state.StockFirstNStateContract;

class CanonicalFirstNPlanTest {
  static Stream<Arguments> cases() {
    var cases = new ArrayList<Arguments>();
    for (int limit : new int[] {2, 5})
      for (boolean compiled : new boolean[] {false, true})
        for (boolean keyless : new boolean[] {false, true})
          for (boolean rank : new boolean[] {false, true})
            for (boolean ttl : new boolean[] {false, true})
              cases.add(Arguments.of(limit, compiled, keyless, rank, ttl));
    return cases.stream();
  }

  @ParameterizedTest
  @MethodSource("cases")
  void normalSubmissionCapturesTheReleasedCounterContract(
      int limit, boolean compiled, boolean keyless, boolean rank, boolean ttl) throws Exception {
    var config = new Configuration();
    config.setString("streamfusion.native.enabled", "true");
    config.setString("parallelism.default", "2");
    config.setString("pipeline.max-parallelism", "257");
    config.setString("table.exec.uid.generation", "DISABLED");
    config.setString("table.exec.state.ttl", ttl ? "1 h" : "0 ms");
    var table = NativePlannerTestEnvironment.create(config);
    table.executeSql(
        "CREATE TABLE input_table(k BIGINT,v BIGINT,pt AS PROCTIME()) WITH"
            + " ('connector'='datagen')");
    table.executeSql(
        "CREATE TABLE output_table(k BIGINT,v BIGINT,n BIGINT) WITH ('connector'='blackhole')");
    String query =
        "INSERT INTO output_table SELECT k,v,"
            + (rank ? "rn" : "CAST(0 AS BIGINT)")
            + " FROM (SELECT k,v,ROW_NUMBER() OVER ("
            + (keyless ? "" : "PARTITION BY k")
            + " ORDER BY pt) AS rn FROM input_table) WHERE rn<="
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
                            instanceof NativeColumnarFirstNOperator)
            .collect(java.util.stream.Collectors.toList());
    assertEquals(1, owners.size(), graph.getStreamingPlanAsJSON());
    var owner = owners.get(0);
    assertEquals(keyless ? 1 : 257, owner.getMaxParallelism());
    assertEquals(keyless ? 1 : 2, owner.getParallelism());
    assertNull(owner.getTransformationUID());
    var operator = ((SimpleOperatorFactory<?>) owner.getOperatorFactory()).getOperator();
    assertInstanceOf(NativeColumnarFirstNOperator.class, operator);
    for (var expected :
        Map.<String, Object>of("limit", limit, "outputRank", rank, "ttlMillis", ttl ? 3600000L : 0L)
            .entrySet()) {
      var field = NativeColumnarFirstNOperator.class.getDeclaredField(expected.getKey());
      field.setAccessible(true);
      assertEquals(expected.getValue(), field.get(operator));
    }
    var field = AbstractNativeStatefulOperator.class.getDeclaredField("stockStateContract");
    field.setAccessible(true);
    assertInstanceOf(StockFirstNStateContract.class, field.get(operator));
    Map<?, ?> registrations =
        InstantiationUtil.deserializeObject(
            Base64.getDecoder()
                .decode(
                    table
                        .getConfig()
                        .getConfiguration()
                        .getString("streamfusion.canonical.state-contracts", "")),
            getClass().getClassLoader());
    assertEquals(1, registrations.size());
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
