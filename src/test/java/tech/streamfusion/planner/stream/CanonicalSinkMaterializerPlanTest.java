package tech.streamfusion.planner.stream;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Path;
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
import org.example.canonical.CanonicalFixtureFactory;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import tech.streamfusion.NativePlannerTestEnvironment;
import tech.streamfusion.operator.NativeSinkMaterializer;
import tech.streamfusion.state.StockSinkMaterializerStateContract;

class CanonicalSinkMaterializerPlanTest {
  @TempDir Path directory;

  static Stream<Arguments> cases() {
    return strategies().flatMap(strategy -> Stream.of(false, true)
        .flatMap(compiled -> Stream.of("DISABLED", "ALWAYS")
            .flatMap(uids -> Stream.of(false, true)
                .map(inputKey -> Arguments.of(strategy, compiled, uids, inputKey)))));
  }

  static Stream<String> strategies() {
    return tech.streamfusion.compat.MaterializerStateCompat.supportsV2()
        ? Stream.of("LEGACY", "VALUE", "MAP", "ADAPTIVE") : Stream.of("LEGACY");
  }

  @ParameterizedTest
  @MethodSource("cases")
  void stockSinkKeepsItsHelpersWhileNativeMaterializationBindsOriginalStateAndIdentity(
      String strategy, boolean compiled, String uids, boolean inputKey) throws Exception {
    assertPlan(strategy, compiled, uids, inputKey, "BIGINT", "BIGINT");
  }

  static Stream<Arguments> typedCases() {
    return tech.streamfusion.CanonicalTestMatrix.select(
        tech.streamfusion.CanonicalMaterializerTypeFixture.shapes()
            .flatMap(shape -> strategies().flatMap(strategy -> Stream.of(false, true)
                .flatMap(compiled -> Stream.of(false, true)
                    .map(inputKey -> Arguments.of(shape, strategy, compiled, inputKey))))));
  }

  @ParameterizedTest
  @MethodSource("typedCases")
  void scalarKeysAndExistingPayloadTypesBindTheirOriginalMaterializer(
      String shape, String strategy, boolean compiled, boolean inputKey) throws Exception {
    String payload = tech.streamfusion.CanonicalMaterializerTypeFixture.sql(shape);
    assertPlan(strategy, compiled, "DISABLED", inputKey,
        shape.startsWith("scalar-") ? payload : "BIGINT", payload);
  }

  private void assertPlan(String strategy, boolean compiled, String uids, boolean inputKey,
      String keyType, String payloadType) throws Exception {
    var config = new Configuration();
    config.setString("streamfusion.native.enabled", "true");
    config.setString("parallelism.default", "2");
    config.setString("pipeline.max-parallelism", "257");
    config.setString("table.exec.uid.generation", uids);
    config.setString("table.exec.sink.upsert-materialize", "FORCE");
    config.setString("table.exec.sink.upsert-materialize-strategy.type", strategy);
    config.setString("table.exec.state.ttl", "1000 ms");
    var table = NativePlannerTestEnvironment.create(config, CanonicalFixtureFactory.class);
    String path = directory.toString().replace("'", "''");
    table.executeSql("CREATE TABLE materializer_input(k " + keyType + " NOT NULL,v " + payloadType + ",n BIGINT"
        + (inputKey ? ",PRIMARY KEY(k,n) NOT ENFORCED" : "")
        + ") WITH ('connector'='canonical-fixture','input'='" + path + "/input')");
    table.executeSql("CREATE TABLE materializer_output(k " + keyType + ",v " + payloadType + ",n BIGINT,"
        + "PRIMARY KEY(k) NOT ENFORCED) WITH ('connector'='canonical-fixture',"
        + "'output'='" + path + "/output','upsert'='true')");
    String query = "INSERT INTO materializer_output SELECT k,v,n FROM materializer_input";
    var planner = (PlannerBase) ((TableEnvironmentImpl) table).getPlanner();
    var plan = compiled ? table.compilePlanSql(query) : null;
    String portable = compiled ? plan.asJsonString() : "";
    var transforms = compiled
        ? planner.translatePlan(planner.loadPlan(PlanReference.fromJsonString(portable)))
        : CanonicalRegularJoinPlanTest.translate(planner, query);
    var graph = CanonicalRegularJoinPlanTest.graph(planner, table, transforms);
    var owners = graph.getStreamNodes().stream()
        .filter(node -> node.getOperatorFactory() instanceof SimpleOperatorFactory
            && ((SimpleOperatorFactory<?>) node.getOperatorFactory()).getOperator()
                instanceof NativeSinkMaterializer)
        .collect(java.util.stream.Collectors.toList());
    assertEquals(1, owners.size(), graph.getStreamingPlanAsJSON());
    var owner = owners.get(0);
    var operator = ((SimpleOperatorFactory<?>) owner.getOperatorFactory()).getOperator();
    var contract = assertInstanceOf(StockSinkMaterializerStateContract.class,
        GroupAggregateStockTestFixture.field(operator, "stockStateContract"));
    assertEquals(strategy, GroupAggregateStockTestFixture.field(contract, "strategy").toString());
    assertEquals(strategy.equals("LEGACY") ? 1000L : 0L,
        GroupAggregateStockTestFixture.field(contract, "ttl"));
    assertEquals(257, tech.streamfusion.compat.FlinkPlanCompat.maximumParallelism(owner));
    assertEquals(uids.equals("DISABLED"), owner.getTransformationUID() == null);
    Map<?, ?> registrations = InstantiationUtil.deserializeObject(
        Base64.getDecoder().decode(table.getConfig().getConfiguration()
            .getString("streamfusion.canonical.state-contracts", "")), getClass().getClassLoader());
    var hashes = new StreamGraphHasherV2().traverseStreamGraphAndGenerateHashes(graph);
    assertEquals(1, registrations.size());
    assertTrue(registrations.containsKey(new OperatorID(hashes.get(owner.getId())).toString()));
    assertEquals("true", table.getConfig().getConfiguration()
        .getString("streamfusion.canonical.metadata-projection-active", "false"));
    assertFalse(portable.contains("tech.streamfusion"));
    if (compiled) assertEquals(portable, plan.asJsonString());
    table.getConfig().set("streamfusion.native.enabled", "false");
    assertFalse(CanonicalRegularJoinPlanTest.translate(planner, query).isEmpty());
    assertEquals("", table.getConfig().getConfiguration()
        .getString("streamfusion.canonical.state-contracts", ""));
  }
}
