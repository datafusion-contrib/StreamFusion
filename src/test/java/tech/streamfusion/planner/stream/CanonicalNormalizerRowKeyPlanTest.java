package tech.streamfusion.planner.stream;

import static org.junit.jupiter.api.Assertions.*;

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
import tech.streamfusion.operator.NativeColumnarChangelogNormalizeOperator;
import tech.streamfusion.state.StockChangelogNormalizeStateContract;

class CanonicalNormalizerRowKeyPlanTest {
  static Stream<Arguments> cases() {
    return Stream.of(false, true)
        .flatMap(
            compiled ->
                Stream.of(false, true)
                    .flatMap(
                        mini ->
                            Stream.of(false, true)
                                .flatMap(
                                    ttl ->
                                        Stream.of(false, true)
                                            .flatMap(
                                                nested ->
                                                    Stream.of("ALWAYS", "DISABLED")
                                                        .map(
                                                            uid ->
                                                                Arguments.of(
                                                                    compiled, mini, ttl, nested,
                                                                    uid))))));
  }

  @ParameterizedTest
  @MethodSource("cases")
  void normalSubmissionCapturesTheActualReleasedNormalizer(
      boolean compiled, boolean mini, boolean ttl, boolean nested, String uid) throws Exception {
    var config = new Configuration();
    config.setString("streamfusion.native.enabled", "true");
    config.setString("parallelism.default", "2");
    config.setString("table.optimizer.agg-phase-strategy", "ONE_PHASE");
    config.setString("pipeline.max-parallelism", "257");
    config.setString("table.exec.uid.generation", uid);
    config.setString("table.exec.state.ttl", ttl ? "1 h" : "0 ms");
    config.setString("table.exec.mini-batch.enabled", Boolean.toString(mini));
    config.setString("table.exec.mini-batch.size", "3");
    config.setString("table.exec.mini-batch.allow-latency", "1 h");
    var table =
        NativePlannerTestEnvironment.create(
            config, org.example.canonical.CanonicalFixtureFactory.class);
    table.executeSql(
        "CREATE TABLE upserts(k "
            + (nested
                ? "ROW<i BIGINT,n ROW<s VARCHAR(100),t TIMESTAMP(9)>,d DECIMAL(38,2)>"
                : "ROW<i BIGINT,s VARCHAR(100)>")
            + " NOT NULL,v BIGINT,PRIMARY KEY(k) NOT ENFORCED) WITH"
            + " ('connector'='canonical-fixture','upsert'='true')");
    table.executeSql("CREATE TABLE rights(k BIGINT,v BIGINT) WITH ('connector'='datagen')");
    table.executeSql("CREATE TABLE output_table(v BIGINT,c BIGINT) WITH ('connector'='blackhole')");
    String query = "INSERT INTO output_table SELECT v,COUNT(*) FROM upserts GROUP BY v";
    var planner = (PlannerBase) ((TableEnvironmentImpl) table).getPlanner();
    var plan = compiled ? table.compilePlanSql(query) : null;
    String json = plan == null ? "" : plan.asJsonString();
    assertFalse(json.contains("tech.streamfusion"));
    if (compiled) table.getConfig().set("table.exec.state.ttl", ttl ? "0 ms" : "1 h");
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
                            instanceof NativeColumnarChangelogNormalizeOperator)
            .collect(java.util.stream.Collectors.toList());
    assertEquals(1, owners.size(), graph.getStreamingPlanAsJSON());
    var owner = owners.get(0);
    assertEquals(257, tech.streamfusion.compat.FlinkPlanCompat.maximumParallelism(owner));
    if (uid.equals("DISABLED")) assertNull(owner.getTransformationUID());
    else assertNotNull(owner.getTransformationUID());
    var operator = ((SimpleOperatorFactory<?>) owner.getOperatorFactory()).getOperator();
    for (var entry :
        Map.<String, Object>of("stateTtlMillis", ttl ? 3600000L : 0L, "miniBatch", mini)
            .entrySet()) {
      var field = NativeColumnarChangelogNormalizeOperator.class.getDeclaredField(entry.getKey());
      field.setAccessible(true);
      assertEquals(entry.getValue(), field.get(operator));
    }
    var field = AbstractNativeStatefulOperator.class.getDeclaredField("stockStateContract");
    field.setAccessible(true);
    var contract =
        assertInstanceOf(StockChangelogNormalizeStateContract.class, field.get(operator));
    var keyType = StockChangelogNormalizeStateContract.class.getDeclaredField("keyType");
    keyType.setAccessible(true);
    assertInstanceOf(
        org.apache.flink.table.types.logical.RowType.class,
        ((org.apache.flink.table.types.logical.RowType) keyType.get(contract)).getTypeAt(0));
    Map<?, ?> registrations =
        InstantiationUtil.deserializeObject(
            Base64.getDecoder()
                .decode(
                    table
                        .getConfig()
                        .getConfiguration()
                        .getString("streamfusion.canonical.state-contracts", "")),
            getClass().getClassLoader());
    assertEquals(mini ? 1 : 2, registrations.size());
    if (mini)
      assertTrue(
          graph.getStreamNodes().stream()
              .anyMatch(
                  node ->
                      node.getOperatorFactory() instanceof SimpleOperatorFactory
                          && ((SimpleOperatorFactory<?>) node.getOperatorFactory()).getOperator()
                              instanceof
                              org.apache.flink.table.runtime.operators.bundle
                                  .KeyedMapBundleOperator),
          "The neighboring retracting aggregate must retain its stock state owner");
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
