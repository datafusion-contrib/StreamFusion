package tech.streamfusion.planner;

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
import tech.streamfusion.operator.NativeColumnarKeepLastDeduplicateOperator;
import tech.streamfusion.state.StockEmptyDedupStateContract;

class CanonicalBareUpdateDedupPlanTest {
  static Stream<Arguments> cases() {
    return Stream.of("proc-last")
        .flatMap(
            mode ->
                Stream.of(false, true)
                    .flatMap(
                        mini ->
                            Stream.of(false, true)
                                .flatMap(
                                    compiled ->
                                        Stream.of(false, true)
                                            .map(ttl -> Arguments.of(mode, compiled, mini, ttl)))))
        .filter(test -> !test.get()[0].equals("row-first") || (boolean) test.get()[2]);
  }

  @ParameterizedTest
  @MethodSource("cases")
  void normalSubmissionCapturesTheActualReleasedDeduplicator(
      String mode, boolean compiled, boolean mini, boolean ttl) throws Exception {
    var config = new Configuration();
    config.setString("table.exec.deduplicate.insert-update-after-sensitive-enabled", "false");
    config.setString("streamfusion.native.enabled", "true");
    config.setString("parallelism.default", "2");
    config.setString("pipeline.max-parallelism", "257");
    config.setString("table.exec.uid.generation", "DISABLED");
    config.setString("table.exec.state.ttl", ttl ? "1 h" : "0 ms");
    config.setString("table.exec.mini-batch.enabled", Boolean.toString(mini));
    config.setString("table.exec.mini-batch.size", "3");
    config.setString("table.exec.mini-batch.allow-latency", "1 h");
    var table =
        NativePlannerTestEnvironment.create(
            config, org.example.canonical.CanonicalFixtureFactory.class);
    table.executeSql(
        "CREATE TABLE input_table(k BIGINT,v BIGINT,pt AS PROCTIME(),"
            + "rt AS TO_TIMESTAMP_LTZ(v,3),WATERMARK FOR rt AS rt - INTERVAL '1' HOUR)"
            + " WITH ('connector'='canonical-fixture','insert-only'='true')");
    table.executeSql(
        "CREATE TABLE output_table(k BIGINT,v BIGINT,n BIGINT,PRIMARY KEY(k) NOT ENFORCED) WITH"
            + " ('connector'='canonical-fixture','upsert'='true')");
    String query =
        "INSERT INTO output_table SELECT k,v,k FROM (SELECT *,ROW_NUMBER() OVER"
            + " (PARTITION BY k ORDER BY "
            + (mode.startsWith("proc") ? "pt" : "rt")
            + (mode.endsWith("last") ? " DESC" : " ASC")
            + ") AS rn FROM input_table) WHERE rn=1";
    var planner = (PlannerBase) ((TableEnvironmentImpl) table).getPlanner();
    var plan = compiled ? table.compilePlanSql(query) : null;
    String json = plan == null ? "" : plan.asJsonString();
    assertFalse(json.contains("tech.streamfusion"));
    if (compiled) {
      table.getConfig().set("table.exec.state.ttl", ttl ? "0 ms" : "1 h");
      table.getConfig().set("table.exec.deduplicate.insert-update-after-sensitive-enabled", "true");
    }
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
                            instanceof NativeColumnarKeepLastDeduplicateOperator)
            .collect(java.util.stream.Collectors.toList());
    assertEquals(1, owners.size(), graph.getStreamingPlanAsJSON());
    var owner = owners.get(0);
    assertEquals(257, owner.getMaxParallelism());
    assertNull(owner.getTransformationUID());
    var operator = ((SimpleOperatorFactory<?>) owner.getOperatorFactory()).getOperator();
    for (var entry :
        Map.<String, Object>of(
                "stateTtlMillis",
                ttl ? 3600000L : 0L,
                "miniBatch",
                mini && !mode.equals("proc-first"),
                "rowtimeOrdered",
                mode.startsWith("row"),
                "keepFirst",
                mode.endsWith("first"),
                "generateInsert",
                false,
                "generateUpdateBefore",
                false)
            .entrySet()) {
      var field = NativeColumnarKeepLastDeduplicateOperator.class.getDeclaredField(entry.getKey());
      field.setAccessible(true);
      assertEquals(entry.getValue(), field.get(operator));
    }
    var field = AbstractNativeStatefulOperator.class.getDeclaredField("stockStateContract");
    field.setAccessible(true);
    assertInstanceOf(StockEmptyDedupStateContract.class, field.get(operator));
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
