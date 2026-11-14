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
import org.example.canonical.CanonicalFixtureFactory;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import tech.streamfusion.NativePlannerTestEnvironment;
import tech.streamfusion.operator.CanonicalRankScalarFixture;
import tech.streamfusion.operator.NativeColumnarTopNOperator;
import tech.streamfusion.state.StockAppendTopNStateContract;
import tech.streamfusion.state.StockRetractingTopNStateContract;
import tech.streamfusion.state.StockUpdatingTopNStateContract;

class CanonicalRankScalarPlanTest {
  static Stream<Arguments> cases() {
    return tech.streamfusion.CanonicalTestMatrix.select(
        CanonicalRankScalarFixture.shapes()
            .flatMap(
                shape ->
                    Stream.of("append", "update", "retract")
                        .flatMap(
                            mode ->
                                Stream.of(false, true)
                                    .flatMap(
                                        compiled ->
                                            Stream.of("DISABLED", "ALWAYS")
                                                .map(
                                                    uids ->
                                                        Arguments.of(
                                                            shape, mode, compiled, uids))))));
  }

  @ParameterizedTest
  @MethodSource("cases")
  void existingScalarKeysSortsAndRetainedValuesKeepOriginalCanonicalIdentity(
      String shape, String mode, boolean compiled, String uids) throws Exception {
    var config = new Configuration();
    config.setString("streamfusion.native.enabled", "true");
    config.setString("parallelism.default", "2");
    config.setString("pipeline.max-parallelism", "257");
    config.setString("table.exec.uid.generation", uids);
    var table = NativePlannerTestEnvironment.create(config, CanonicalFixtureFactory.class);
    String type = CanonicalRankScalarFixture.sql(shape);
    table.executeSql(
        "CREATE TABLE input_table(k "
            + type
            + ",v "
            + type
            + ",id BIGINT,p BIGINT) WITH ('connector'='canonical-fixture','input'='unused.csv',"
            + "'insert-only'='"
            + !mode.equals("retract")
            + "')");
    table.executeSql(
        "CREATE TABLE output_table(k "
            + type
            + ",v "
            + type
            + ",n BIGINT) WITH ('connector'='blackhole')");
    String source =
        mode.equals("update")
            ? "(SELECT k,v,id,p,COUNT(*) AS c FROM input_table GROUP BY k,v,id,p)"
            : "input_table";
    String order = mode.equals("update") ? "c DESC,v ASC,id DESC" : "v ASC,p DESC,id ASC";
    String query =
        "INSERT INTO output_table SELECT k,v,rn FROM (SELECT *,ROW_NUMBER() OVER"
            + " (PARTITION BY k ORDER BY "
            + order
            + ") AS rn FROM "
            + source
            + ") WHERE rn<=3";
    var planner = (PlannerBase) ((TableEnvironmentImpl) table).getPlanner();
    var plan = compiled ? table.compilePlanSql(query) : null;
    String portable = compiled ? plan.asJsonString() : "";
    var transforms =
        compiled
            ? planner.translatePlan(planner.loadPlan(PlanReference.fromJsonString(portable)))
            : CanonicalRegularJoinPlanTest.translate(planner, query);
    var graph = CanonicalRegularJoinPlanTest.graph(planner, table, transforms);
    var owners =
        graph.getStreamNodes().stream()
            .filter(
                node ->
                    node.getOperatorFactory() instanceof SimpleOperatorFactory
                        && ((SimpleOperatorFactory<?>) node.getOperatorFactory()).getOperator()
                            instanceof NativeColumnarTopNOperator)
            .collect(java.util.stream.Collectors.toList());
    boolean floating = shape.equals("scalar-float") || shape.equals("scalar-double");
    if (floating && !mode.equals("update")) {
      for (var candidate : owners)
        assertNull(
            GroupAggregateStockTestFixture.field(
                ((SimpleOperatorFactory<?>) candidate.getOperatorFactory()).getOperator(),
                "stockStateContract"));
      if (compiled) assertTrue(owners.isEmpty(), graph.getStreamingPlanAsJSON());
      assertFalse(portable.contains("tech.streamfusion"));
      if (compiled) assertEquals(portable, plan.asJsonString());
      return;
    }
    assertEquals(1, owners.size(), graph.getStreamingPlanAsJSON());
    var owner = owners.get(0);
    var operator = ((SimpleOperatorFactory<?>) owner.getOperatorFactory()).getOperator();
    var contract = GroupAggregateStockTestFixture.field(operator, "stockStateContract");
    assertInstanceOf(
        mode.equals("append")
            ? StockAppendTopNStateContract.class
            : mode.equals("update")
                ? StockUpdatingTopNStateContract.class
                : StockRetractingTopNStateContract.class,
        contract);
    assertEquals(3, ((int[]) GroupAggregateStockTestFixture.field(operator, "sortIndices")).length);
    assertEquals(257, tech.streamfusion.compat.FlinkPlanCompat.maximumParallelism(owner));
    Map<?, ?> registrations =
        InstantiationUtil.deserializeObject(
            Base64.getDecoder()
                .decode(
                    table
                        .getConfig()
                        .getConfiguration()
                        .getString("streamfusion.canonical.state-contracts", "")),
            getClass().getClassLoader());
    var hashes = new StreamGraphHasherV2().traverseStreamGraphAndGenerateHashes(graph);
    assertTrue(registrations.containsKey(new OperatorID(hashes.get(owner.getId())).toString()));
    assertEquals(
        "true",
        table
            .getConfig()
            .getConfiguration()
            .getString("streamfusion.canonical.metadata-projection-active", "false"));
    assertFalse(portable.contains("tech.streamfusion"));
    if (compiled) assertEquals(portable, plan.asJsonString());
  }
}
