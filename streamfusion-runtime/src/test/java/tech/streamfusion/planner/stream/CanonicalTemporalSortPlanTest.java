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
import org.apache.flink.table.runtime.operators.sort.RowTimeSortOperator;
import org.apache.flink.util.InstantiationUtil;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import tech.streamfusion.NativePlannerTestEnvironment;
import tech.streamfusion.operator.NativeColumnarTemporalSortOperator;
import tech.streamfusion.state.StockTemporalSortStateContract;

class CanonicalTemporalSortPlanTest {
  static Stream<Arguments> cases() {
    return Stream.of("BIGINT", "DOUBLE", "VARCHAR", "DECIMAL(38,2)")
        .flatMap(
            type ->
                Stream.of(
                        "TIMESTAMP(0)",
                        "TIMESTAMP(1)",
                        "TIMESTAMP(2)",
                        "TIMESTAMP(3)",
                        "TIMESTAMP_LTZ(0)",
                        "TIMESTAMP_LTZ(1)",
                        "TIMESTAMP_LTZ(2)",
                        "TIMESTAMP_LTZ(3)")
                    .flatMap(
                        clock ->
                            Stream.of(false, true)
                                .flatMap(
                                    compiled ->
                                        Stream.of("DISABLED", "ALWAYS")
                                            .flatMap(
                                                uids ->
                                                    Stream.of(1, 128)
                                                        .map(
                                                            maximum ->
                                                                Arguments.of(
                                                                    type, clock, compiled, uids,
                                                                    maximum))))));
  }

  @ParameterizedTest
  @MethodSource("cases")
  void ordinaryAndPortablePlansPreserveSingletonStateAndStockIdentity(
      String type, String clock, boolean compiled, String uids, int maximum) throws Exception {
    var config = new Configuration();
    config.setString("streamfusion.native.enabled", "true");
    config.setString("table.exec.uid.generation", uids);
    config.setString("parallelism.default", maximum == 1 ? "1" : "3");
    config.setString("pipeline.max-parallelism", Integer.toString(maximum));
    var table = NativePlannerTestEnvironment.create(config);
    table.getConfig().set("streamfusion.native.enabled", "false");
    table.executeSql(
        "CREATE TABLE sort_input(k BIGINT,v "
            + type
            + ",rt "
            + clock
            + ",WATERMARK FOR rt AS rt) WITH ('connector'='datagen')");
    table.executeSql(
        "CREATE TABLE sort_output(k BIGINT,v "
            + type
            + ",rt "
            + clock
            + ") WITH ('connector'='blackhole')");
    String query = "INSERT INTO sort_output SELECT k,v,rt FROM sort_input ORDER BY rt";
    var plan = table.compilePlanSql(query);
    String portable = plan.asJsonString();
    assertTrue(portable.contains("stream-exec-temporal-sort"));
    assertFalse(portable.contains("tech.streamfusion"));
    var planner = (PlannerBase) ((TableEnvironmentImpl) table).getPlanner();
    var baseline =
        CanonicalRegularJoinPlanTest.graph(
            planner,
            table,
            planner.translatePlan(planner.loadPlan(PlanReference.fromJsonString(portable))));
    var original =
        baseline.getStreamNodes().stream()
            .filter(
                node ->
                    node.getOperatorFactory() instanceof SimpleOperatorFactory
                        && ((SimpleOperatorFactory<?>) node.getOperatorFactory()).getOperator()
                            instanceof RowTimeSortOperator)
            .findFirst()
            .orElseThrow();
    table.getConfig().set("streamfusion.native.enabled", "true");
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
                            instanceof NativeColumnarTemporalSortOperator)
            .collect(java.util.stream.Collectors.toList());
    assertEquals(1, owners.size(), graph.getStreamingPlanAsJSON());
    var owner = owners.get(0);
    var operator = ((SimpleOperatorFactory<?>) owner.getOperatorFactory()).getOperator();
    var contract = GroupAggregateStockTestFixture.field(operator, "stockStateContract");
    assertInstanceOf(StockTemporalSortStateContract.class, contract);
    assertEquals(2, GroupAggregateStockTestFixture.field(contract, "timeColumn"));
    assertEquals(
        tech.streamfusion.compat.FlinkPlanCompat.maximumParallelism(original),
        tech.streamfusion.compat.FlinkPlanCompat.maximumParallelism(owner));
    assertEquals(1, owner.getParallelism());
    assertEquals(uids.equals("DISABLED"), owner.getTransformationUID() == null);
    if (compiled) assertEquals(original.getTransformationUID(), owner.getTransformationUID());
    assertTrue(graph.getStreamingPlanAsJSON().contains("RowDataToArrow"));
    assertTrue(graph.getStreamingPlanAsJSON().contains("ArrowToRowData"));
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
    assertEquals(1, registrations.size());
    assertTrue(registrations.containsKey(new OperatorID(hashes.get(owner.getId())).toString()));
    assertTrue(
        Boolean.parseBoolean(
            table
                .getConfig()
                .getConfiguration()
                .getString("streamfusion.canonical.metadata-projection-active", "false")));
    assertEquals(portable, plan.asJsonString());
    table.getConfig().set("streamfusion.native.enabled", "false");
    assertFalse(CanonicalRegularJoinPlanTest.translate(planner, query).isEmpty());
    assertEquals(
        "",
        table
            .getConfig()
            .getConfiguration()
            .getString("streamfusion.canonical.state-contracts", ""));
  }
}
