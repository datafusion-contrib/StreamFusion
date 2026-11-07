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
import tech.streamfusion.operator.NativeColumnarGroupAggregateOperator;
import tech.streamfusion.state.StockGroupTypedStateContract;

class CanonicalGroupTypedPlanTest {
  static Stream<Arguments> cases() {
    return Stream.of(false, true)
        .flatMap(
            compiled ->
                Stream.of(false, true)
                    .flatMap(
                        append ->
                            Stream.of(false, true)
                                .flatMap(
                                    shared ->
                                        Stream.of(false, true)
                                            .flatMap(
                                                notNull ->
                                                    Stream.of("DISABLED", "ALWAYS")
                                                        .flatMap(
                                                            uids ->
                                                                Stream.of("INT", "BIGINT")
                                                                    .map(
                                                                        sumType ->
                                                                            Arguments.of(
                                                                                compiled, append,
                                                                                shared, notNull,
                                                                                uids,
                                                                                sumType)))))));
  }

  @ParameterizedTest
  @MethodSource("cases")
  void realStockSchemasBindTypedAccumulatorsAndSharedViews(
      boolean compiled,
      boolean append,
      boolean shared,
      boolean notNull,
      String uids,
      String sumType)
      throws Exception {
    var config = new Configuration();
    config.setString("streamfusion.native.enabled", "true");
    config.setString("parallelism.default", "2");
    config.setString("pipeline.max-parallelism", "257");
    config.setString("table.exec.uid.generation", uids);
    var table = NativePlannerTestEnvironment.create(config);
    table.getConfig().set("table.exec.mini-batch.enabled", "false");
    table.getConfig().set("table.optimizer.agg-phase-strategy", "ONE_PHASE");
    table.registerCatalog(
        "fixture",
        new org.apache.flink.table.catalog.GenericInMemoryCatalog("fixture", "db") {
          @Override
          public java.util.Optional<org.apache.flink.table.factories.Factory> getFactory() {
            return java.util.Optional.of(new org.example.canonical.CanonicalFixtureFactory());
          }
        });
    table.useCatalog("fixture");
    table.executeSql(
        "CREATE TABLE input_table(k STRING,t TIMESTAMP(3),v "
            + sumType
            + ",s STRING"
            + (notNull ? " NOT NULL" : "")
            + ") WITH"
            + " ('connector'='canonical-fixture','input'='unused.csv','ready'='unused.ready','insert-only'='"
            + append
            + "')");
    table.useCatalog("default_catalog");
    table.executeSql(
        "CREATE TABLE output_table(k STRING,t TIMESTAMP(3),n BIGINT,total "
            + sumType
            + ",d BIGINT"
            + (shared ? ",f BIGINT" : "")
            + ") WITH ('connector'='blackhole')");
    String query =
        "INSERT INTO output_table SELECT k,t,COUNT(*),SUM(v),COUNT(DISTINCT s)"
            + (shared ? ",COUNT(DISTINCT s) FILTER(WHERE v>3)" : "")
            + " FROM fixture.db.input_table GROUP BY k,t";
    var planner = (PlannerBase) ((TableEnvironmentImpl) table).getPlanner();
    var plan = compiled ? table.compilePlanSql(query) : null;
    String portable = plan == null ? "" : plan.asJsonString();
    assertFalse(portable.contains("tech.streamfusion"));
    var transformations =
        compiled
            ? planner.translatePlan(planner.loadPlan(PlanReference.fromJsonString(portable)))
            : CanonicalRegularJoinPlanTest.translate(planner, query);
    var graph = CanonicalRegularJoinPlanTest.graph(planner, table, transformations);
    var owners =
        graph.getStreamNodes().stream()
            .filter(
                node ->
                    node.getOperatorFactory() instanceof SimpleOperatorFactory
                        && ((SimpleOperatorFactory<?>) node.getOperatorFactory()).getOperator()
                            instanceof NativeColumnarGroupAggregateOperator)
            .collect(java.util.stream.Collectors.toList());
    assertEquals(1, owners.size(), graph.getStreamingPlanAsJSON());
    var owner = owners.get(0);
    assertEquals(257, owner.getMaxParallelism());
    assertEquals(2, owner.getParallelism());
    assertEquals(uids.equals("DISABLED"), owner.getTransformationUID() == null);
    var operator = ((SimpleOperatorFactory<?>) owner.getOperatorFactory()).getOperator();
    assertEquals(
        sumType.equals("INT") ? 2 : 0,
        ((int[]) GroupAggregateStockTestFixture.field(operator, "valueTypes"))[1]);
    var contract =
        assertInstanceOf(
            StockGroupTypedStateContract.class,
            GroupAggregateStockTestFixture.field(operator, "stockStateContract"));
    assertEquals(
        2,
        ((org.apache.flink.table.types.logical.RowType)
                GroupAggregateStockTestFixture.field(contract, "keyType"))
            .getFieldCount());
    assertArrayEquals(
        shared ? new int[] {3, 0, 7, 7} : new int[] {3, 0, 7},
        (int[]) GroupAggregateStockTestFixture.field(contract, "kinds"));
    var views = (java.util.List<?>) GroupAggregateStockTestFixture.field(contract, "views");
    assertEquals(1, views.size());
    assertEquals(
        shared ? 2 : 1,
        ((int[]) GroupAggregateStockTestFixture.field(views.get(0), "aggregates")).length);
    assertEquals(!append, GroupAggregateStockTestFixture.field(views.get(0), "counted"));
    assertFalse((boolean) GroupAggregateStockTestFixture.field(operator, "miniBatch"));
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
    assertTrue(
        graph.getStreamNodes().stream()
            .anyMatch(node -> node.getOperatorName().contains("RowDataToArrow")));
    assertTrue(
        graph.getStreamNodes().stream()
            .anyMatch(node -> node.getOperatorName().contains("ArrowToRowData")));
    if (compiled) assertEquals(portable, plan.asJsonString());
  }
}
