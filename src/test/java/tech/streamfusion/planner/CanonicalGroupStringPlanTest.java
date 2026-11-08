package tech.streamfusion.planner;

import static org.junit.jupiter.api.Assertions.*;
import static tech.streamfusion.planner.GroupAggregateStockTestFixture.field;

import java.util.stream.Stream;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.table.api.PlanReference;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import tech.streamfusion.NativePlannerTestEnvironment;
import tech.streamfusion.operator.NativeColumnarGroupAggregateOperator;
import tech.streamfusion.operator.RowDataToArrowOperator;
import tech.streamfusion.state.StockGroupTypedStateContract;

class CanonicalGroupStringPlanTest {
  static Stream<Arguments> cases() {
    return Stream.of(false, true)
        .flatMap(
            compiled ->
                Stream.of("STRING", "VARCHAR(4096)", "CHAR(4)")
                    .filter(
                        type ->
                            !type.startsWith("CHAR")
                                || tech.streamfusion.compat.CanonicalTestCompat.flink22())
                    .flatMap(
                        type ->
                            Stream.of("DISABLED", "ALWAYS")
                                .flatMap(
                                    uids ->
                                        Stream.of(false, true)
                                            .map(
                                                computed ->
                                                    Arguments.of(
                                                        compiled, type, uids, computed)))));
  }

  @ParameterizedTest(name = "compiled={0},type={1},uids={2},computed={3}")
  @MethodSource("cases")
  void stringExtremaUseCapturedStockProducersAndCachePreservingArrowInput(
      boolean compiled, String type, String uids, boolean computed) throws Exception {
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
        "CREATE TABLE input_table(k BIGINT,v "
            + type
            + ") WITH"
            + " ('connector'='canonical-fixture','input'='unused.csv','ready'='unused.ready','insert-only'='true')");
    table.useCatalog("default_catalog");
    table.executeSql(
        "CREATE TABLE output_table(k BIGINT,lo STRING,hi STRING) WITH ('connector'='blackhole')");
    String value = computed ? "UPPER(v)" : "v";
    String query =
        "INSERT INTO output_table SELECT k,MIN("
            + value
            + "),MAX("
            + value
            + ") FROM fixture.db.input_table GROUP BY k";
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
    var node = owners.get(0);
    assertEquals(257, tech.streamfusion.compat.FlinkPlanCompat.maximumParallelism(node));
    assertEquals(2, node.getParallelism());
    assertEquals(uids.equals("DISABLED"), node.getTransformationUID() == null);
    var owner = ((SimpleOperatorFactory<?>) node.getOperatorFactory()).getOperator();
    var contract =
        assertInstanceOf(StockGroupTypedStateContract.class, field(owner, "stockStateContract"));
    assertArrayEquals(new int[] {10, 11}, (int[]) field(contract, "kinds"));
    var entries =
        graph.getStreamNodes().stream()
            .filter(
                entry ->
                    entry.getOperatorFactory() instanceof SimpleOperatorFactory
                        && ((SimpleOperatorFactory<?>) entry.getOperatorFactory()).getOperator()
                            instanceof RowDataToArrowOperator)
            .collect(java.util.stream.Collectors.toList());
    assertEquals(1, entries.size(), graph.getStreamingPlanAsJSON());
    assertEquals(
        true,
        field(
            ((SimpleOperatorFactory<?>) entries.get(0).getOperatorFactory()).getOperator(),
            "preserveStringCaches"));
    if (compiled) assertEquals(portable, plan.asJsonString());
  }
}
