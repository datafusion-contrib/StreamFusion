package tech.streamfusion.planner.stream;

import static org.junit.jupiter.api.Assertions.*;
import static tech.streamfusion.planner.stream.GroupAggregateStockTestFixture.field;

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
import tech.streamfusion.operator.NativeColumnarLocalGroupAggregateOperator;
import tech.streamfusion.state.StockGroupTypedStateContract;

class CanonicalMiniBatchGroupPlanTest {
  static Stream<Arguments> cases() {
    return Stream.of(false, true)
        .flatMap(
            compiled ->
                MiniBatchGroupStockFixture.types()
                    .flatMap(
                        type ->
                            Stream.of("DISABLED", "ALWAYS")
                                .flatMap(
                                    uids ->
                                        Stream.of(false, true)
                                            .map(
                                                twoPhase ->
                                                    Arguments.of(
                                                        compiled, type, uids, twoPhase)))));
  }

  @ParameterizedTest(name = "compiled={0},type={1},uids={2},twoPhase={3}")
  @MethodSource("cases")
  void actualStockBundleGraphsBindNativeAccumulators(
      boolean compiled, String type, String uids, boolean twoPhase) throws Exception {
    var config = new Configuration();
    config.setString("streamfusion.native.enabled", "true");
    config.setString("parallelism.default", "2");
    config.setString("pipeline.max-parallelism", "257");
    config.setString("table.exec.uid.generation", uids);
    var table = NativePlannerTestEnvironment.create(config);
    table.getConfig().set("table.exec.mini-batch.enabled", "true");
    table.getConfig().set("table.exec.mini-batch.size", "5");
    table.getConfig().set("table.exec.mini-batch.allow-latency", "10 s");
    table
        .getConfig()
        .set("table.optimizer.agg-phase-strategy", twoPhase ? "TWO_PHASE" : "ONE_PHASE");
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
        "CREATE TABLE input_table(k BIGINT,a "
            + MiniBatchGroupStockFixture.arithmeticType(type)
            + ",v "
            + type
            + ") WITH"
            + " ('connector'='canonical-fixture','input'='unused.csv','ready'='unused.ready','insert-only'='true')");
    table.useCatalog("default_catalog");
    table.executeSql(
        "CREATE TABLE output_table(k BIGINT,n BIGINT,av "
            + MiniBatchGroupStockFixture.arithmeticType(type)
            + ",lo "
            + type
            + ",hi "
            + type
            + ",s "
            + MiniBatchGroupStockFixture.arithmeticType(type)
            + ") WITH"
            + " ('connector'='blackhole')");
    String query =
        "INSERT INTO output_table SELECT k,COUNT(*),AVG(a) FILTER(WHERE a>1),MIN(v),MAX(v),SUM(a)"
            + " FILTER(WHERE a>1) FROM fixture.db.input_table GROUP BY k";
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
                n ->
                    n.getOperatorFactory() instanceof SimpleOperatorFactory
                        && ((SimpleOperatorFactory<?>) n.getOperatorFactory()).getOperator()
                            instanceof NativeColumnarGroupAggregateOperator)
            .collect(java.util.stream.Collectors.toList());
    assertEquals(1, owners.size(), graph.getStreamingPlanAsJSON());
    var node = owners.get(0);
    var owner = ((SimpleOperatorFactory<?>) node.getOperatorFactory()).getOperator();
    assertInstanceOf(StockGroupTypedStateContract.class, field(owner, "stockStateContract"));
    assertEquals(true, field(owner, "miniBatch"));
    assertEquals(257, tech.streamfusion.compat.FlinkPlanCompat.maximumParallelism(node));
    assertEquals(2, node.getParallelism());
    assertEquals(uids.equals("DISABLED"), node.getTransformationUID() == null);
    long locals =
        graph.getStreamNodes().stream()
            .filter(
                n ->
                    n.getOperatorFactory() instanceof SimpleOperatorFactory
                        && ((SimpleOperatorFactory<?>) n.getOperatorFactory()).getOperator()
                            instanceof NativeColumnarLocalGroupAggregateOperator)
            .count();
    assertEquals(twoPhase ? 1 : 0, locals, graph.getStreamingPlanAsJSON());
    if (compiled) assertEquals(portable, plan.asJsonString());
  }
}
