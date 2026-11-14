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
import tech.streamfusion.operator.NativeColumnarTopNOperator;
import tech.streamfusion.state.StockUpdatingTopNStateContract;

class CanonicalUpdatingRankBoundPlanTest {
  static Stream<Arguments> cases() {
    return Stream.of("INT", "BIGINT")
        .flatMap(
            type ->
                Stream.of(false, true)
                    .flatMap(
                        compiled ->
                            Stream.of(false, true)
                                .map(rank -> Arguments.of(type, compiled, rank))));
  }

  @ParameterizedTest
  @MethodSource("cases")
  void actualVariableUpdatingOwnerRetainsIndependentBoundState(
      String type, boolean compiled, boolean rank) throws Exception {
    var config = new Configuration();
    config.setString("streamfusion.native.enabled", "true");
    config.setString("table.exec.state.ttl", "0 ms");
    config.setString("pipeline.max-parallelism", "257");
    var table = NativePlannerTestEnvironment.create(config);
    table.executeSql("CREATE TABLE input_table(k BIGINT,v BIGINT) WITH ('connector'='datagen')");
    table.executeSql(
        "CREATE TABLE output_table(k BIGINT,v BIGINT,n BIGINT) WITH ('connector'='blackhole')");
    String query =
        "INSERT INTO output_table SELECT k,v,"
            + (rank ? "rn" : "CAST(0 AS BIGINT)")
            + " FROM (SELECT *,ROW_NUMBER() OVER (PARTITION BY k ORDER BY c DESC,v ASC) AS rn"
            + " FROM (SELECT k,v,COUNT(*) AS c,CAST(COALESCE(MOD(ABS(k),3)+1,2) AS "
            + type
            + ") AS b FROM input_table GROUP BY k,v)) WHERE rn<=b";
    var planner = (PlannerBase) ((TableEnvironmentImpl) table).getPlanner();
    var plan = compiled ? table.compilePlanSql(query) : null;
    String portable = compiled ? plan.asJsonString() : "";
    var transformations =
        compiled
            ? planner.translatePlan(planner.loadPlan(PlanReference.fromJsonString(portable)))
            : CanonicalRegularJoinPlanTest.translate(planner, query);
    var graph = CanonicalRegularJoinPlanTest.graph(planner, table, transformations);
    var owners =
        graph.getStreamNodes().stream()
            .filter(node -> node.getOperatorFactory() instanceof SimpleOperatorFactory)
            .map(node -> ((SimpleOperatorFactory<?>) node.getOperatorFactory()).getOperator())
            .filter(NativeColumnarTopNOperator.class::isInstance)
            .collect(java.util.stream.Collectors.toList());
    assertEquals(1, owners.size(), graph.getStreamingPlanAsJSON());
    var owner = owners.get(0);
    var contract =
        assertInstanceOf(StockUpdatingTopNStateContract.class, field(owner, "stockStateContract"));
    assertEquals(
        java.util.Set.of("rankEnd", "data-state-with-update"),
        contract.stateDescriptors().keySet());
    assertEquals(true, field(owner, "firstBound"));
    assertTrue((int) field(owner, "rankEndColumn") >= 0);
    assertEquals(0L, field(owner, "stateTtlMillis"));
    assertFalse(portable.contains("tech.streamfusion"));
    if (compiled) assertEquals(portable, plan.asJsonString());
  }
}
