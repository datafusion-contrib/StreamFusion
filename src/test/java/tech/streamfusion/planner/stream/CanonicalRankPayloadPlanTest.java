package tech.streamfusion.planner.stream;

import static org.junit.jupiter.api.Assertions.*;

import java.util.stream.Stream;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.table.api.PlanReference;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.types.logical.LogicalTypeFamily;
import org.apache.flink.table.types.logical.RowType;
import org.example.canonical.CanonicalFixtureFactory;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import tech.streamfusion.NativePlannerTestEnvironment;
import tech.streamfusion.operator.NativeColumnarTopNOperator;
import tech.streamfusion.state.StockAppendTopNStateContract;
import tech.streamfusion.state.StockRetractingTopNStateContract;
import tech.streamfusion.state.StockUpdatingTopNStateContract;

class CanonicalRankPayloadPlanTest {
  static Stream<Arguments> cases() {
    return Stream.of("append", "update", "retract")
        .flatMap(
            algorithm ->
                Stream.of(false, true)
                    .flatMap(
                        maps ->
                            Stream.of(false, true)
                                .map(compiled -> Arguments.of(algorithm, maps, compiled))));
  }

  @ParameterizedTest
  @MethodSource("cases")
  void retainedPayloadAdmissionKeepsIndependentKeyAndRetractionRules(
      String algorithm, boolean maps, boolean compiled) throws Exception {
    var config = new Configuration();
    config.setString("streamfusion.native.enabled", "true");
    var table = NativePlannerTestEnvironment.create(config, CanonicalFixtureFactory.class);
    table.createTemporarySystemFunction(
        "rank_checksum",
        maps
            ? CanonicalFixtureFactory.NestedRankChecksum.class
            : CanonicalFixtureFactory.NestedRowRankChecksum.class);
    table.executeSql(
        "CREATE TABLE input_table(k BIGINT,v BIGINT,id BIGINT,p "
            + (maps
                ? CanonicalFixtureFactory.NestedJoinChecksum.TYPE
                : CanonicalFixtureFactory.NestedRowRankChecksum.TYPE)
            + ") WITH ('connector'='canonical-fixture','input'='unused.csv','insert-only'='"
            + !algorithm.equals("retract")
            + "')");
    table.executeSql(
        "CREATE TABLE output_table(k BIGINT,n BIGINT,total BIGINT) WITH ('connector'='blackhole')");
    String input =
        algorithm.equals("update")
            ? "(SELECT k,id,COUNT(*) AS c,LAST_VALUE(p) AS p FROM input_table GROUP BY k,id)"
            : "input_table";
    String query =
        "INSERT INTO output_table SELECT k,id,rank_checksum(p,rn) FROM (SELECT *,"
            + "ROW_NUMBER() OVER (PARTITION BY k ORDER BY "
            + (algorithm.equals("update") ? "c DESC,id ASC" : "v ASC,id ASC")
            + ") AS rn FROM "
            + input
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
    boolean admitted = !maps || algorithm.equals("append");
    assertEquals(admitted ? 1 : 0, owners.size(), graph.getStreamingPlanAsJSON());
    if (admitted) {
      var operator = ((SimpleOperatorFactory<?>) owners.get(0).getOperatorFactory()).getOperator();
      var contract = GroupAggregateStockTestFixture.field(operator, "stockStateContract");
      assertInstanceOf(
          algorithm.equals("append")
              ? StockAppendTopNStateContract.class
              : algorithm.equals("update")
                  ? StockUpdatingTopNStateContract.class
                  : StockRetractingTopNStateContract.class,
          contract);
      var type = (RowType) GroupAggregateStockTestFixture.field(operator, "rowType");
      assertTrue(
          type.getChildren().stream().anyMatch(field -> field.is(LogicalTypeFamily.CONSTRUCTED)),
          "The function must retain the full payload through the rank owner");
    }
    assertFalse(portable.contains("tech.streamfusion"));
    if (compiled) assertEquals(portable, plan.asJsonString());
  }
}
