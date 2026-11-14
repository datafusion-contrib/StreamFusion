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
import tech.streamfusion.operator.NativeOverAggregateOperator;
import tech.streamfusion.state.StockOverFoldStateContract;

class CanonicalOverDistinctPlanTest {
  static Stream<Arguments> cases() {
    return Stream.of("INT", "BIGINT")
        .flatMap(
            type ->
                Stream.of("rows", "range", "nullable", "ttl", "event", "bounded", "sum", "avg")
                    .flatMap(
                        mode ->
                            Stream.of(false, true)
                                .map(compiled -> Arguments.of(type, mode, compiled))));
  }

  @ParameterizedTest
  @MethodSource("cases")
  void onlyTheCapturedNonNullCountMembershipContractIsBound(
      String type, String mode, boolean compiled) throws Exception {
    var config = new Configuration();
    config.setString("streamfusion.native.enabled", "true");
    config.setString("table.exec.state.ttl", mode.equals("ttl") ? "1 h" : "0 ms");
    var table = NativePlannerTestEnvironment.create(config);
    table.executeSql(
        "CREATE TABLE input_table(k BIGINT,v "
            + type
            + (mode.equals("nullable") ? "" : " NOT NULL")
            + ",rt TIMESTAMP(3),pt AS PROCTIME(),WATERMARK FOR rt AS rt)"
            + " WITH ('connector'='datagen')");
    table.executeSql(
        "CREATE TABLE output_table(k BIGINT,v "
            + type
            + ",c BIGINT) WITH ('connector'='blackhole')");
    String query =
        "INSERT INTO output_table SELECT k,v,"
            + (mode.equals("sum") ? "SUM" : mode.equals("avg") ? "AVG" : "COUNT")
            + "(DISTINCT v) OVER (PARTITION BY k ORDER BY "
            + (mode.equals("event") ? "rt" : "pt")
            + (mode.equals("range") ? " RANGE" : " ROWS")
            + " BETWEEN "
            + (mode.equals("bounded") ? "2" : "UNBOUNDED")
            + " PRECEDING AND CURRENT ROW) FROM input_table";
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
            .filter(NativeOverAggregateOperator.class::isInstance)
            .collect(java.util.stream.Collectors.toList());
    boolean admitted =
        mode.equals("rows") || mode.equals("range") || mode.equals("sum") || mode.equals("avg");
    assertEquals(admitted ? 1 : 0, owners.size(), graph.getStreamingPlanAsJSON());
    if (admitted) {
      var contract =
          assertInstanceOf(
              StockOverFoldStateContract.class, field(owners.get(0), "stockStateContract"));
      assertEquals(
          1,
          contract.stateDescriptors().values().stream()
              .filter(kind -> kind == org.apache.flink.api.common.state.StateDescriptor.Type.MAP)
              .count());
      assertArrayEquals(
          new int[] {mode.equals("sum") ? 100 : mode.equals("avg") ? 104 : 103},
          (int[]) field(owners.get(0), "aggregateKinds"));
    }
    assertFalse(portable.contains("tech.streamfusion"));
    if (compiled) assertEquals(portable, plan.asJsonString());
  }
}
