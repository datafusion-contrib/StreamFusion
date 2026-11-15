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
    return tech.streamfusion.operator.CanonicalOverDistinctValueFixture.countTypes()
        .flatMap(
            type ->
                Stream.of(
                        "rows",
                        "range",
                        "nullable",
                        "independent",
                        "ttl",
                        "event",
                        "bounded",
                        "sum",
                        "avg")
                    .filter(
                        mode ->
                            !mode.equals("sum") && !mode.equals("avg")
                                || tech.streamfusion.operator.CanonicalOverDistinctValueFixture
                                    .integral(type)
                                || type.startsWith("DECIMAL")
                                || type.equals("FLOAT")
                                || type.equals("DOUBLE"))
                    .flatMap(
                        mode ->
                            Stream.of(false, true)
                                .map(compiled -> Arguments.of(type, mode, compiled))));
  }

  @ParameterizedTest
  @MethodSource("cases")
  void onlyTheCapturedScalarMembershipContractIsBound(String type, String mode, boolean compiled)
      throws Exception {
    var config = new Configuration();
    config.setString("streamfusion.native.enabled", "true");
    config.setString("table.exec.state.ttl", mode.equals("ttl") ? "1 h" : "0 ms");
    var table =
        NativePlannerTestEnvironment.create(
            config, org.example.canonical.CanonicalFixtureFactory.class);
    table.executeSql(
        "CREATE TABLE input_table(k BIGINT,v "
            + type
            + (mode.equals("nullable") ? "" : " NOT NULL")
            + ",rt TIMESTAMP(3),pt AS PROCTIME(),WATERMARK FOR rt AS rt)"
            + " WITH ('connector'='canonical-fixture','input'='unused.csv','insert-only'='true')");
    String query =
        "INSERT INTO output_table SELECT k,v,"
            + (mode.equals("sum") ? "SUM" : mode.equals("avg") ? "AVG" : "COUNT")
            + "(DISTINCT v) OVER w"
            + (mode.equals("independent") ? ",COUNT(DISTINCT k) OVER w" : "")
            + " FROM input_table WINDOW w AS (PARTITION BY k ORDER BY "
            + (mode.equals("event") ? "rt" : "pt")
            + (mode.equals("range") ? " RANGE" : " ROWS")
            + " BETWEEN "
            + (mode.equals("bounded") ? "2" : "UNBOUNDED")
            + " PRECEDING AND CURRENT ROW)";
    table.createTemporaryTable(
        "output_table",
        org.apache.flink.table.api.TableDescriptor.forConnector("blackhole")
            .schema(
                org.apache.flink.table.api.Schema.newBuilder()
                    .fromResolvedSchema(
                        table
                            .sqlQuery(query.substring("INSERT INTO output_table ".length()))
                            .getResolvedSchema())
                    .build())
            .build());
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
        mode.equals("rows")
            || mode.equals("range")
            || mode.equals("nullable")
            || mode.equals("independent")
            || (mode.equals("sum") || mode.equals("avg"))
                && tech.streamfusion.operator.CanonicalOverDistinctValueFixture.arithmetic(type);
    var bound = new java.util.ArrayList<Object>();
    for (var owner : owners) if (field(owner, "stockStateContract") != null) bound.add(owner);
    assertEquals(admitted ? 1 : 0, bound.size(), graph.getStreamingPlanAsJSON());
    if (!admitted && compiled) assertTrue(owners.isEmpty(), graph.getStreamingPlanAsJSON());
    if (admitted) {
      var contract =
          assertInstanceOf(
              StockOverFoldStateContract.class, field(bound.get(0), "stockStateContract"));
      assertEquals(
          mode.equals("independent") ? 2 : 1,
          contract.stateDescriptors().values().stream()
              .filter(kind -> kind == org.apache.flink.api.common.state.StateDescriptor.Type.MAP)
              .count());
      assertArrayEquals(
          mode.equals("independent")
              ? new int[] {103, 103}
              : mode.equals("avg")
                  ? new int[] {100, 103}
                  : new int[] {mode.equals("sum") ? 100 : 103},
          (int[]) field(bound.get(0), "aggregateKinds"),
          type
              + ","
              + mode
              + ","
              + compiled
              + ": "
              + java.util.Arrays.toString((int[]) field(bound.get(0), "aggregateKinds")));
    }
    assertFalse(portable.contains("tech.streamfusion"));
    if (compiled) assertEquals(portable, plan.asJsonString());
  }
}
