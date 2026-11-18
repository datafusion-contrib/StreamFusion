package tech.streamfusion.planner.stream;

import static org.junit.jupiter.api.Assertions.*;

import java.util.stream.Stream;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.table.api.PlanReference;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.example.canonical.CanonicalFixtureFactory;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.NativePlannerTestEnvironment;
import tech.streamfusion.compat.FlinkPlanCompat;
import tech.streamfusion.operator.CanonicalOverValueFixture;
import tech.streamfusion.operator.NativeOverAggregateOperator;
import tech.streamfusion.state.*;

class CanonicalOverCountPayloadPlanTest {
  static Stream<Arguments> cases() {
    return tech.streamfusion.CanonicalTestMatrix.select(
        CanonicalOverValueFixture.countTypes()
            .flatMap(
                type ->
                    Stream.of(
                            "proc-bounded",
                            "event-rows",
                            "event-range",
                            "proc-rows",
                            "proc-range",
                            "event-unbounded-rows",
                            "event-unbounded-range")
                        .flatMap(
                            mode ->
                                Stream.of(false, true)
                                    .flatMap(
                                        compiled ->
                                            Stream.of(1, FlinkPlanCompat.unboundedOverVersion())
                                                .distinct()
                                                .filter(
                                                    version ->
                                                        mode.startsWith("event-unbounded")
                                                            || version == 1)
                                                .map(
                                                    version ->
                                                        Arguments.of(
                                                            type, mode, compiled, version))))));
  }

  @ParameterizedTest
  @MethodSource("cases")
  void existingValidityReadersBindCountWithoutRestrictingItsArgumentCodec(
      String type, String mode, boolean compiled, int version) throws Exception {
    assertPlan(type, mode, compiled, version, true);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void fixedBinaryArgumentsRetainTheExistingStockAdmission(boolean compiled) throws Exception {
    assertPlan("BINARY(8)", "proc-rows", compiled, 1, false);
  }

  private void assertPlan(String type, String mode, boolean compiled, int version, boolean admitted)
      throws Exception {
    var config = new Configuration();
    config.setString("streamfusion.native.enabled", "true");
    config.setString("table.exec.unbounded-over.version", Integer.toString(version));
    config.setString("table.exec.state.ttl", "1 h");
    var table = NativePlannerTestEnvironment.create(config, CanonicalFixtureFactory.class);
    table.createTemporaryTable(
        "count_input",
        org.apache.flink.table.api.TableDescriptor.forConnector("canonical-fixture")
            .schema(
                tech.streamfusion.CanonicalTestSchemas.columns(
                        "k", "BIGINT", "v", type, "rt", "TIMESTAMP(3)")
                    .columnByExpression("pt", "PROCTIME()")
                    .watermark("rt", "rt")
                    .build())
            .option("input", "unused.csv")
            .option("insert-only", "true")
            .build());
    table.executeSql(
        "CREATE TABLE count_output(k BIGINT,n BIGINT,c BIGINT) WITH ('connector'='blackhole')");
    boolean bounded =
        mode.equals("proc-bounded") || mode.equals("event-rows") || mode.equals("event-range");
    boolean event = mode.startsWith("event");
    boolean range = mode.endsWith("range");
    String frame = range ? "RANGE" : "ROWS";
    String lower = !bounded ? "UNBOUNDED" : range ? "INTERVAL '0.032' SECOND" : "2";
    String query =
        "INSERT INTO count_output SELECT k,COUNT(v) OVER w,COUNT(k) OVER w"
            + " FROM count_input WINDOW w AS (PARTITION BY k ORDER BY "
            + (event ? "rt" : "pt")
            + " "
            + frame
            + " BETWEEN "
            + lower
            + " PRECEDING AND CURRENT ROW)";
    var planner = (PlannerBase) ((TableEnvironmentImpl) table).getPlanner();
    var plan = compiled ? table.compilePlanSql(query) : null;
    String portable = plan == null ? "" : plan.asJsonString();
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
    assertFalse(portable.contains("tech.streamfusion"));
    if (compiled) assertEquals(portable, plan.asJsonString());
    if (!admitted) {
      assertTrue(owners.isEmpty(), graph.getStreamingPlanAsJSON());
      assertTrue(
          graph.getStreamNodes().stream()
              .filter(node -> node.getOperatorFactory() instanceof SimpleOperatorFactory)
              .map(node -> ((SimpleOperatorFactory<?>) node.getOperatorFactory()).getOperator())
              .anyMatch(
                  org.apache.flink.streaming.api.operators.KeyedProcessOperator.class::isInstance));
      return;
    }
    assertEquals(1, owners.size(), graph.getStreamingPlanAsJSON());
    var owner = owners.get(0);
    assertArrayEquals(
        new int[] {3, 3}, (int[]) GroupAggregateStockTestFixture.field(owner, "aggregateKinds"));
    Class<?> contract =
        bounded
            ? event
                ? range ? StockOverRangeStateContract.class : StockOverEventRowsStateContract.class
                : StockOverProcessingRowsStateContract.class
            : event ? StockOverEventFoldStateContract.class : StockOverFoldStateContract.class;
    assertInstanceOf(contract, GroupAggregateStockTestFixture.field(owner, "stockStateContract"));
  }
}
