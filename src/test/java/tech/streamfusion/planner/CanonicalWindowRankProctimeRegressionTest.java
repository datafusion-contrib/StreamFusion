package tech.streamfusion.planner;

import static org.junit.jupiter.api.Assertions.*;

import java.util.stream.Stream;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import tech.streamfusion.NativePlannerTestEnvironment;
import tech.streamfusion.operator.NativeColumnarWindowRankOperator;

class CanonicalWindowRankProctimeRegressionTest {
  static Stream<Arguments> cases() {
    return Stream.of("rank", "first", "last")
        .flatMap(mode -> Stream.of(Arguments.of(mode, false), Arguments.of(mode, true)));
  }

  @ParameterizedTest
  @MethodSource("cases")
  void eventTimeInteropDoesNotDisableExistingNativeProcessingTimeWindows(String mode, boolean keyed)
      throws Exception {
    var config = new Configuration();
    config.setString("streamfusion.native.enabled", "true");
    config.setString("table.exec.uid.generation", "DISABLED");
    var table = NativePlannerTestEnvironment.create(config);
    table.executeSql(
        "CREATE TABLE pc_input(k BIGINT,v BIGINT,pt AS PROCTIME()) WITH ('connector'='datagen')");
    table.executeSql("CREATE TABLE pc_output(k BIGINT,v BIGINT) WITH ('connector'='blackhole')");
    String query =
        "INSERT INTO pc_output SELECT k,v FROM (SELECT k,v,ROW_NUMBER() OVER (PARTITION BY"
            + " window_start,window_end"
            + (keyed ? ",k" : "")
            + " ORDER BY "
            + (mode.equals("rank") ? "v" : "pt")
            + (mode.equals("last") ? " DESC" : " ASC")
            + ") AS rn FROM TABLE(TUMBLE(TABLE pc_input,DESCRIPTOR(pt),INTERVAL '3' SECOND))) WHERE"
            + " rn <= "
            + (mode.equals("rank") ? "3" : "1");
    var planner = (PlannerBase) ((TableEnvironmentImpl) table).getPlanner();
    var graph =
        CanonicalRegularJoinPlanTest.graph(
            planner, table, CanonicalRegularJoinPlanTest.translate(planner, query));
    var owners =
        graph.getStreamNodes().stream()
            .filter(
                node ->
                    node.getOperatorFactory() instanceof SimpleOperatorFactory
                        && ((SimpleOperatorFactory<?>) node.getOperatorFactory()).getOperator()
                            instanceof NativeColumnarWindowRankOperator)
            .collect(java.util.stream.Collectors.toList());
    assertEquals(1, owners.size(), graph.getStreamingPlanAsJSON());
    var operator = ((SimpleOperatorFactory<?>) owners.get(0).getOperatorFactory()).getOperator();
    assertEquals(true, GroupAggregateStockTestFixture.field(operator, "proctime"));
    assertNull(GroupAggregateStockTestFixture.field(operator, "stockStateContract"));
    table.getConfig().set("streamfusion.native.enabled", "false");
    var failure =
        assertThrows(
            org.apache.flink.table.api.TableException.class,
            () -> CanonicalRegularJoinPlanTest.translate(planner, query));
    assertTrue(failure.getMessage().contains("Processing time Window"), failure.toString());
  }
}
