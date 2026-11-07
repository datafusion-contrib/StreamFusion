package tech.streamfusion.planner;

import static org.junit.jupiter.api.Assertions.*;

import java.util.stream.Stream;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.table.api.PlanReference;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import tech.streamfusion.NativePlannerTestEnvironment;
import tech.streamfusion.operator.NativeOverAggregateOperator;

class CanonicalBoundedOverAdmissionTest {
  static Stream<Arguments> cases() {
    return Stream.of(
            "unbounded",
            "processing-range",
            "minimum",
            "distinct",
            "event-cleanup",
            "integer-value",
            "integer-key",
            "composite-key",
            "async",
            "unaligned")
        .flatMap(mode -> Stream.of("DISABLED", "ALWAYS").map(uids -> Arguments.of(mode, uids)));
  }

  @ParameterizedTest
  @MethodSource("cases")
  void compiledContractsSelectQualifiedUnboundedOrReleasedStateOwners(String mode, String uids)
      throws Exception {
    var config = new Configuration();
    config.setString("streamfusion.native.enabled", "true");
    config.setString("table.exec.uid.generation", uids);
    if (mode.equals("event-cleanup")) config.setString("table.exec.state.ttl", "1000 ms");
    if (mode.equals("async")) config.setString("table.exec.async-state.enabled", "true");
    if (mode.equals("unaligned"))
      config.setString("execution.checkpointing.unaligned.enabled", "true");
    var table = NativePlannerTestEnvironment.create(config);
    table.executeSql(
        "CREATE TABLE negative_input(k "
            + (mode.equals("integer-key") ? "INT" : "BIGINT")
            + ",k2 BIGINT,v "
            + (mode.equals("integer-value") ? "INT" : "BIGINT")
            + ",rt TIMESTAMP(3),pt AS PROCTIME(),WATERMARK FOR rt AS rt) WITH"
            + " ('connector'='datagen')");
    table.executeSql(
        "CREATE TABLE negative_output(k BIGINT,s BIGINT,a BIGINT) WITH ('connector'='blackhole')");
    String first =
        mode.equals("minimum") ? "MIN(v)" : mode.equals("distinct") ? "SUM(DISTINCT v)" : "SUM(v)";
    String frame =
        mode.equals("unbounded")
            ? "ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW"
            : mode.equals("processing-range")
                ? "RANGE BETWEEN INTERVAL '0.032' SECOND PRECEDING AND CURRENT ROW"
                : "ROWS BETWEEN 2 PRECEDING AND CURRENT ROW";
    String query =
        "INSERT INTO negative_output SELECT k,"
            + first
            + " OVER w,AVG(v) OVER w FROM negative_input WINDOW w AS (PARTITION BY k"
            + (mode.equals("composite-key") ? ",k2" : "")
            + " ORDER BY "
            + (mode.equals("processing-range") ? "pt" : "rt")
            + " "
            + frame
            + ")";
    var plan = table.compilePlanSql(query);
    String portable = plan.asJsonString();
    var planner = (PlannerBase) ((TableEnvironmentImpl) table).getPlanner();
    var transforms =
        planner.translatePlan(planner.loadPlan(PlanReference.fromJsonString(portable)));
    var graph = CanonicalRegularJoinPlanTest.graph(planner, table, transforms);
    var nativeOwners =
        graph.getStreamNodes().stream()
            .filter(node -> node.getOperatorFactory() instanceof SimpleOperatorFactory)
            .map(node -> ((SimpleOperatorFactory<?>) node.getOperatorFactory()).getOperator())
            .filter(NativeOverAggregateOperator.class::isInstance)
            .collect(java.util.stream.Collectors.toList());
    if (mode.equals("unbounded")) {
      assertEquals(1, nativeOwners.size(), graph.getStreamingPlanAsJSON());
      assertInstanceOf(
          tech.streamfusion.state.StockOverEventFoldStateContract.class,
          GroupAggregateStockTestFixture.field(nativeOwners.get(0), "stockStateContract"));
    } else assertTrue(nativeOwners.isEmpty(), graph.getStreamingPlanAsJSON());
    assertEquals(portable, plan.asJsonString());
    assertFalse(portable.contains("tech.streamfusion"));
    if (!mode.equals("unbounded"))
      assertTrue(
          graph.getStreamNodes().stream()
              .anyMatch(
                  node -> {
                    if (!(node.getOperatorFactory() instanceof SimpleOperatorFactory)) return false;
                    var operator =
                        ((SimpleOperatorFactory<?>) node.getOperatorFactory()).getOperator();
                    return operator instanceof KeyedProcessOperator
                        && ((KeyedProcessOperator<?, ?, ?>) operator)
                            .getUserFunction()
                            .getClass()
                            .getName()
                            .contains(".operators.over.");
                  }),
          graph.getStreamingPlanAsJSON());
  }
}
