package tech.streamfusion.planner.stream;

import static org.junit.jupiter.api.Assertions.*;
import static tech.streamfusion.planner.stream.GroupAggregateStockTestFixture.field;

import java.util.ArrayList;
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
import tech.streamfusion.operator.CanonicalRowKeyFixture;
import tech.streamfusion.operator.NativeOverAggregateOperator;
import tech.streamfusion.state.StockOverFoldStateContract;

class CanonicalOverRowKeyPlanTest {
  static Stream<Arguments> cases() {
    var cases = new ArrayList<Arguments>();
    for (boolean nested : new boolean[] {false, true})
      for (boolean compiled : new boolean[] {false, true})
        for (String mode :
            new String[] {
              "rows", "range", "ttl", "event", "bounded", "distinct", "string-key", "string-min"
            }) cases.add(Arguments.of(nested, compiled, mode));
    return tech.streamfusion.CanonicalTestMatrix.select(cases.stream());
  }

  @ParameterizedTest
  @MethodSource("cases")
  void onlyUnboundedProcessingFoldsBindTheOwnedRowInput(
      boolean nested, boolean compiled, String mode) throws Exception {
    var config = new Configuration();
    config.setString("streamfusion.native.enabled", "true");
    config.setString("table.exec.state.ttl", mode.equals("ttl") ? "1 h" : "0 ms");
    var table =
        NativePlannerTestEnvironment.create(
            config, org.example.canonical.CanonicalFixtureFactory.class);
    table.executeSql(
        "CREATE TABLE input_table(k "
            + CanonicalRowKeyFixture.sql(nested)
            + ",v BIGINT NOT NULL,s STRING,rt TIMESTAMP(3),pt AS PROCTIME(),WATERMARK FOR rt AS rt)"
            + " WITH ('connector'='canonical-fixture','input'='unused.csv','insert-only'='true')");
    table.executeSql(
        "CREATE TABLE output_table(k "
            + CanonicalRowKeyFixture.sql(nested)
            + ",a "
            + (mode.equals("string-min") ? "STRING" : "BIGINT")
            + ") WITH ('connector'='blackhole')");
    String query =
        "INSERT INTO output_table SELECT k,"
            + (mode.equals("string-min")
                ? "MIN(s)"
                : mode.equals("distinct") ? "COUNT(DISTINCT v)" : "SUM(v)")
            + " OVER (PARTITION BY k"
            + (mode.equals("string-key") ? ",s" : "")
            + " ORDER BY "
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
            .toList();
    boolean admitted =
        mode.equals("rows")
            || mode.equals("range")
            || mode.equals("ttl")
            || mode.equals("string-key")
            || mode.equals("string-min");
    assertEquals(admitted ? 1 : 0, owners.size(), graph.getStreamingPlanAsJSON());
    if (admitted) {
      var contract =
          assertInstanceOf(
              StockOverFoldStateContract.class, field(owners.get(0), "stockStateContract"));
      assertEquals(true, field(contract, "preserveRowKeys"));
      assertTrue(
          graph.getStreamNodes().stream()
              .filter(node -> node.getOperatorFactory() instanceof SimpleOperatorFactory)
              .map(node -> ((SimpleOperatorFactory<?>) node.getOperatorFactory()).getOperator())
              .filter(tech.streamfusion.operator.RowDataToArrowOperator.class::isInstance)
              .anyMatch(
                  operator -> {
                    try {
                      return org.apache.arrow.vector.types.pojo.Schema.deserializeMessage(
                              java.nio.ByteBuffer.wrap((byte[]) field(operator, "arrowSchema")))
                          .toJson()
                          .contains("streamfusion.nested-row-binary");
                    } catch (Exception failure) {
                      throw new AssertionError(failure);
                    }
                  }));
    }
    assertFalse(portable.contains("tech.streamfusion"));
    if (compiled) assertEquals(portable, plan.asJsonString());
  }
}
