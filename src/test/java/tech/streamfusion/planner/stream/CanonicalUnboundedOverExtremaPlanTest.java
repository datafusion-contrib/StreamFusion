package tech.streamfusion.planner.stream;

import static org.junit.jupiter.api.Assertions.*;

import java.util.stream.Stream;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.table.api.PlanReference;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.*;
import tech.streamfusion.NativePlannerTestEnvironment;
import tech.streamfusion.compat.FlinkPlanCompat;
import tech.streamfusion.operator.NativeOverAggregateOperator;
import tech.streamfusion.state.*;

class CanonicalUnboundedOverExtremaPlanTest {
  static Stream<Arguments> cases() {
    return tech.streamfusion.CanonicalTestMatrix.select(
        Stream.of(
                "TINYINT",
                "INT",
                "BIGINT",
                "FLOAT",
                "DOUBLE",
                "DECIMAL(18,2)",
                "DECIMAL(38,20)",
                "STRING",
                "CHAR(32)",
                "BOOLEAN",
                "DATE",
                "TIME(3)",
                "TIMESTAMP(9)",
                "TIMESTAMP_LTZ(3)")
            .flatMap(
                type ->
                    Stream.of(false, true)
                        .flatMap(
                            event ->
                                Stream.of(false, true)
                                    .flatMap(
                                        rows ->
                                            Stream.of(false, true)
                                                .flatMap(
                                                    compiled ->
                                                        Stream.of(
                                                                1,
                                                                FlinkPlanCompat
                                                                    .unboundedOverVersion())
                                                            .distinct()
                                                            .map(
                                                                version ->
                                                                    Arguments.of(
                                                                        type, event, rows, compiled,
                                                                        version)))))));
  }

  @ParameterizedTest
  @MethodSource("cases")
  void releasedScalarExtremaBindForOrdinaryAndPortablePlans(
      String type, boolean event, boolean rows, boolean compiled, int version) throws Exception {
    var config = new Configuration();
    config.setString("streamfusion.native.enabled", "true");
    config.setString("table.exec.unbounded-over.version", Integer.toString(version));
    config.setString("table.exec.state.ttl", "1 h");
    var table = NativePlannerTestEnvironment.create(config);
    table.executeSql(
        "CREATE TABLE input_table(k BIGINT,v "
            + type
            + ",rt TIMESTAMP(3),pt AS PROCTIME(),WATERMARK FOR rt AS rt) WITH"
            + " ('connector'='datagen')");
    table.executeSql(
        "CREATE TABLE output_table(k BIGINT,lo "
            + type
            + ",hi "
            + type
            + ") WITH ('connector'='blackhole')");
    String query =
        "INSERT INTO output_table SELECT k,MIN(v) OVER w,MAX(v) OVER w"
            + " FROM input_table WINDOW w AS (PARTITION BY k ORDER BY "
            + (event ? "rt" : "pt")
            + (rows ? " ROWS" : " RANGE")
            + " BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)";
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
    assertEquals(1, owners.size(), graph.getStreamingPlanAsJSON());
    var owner = owners.get(0);
    assertArrayEquals(
        new int[] {1, 2}, (int[]) GroupAggregateStockTestFixture.field(owner, "aggregateKinds"));
    assertInstanceOf(
        event ? StockOverEventFoldStateContract.class : StockOverFoldStateContract.class,
        GroupAggregateStockTestFixture.field(owner, "stockStateContract"));
    assertFalse(portable.contains("tech.streamfusion"));
    if (compiled) assertEquals(portable, plan.asJsonString());
  }
}
