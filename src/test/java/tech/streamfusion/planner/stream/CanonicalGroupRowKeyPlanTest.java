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
import org.junit.jupiter.params.provider.*;
import tech.streamfusion.NativePlannerTestEnvironment;
import tech.streamfusion.operator.*;
import tech.streamfusion.state.StockGroupTypedStateContract;

class CanonicalGroupRowKeyPlanTest {
  static Stream<Arguments> cases() {
    return tech.streamfusion.CanonicalTestMatrix.select(
        Stream.of("eager", "mini", "global")
            .flatMap(
                mode ->
                    Stream.of(false, true)
                        .flatMap(
                            nested ->
                                Stream.of(false, true)
                                    .flatMap(
                                        append ->
                                            Stream.of(false, true)
                                                .filter(
                                                    compiled -> append || !mode.equals("global"))
                                                .map(
                                                    compiled ->
                                                        Arguments.of(
                                                            mode, nested, append, compiled))))));
  }

  @ParameterizedTest
  @MethodSource("cases")
  void stockGroupingKeysKeepTheirBinaryRowInputBoundary(
      String mode, boolean nested, boolean append, boolean compiled) throws Exception {
    var config = new Configuration();
    config.setString("streamfusion.native.enabled", "true");
    config.setString("table.exec.mini-batch.enabled", Boolean.toString(!mode.equals("eager")));
    config.setString("table.exec.mini-batch.size", "3");
    config.setString("table.exec.mini-batch.allow-latency", "1 h");
    config.setString(
        "table.optimizer.agg-phase-strategy", mode.equals("global") ? "TWO_PHASE" : "ONE_PHASE");
    config.setString("parallelism.default", "2");
    var table = NativePlannerTestEnvironment.create(config, CanonicalFixtureFactory.class);
    table.executeSql(
        "CREATE TABLE input_table(k "
            + CanonicalRowKeyFixture.sql(nested)
            + ",v BIGINT) WITH ('connector'='canonical-fixture','insert-only'='"
            + append
            + "')");
    table.executeSql(
        "CREATE TABLE output_table(k "
            + CanonicalRowKeyFixture.sql(nested)
            + ",s BIGINT,n BIGINT)"
            + " WITH ('connector'='blackhole')");
    String query =
        "INSERT INTO output_table SELECT k,SUM(v),COUNT(DISTINCT v)"
            + " FROM input_table GROUP BY k";
    var planner = (PlannerBase) ((TableEnvironmentImpl) table).getPlanner();
    var plan = compiled ? table.compilePlanSql(query) : null;
    String portable = plan == null ? "" : plan.asJsonString();
    var transformations =
        compiled
            ? planner.translatePlan(planner.loadPlan(PlanReference.fromJsonString(portable)))
            : CanonicalRegularJoinPlanTest.translate(planner, query);
    var graph = CanonicalRegularJoinPlanTest.graph(planner, table, transformations);
    var operators =
        graph.getStreamNodes().stream()
            .filter(node -> node.getOperatorFactory() instanceof SimpleOperatorFactory)
            .map(node -> ((SimpleOperatorFactory<?>) node.getOperatorFactory()).getOperator())
            .collect(java.util.stream.Collectors.toList());
    var owners =
        operators.stream()
            .filter(NativeColumnarGroupAggregateOperator.class::isInstance)
            .collect(java.util.stream.Collectors.toList());
    assertEquals(1, owners.size(), graph.getStreamingPlanAsJSON());
    assertInstanceOf(
        StockGroupTypedStateContract.class,
        GroupAggregateStockTestFixture.field(owners.get(0), "stockStateContract"));
    assertEquals(
        mode.equals("global") ? 1 : 0,
        operators.stream()
            .filter(NativeColumnarLocalGroupAggregateOperator.class::isInstance)
            .count());
    assertTrue(
        operators.stream()
            .filter(RowDataToArrowOperator.class::isInstance)
            .anyMatch(
                op -> {
                  try {
                    var schema =
                        org.apache.arrow.vector.types.pojo.Schema.deserializeMessage(
                            java.nio.ByteBuffer.wrap(
                                (byte[]) GroupAggregateStockTestFixture.field(op, "arrowSchema")));
                    return schema.toJson().contains("streamfusion.nested-row-binary");
                  } catch (Exception failure) {
                    throw new AssertionError(failure);
                  }
                }),
        "The real grouping boundary must retain original ROW bytes");
    assertFalse(portable.contains("tech.streamfusion"));
    if (compiled) assertEquals(portable, plan.asJsonString());
  }
}
