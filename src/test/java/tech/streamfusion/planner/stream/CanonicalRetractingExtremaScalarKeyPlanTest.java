package tech.streamfusion.planner.stream;

import static org.junit.jupiter.api.Assertions.*;
import static tech.streamfusion.planner.stream.GroupAggregateStockTestFixture.field;

import java.util.stream.Stream;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.table.api.PlanReference;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import tech.streamfusion.NativePlannerTestEnvironment;
import tech.streamfusion.operator.NativeColumnarGroupAggregateOperator;
import tech.streamfusion.state.StockGroupExtremaStateContract;

class CanonicalRetractingExtremaScalarKeyPlanTest {
  static Stream<Arguments> cases() {
    return tech.streamfusion.CanonicalTestMatrix.select(
        Stream.concat(
                CanonicalExtremaScalarKeyFixture.schemas(),
                Stream.of("ROW<x BIGINT>", "ARRAY<BIGINT>", "ROW<x BIGINT,y VARCHAR(10)>"))
            .flatMap(
                schema ->
                    Stream.of(false, true)
                        .flatMap(
                            compiled ->
                                Stream.of(false, true)
                                    .flatMap(
                                        mini ->
                                            Stream.of(0L, 1000L)
                                                .map(
                                                    ttl ->
                                                        Arguments.of(
                                                            schema, compiled, mini, ttl))))));
  }

  @ParameterizedTest
  @MethodSource("cases")
  void onlyCompleteScalarKeyCodecsBindReleasedExtremaViews(
      String definition, boolean compiled, boolean mini, long ttl) throws Exception {
    var keys = CanonicalExtremaScalarKeyFixture.schema(definition);
    String fields =
        java.util.stream.IntStream.range(0, keys.getFieldCount())
            .mapToObj(index -> "k" + index + " " + keys.getTypeAt(index).asSerializableString())
            .collect(java.util.stream.Collectors.joining(","));
    String names =
        java.util.stream.IntStream.range(0, keys.getFieldCount())
            .mapToObj(index -> "k" + index)
            .collect(java.util.stream.Collectors.joining(","));
    var config = new Configuration();
    config.setString("streamfusion.native.enabled", "true");
    config.setString("pipeline.max-parallelism", "257");
    config.setString("parallelism.default", "2");
    config.setString("table.exec.state.ttl", ttl + " ms");
    config.setString("table.exec.mini-batch.enabled", Boolean.toString(mini));
    config.setString("table.exec.mini-batch.size", "7");
    config.setString("table.exec.mini-batch.allow-latency", "1 h");
    config.setString("table.optimizer.agg-phase-strategy", "ONE_PHASE");
    var table =
        NativePlannerTestEnvironment.create(
            config, org.example.canonical.CanonicalFixtureFactory.class);
    table.executeSql(
        "CREATE TABLE input_table("
            + fields
            + ",v BIGINT) WITH"
            + " ('connector'='canonical-fixture','input'='unused.csv','insert-only'='false')");
    table.executeSql(
        "CREATE TABLE output_table("
            + fields
            + ",c BIGINT,lo BIGINT,hi BIGINT) WITH ('connector'='blackhole')");
    String sql =
        "INSERT INTO output_table SELECT "
            + names
            + ",COUNT(v),MIN(v),MAX(v) FROM input_table GROUP BY "
            + names;
    var planner = (PlannerBase) ((TableEnvironmentImpl) table).getPlanner();
    var plan = compiled ? table.compilePlanSql(sql) : null;
    String portable = plan == null ? "" : plan.asJsonString();
    var transforms =
        compiled
            ? planner.translatePlan(planner.loadPlan(PlanReference.fromJsonString(portable)))
            : CanonicalRegularJoinPlanTest.translate(planner, sql);
    var graph = CanonicalRegularJoinPlanTest.graph(planner, table, transforms);
    var bound = new java.util.ArrayList<Object>();
    for (var node : graph.getStreamNodes()) {
      if (!(node.getOperatorFactory() instanceof SimpleOperatorFactory)) continue;
      var operator = ((SimpleOperatorFactory<?>) node.getOperatorFactory()).getOperator();
      if (!(operator instanceof NativeColumnarGroupAggregateOperator)) continue;
      var contract = field(operator, "stockStateContract");
      if (contract == null) continue;
      bound.add(contract);
      assertEquals(257, tech.streamfusion.compat.FlinkPlanCompat.maximumParallelism(node));
      assertInstanceOf(StockGroupExtremaStateContract.class, contract);
      var expectedKeys =
          keys.getChildren().stream()
              .map(
                  type ->
                      type.is(
                              org.apache.flink.table.types.logical.LogicalTypeRoot
                                  .TIME_WITHOUT_TIME_ZONE)
                          ? new org.apache.flink.table.types.logical.TimeType(type.isNullable(), 0)
                          : type)
              .collect(tech.streamfusion.compat.ListCollectors.toList());
      assertEquals(expectedKeys, ((RowType) field(contract, "keyType")).getChildren());
    }
    boolean admitted = CanonicalExtremaScalarKeyFixture.schemas().anyMatch(definition::equals);
    assertEquals(admitted ? 1 : 0, bound.size(), graph.getStreamingPlanAsJSON());
    assertFalse(portable.contains("tech.streamfusion"));
    if (compiled) assertEquals(portable, plan.asJsonString());
  }
}
