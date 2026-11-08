package tech.streamfusion.planner;

import static org.junit.jupiter.api.Assertions.*;
import static tech.streamfusion.planner.GroupAggregateStockTestFixture.field;

import java.util.Optional;
import java.util.stream.Stream;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.table.api.PlanReference;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.*;
import tech.streamfusion.NativePlannerTestEnvironment;
import tech.streamfusion.operator.NativeColumnarGroupAggregateOperator;

class CanonicalGroupWideIndexPlanTest {
  static Stream<Arguments> cases() {
    return Stream.of(254, 255)
        .flatMap(slot -> Stream.of(false, true).map(compiled -> Arguments.of(slot, compiled)));
  }

  @ParameterizedTest(name = "distinctSlot={0},compiled={1}")
  @MethodSource("cases")
  void wideDistinctUsesStockFallbackBeforeAnUnencodableNativeBinding(int slot, boolean compiled)
      throws Exception {
    var config = new Configuration();
    config.setString("streamfusion.native.enabled", "true");
    config.setString("pipeline.max-parallelism", "128");
    var table = NativePlannerTestEnvironment.create(config);
    table.getConfig().set("table.exec.mini-batch.enabled", "false");
    table.registerCatalog(
        "fixture",
        new org.apache.flink.table.catalog.GenericInMemoryCatalog("fixture", "db") {
          @Override
          public Optional<org.apache.flink.table.factories.Factory> getFactory() {
            return Optional.of(new org.example.canonical.CanonicalFixtureFactory());
          }
        });
    table.useCatalog("fixture");
    var inputs = new StringBuilder("k BIGINT,m INT");
    var outputs = new StringBuilder("k BIGINT");
    var select = new StringBuilder("k");
    for (int i = 0; i < slot; i++) {
      inputs.append(",v").append(i).append(" BIGINT");
      outputs.append(",s").append(i).append(" BIGINT");
      select.append(",SUM(v").append(i).append(")");
    }
    outputs.append(",n BIGINT");
    select.append(",COUNT(DISTINCT m)");
    table.executeSql(
        "CREATE TABLE input_table("
            + inputs
            + ") WITH ('connector'='canonical-fixture',"
            + "'input'='unused.csv','ready'='unused.ready','insert-only'='true')");
    table.useCatalog("default_catalog");
    table.executeSql("CREATE TABLE output_table(" + outputs + ") WITH ('connector'='blackhole')");
    String sql =
        "INSERT INTO output_table SELECT " + select + " FROM fixture.db.input_table GROUP BY k";
    var planner = (PlannerBase) ((TableEnvironmentImpl) table).getPlanner();
    var plan = compiled ? table.compilePlanSql(sql) : null;
    String json = plan == null ? "" : plan.asJsonString();
    assertFalse(json.contains("tech.streamfusion"));
    var transformations =
        compiled
            ? planner.translatePlan(planner.loadPlan(PlanReference.fromJsonString(json)))
            : CanonicalRegularJoinPlanTest.translate(planner, sql);
    var graph = CanonicalRegularJoinPlanTest.graph(planner, table, transformations);
    var nativeOwners =
        graph.getStreamNodes().stream()
            .filter(n -> n.getOperatorFactory() instanceof SimpleOperatorFactory)
            .map(n -> ((SimpleOperatorFactory<?>) n.getOperatorFactory()).getOperator())
            .filter(NativeColumnarGroupAggregateOperator.class::isInstance)
            .collect(java.util.stream.Collectors.toList());
    assertEquals(slot < 255 ? 1 : 0, nativeOwners.size(), graph.getStreamingPlanAsJSON());
    if (slot < 255) assertNotNull(field(nativeOwners.get(0), "stockStateContract"));
    else {
      long stockOwners =
          graph.getStreamNodes().stream()
              .filter(n -> n.getOperatorFactory() instanceof SimpleOperatorFactory)
              .map(n -> ((SimpleOperatorFactory<?>) n.getOperatorFactory()).getOperator())
              .filter(
                  op -> op instanceof org.apache.flink.streaming.api.operators.KeyedProcessOperator)
              .map(
                  op ->
                      ((org.apache.flink.streaming.api.operators.KeyedProcessOperator<?, ?, ?>) op)
                          .getUserFunction())
              .filter(
                  function ->
                      function.getClass()
                          == org.apache.flink.table.runtime.operators.aggregate.GroupAggFunction
                              .class)
              .count();
      assertEquals(1, stockOwners, "Unencodable state must keep the actual stock owner");
    }
    if (compiled) assertEquals(json, plan.asJsonString());
  }
}
