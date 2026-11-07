package tech.streamfusion.planner;

import static org.junit.jupiter.api.Assertions.*;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.table.api.PlanReference;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.NativePlannerTestEnvironment;
import tech.streamfusion.operator.NativeColumnarGroupAggregateOperator;
import tech.streamfusion.state.StockGroupArithmeticStateContract;

class CanonicalGroupArithmeticAdmissionTest {
  @ParameterizedTest
  @ValueSource(
      strings = {
        "min-integer",
        "distinct",
        "filter",
        "mini",
        "two-phase",
        "empty",
        "composite",
        "floating",
        "integer"
      })
  void compiledContractsSelectTheVerifiedScalarOrTypedStateLayout(String mode) throws Exception {
    var table = environment(mode);
    boolean mini = mode.equals("mini") || mode.equals("two-phase");
    table.getConfig().set("table.exec.mini-batch.enabled", Boolean.toString(mini));
    if (mini) {
      table.getConfig().set("table.exec.mini-batch.allow-latency", "1 s");
      table.getConfig().set("table.exec.mini-batch.size", "3");
    }
    if (mode.equals("two-phase"))
      table.getConfig().set("table.optimizer.agg-phase-strategy", "TWO_PHASE");
    String query;
    if (mode.equals("empty"))
      query = "SELECT k,CAST(0 AS BIGINT),CAST(0 AS BIGINT) FROM fixture.db.input_table GROUP BY k";
    else if (mode.equals("composite"))
      query = "SELECT k,v,COUNT(*) FROM fixture.db.input_table GROUP BY k,v";
    else
      query =
          "SELECT k,COUNT(*),"
              + (mode.equals("min-integer")
                  ? "MIN(v)"
                  : mode.equals("distinct")
                      ? "COUNT(DISTINCT v)"
                      : mode.equals("filter") ? "SUM(v) FILTER (WHERE v>0)" : "SUM(v)")
              + " FROM fixture.db.input_table GROUP BY k";
    String sql = "INSERT INTO output_table " + query;
    var plan = table.compilePlanSql(sql);
    var planner = (PlannerBase) ((TableEnvironmentImpl) table).getPlanner();
    var graph =
        CanonicalRegularJoinPlanTest.graph(
            planner,
            table,
            planner.translatePlan(
                planner.loadPlan(PlanReference.fromJsonString(plan.asJsonString()))));
    var nativeOwners =
        graph.getStreamNodes().stream()
            .filter(node -> node.getOperatorFactory() instanceof SimpleOperatorFactory)
            .map(node -> ((SimpleOperatorFactory<?>) node.getOperatorFactory()).getOperator())
            .filter(NativeColumnarGroupAggregateOperator.class::isInstance)
            .collect(java.util.stream.Collectors.toList());
    if (mode.equals("min-integer")) {
      assertEquals(1, nativeOwners.size(), graph.getStreamingPlanAsJSON());
      var contract =
          assertInstanceOf(
              tech.streamfusion.state.StockGroupExtremaStateContract.class,
              GroupAggregateStockTestFixture.field(nativeOwners.get(0), "stockStateContract"));
      assertArrayEquals(
          new int[] {3, 38}, (int[]) GroupAggregateStockTestFixture.field(contract, "kinds"));
      var mapNames = (String[]) GroupAggregateStockTestFixture.field(contract, "mapNames");
      assertEquals(2, mapNames.length);
      assertNull(mapNames[0]);
      assertNotNull(mapNames[1]);
    } else if (java.util.Set.of("distinct", "filter", "composite", "floating", "integer")
        .contains(mode)) {
      assertEquals(1, nativeOwners.size(), graph.getStreamingPlanAsJSON());
      var operator = nativeOwners.get(0);
      var contract =
          assertInstanceOf(
              tech.streamfusion.state.StockGroupTypedStateContract.class,
              GroupAggregateStockTestFixture.field(operator, "stockStateContract"));
      assertEquals(
          mode.equals("composite") ? 2 : 1,
          ((org.apache.flink.table.types.logical.RowType)
                  GroupAggregateStockTestFixture.field(contract, "keyType"))
              .getFieldCount());
      var views = (java.util.List<?>) GroupAggregateStockTestFixture.field(contract, "views");
      assertEquals(mode.equals("distinct") ? 1 : 0, views.size());
      if (mode.equals("filter"))
        assertTrue(
            java.util.Arrays.stream(
                    (int[]) GroupAggregateStockTestFixture.field(operator, "filterColumns"))
                .anyMatch(column -> column >= 0));
      if (mode.equals("floating") || mode.equals("integer"))
        assertEquals(
            mode.equals("floating") ? 1 : 2,
            ((int[]) GroupAggregateStockTestFixture.field(operator, "valueTypes"))[1]);
    } else {
      assertTrue(nativeOwners.isEmpty(), graph.getStreamingPlanAsJSON());
      assertTrue(
          graph.getStreamNodes().stream()
              .anyMatch(node -> node.getOperatorName().contains("GroupAggregate")),
          graph.getStreamingPlanAsJSON());
    }
    assertFalse(plan.asJsonString().contains("tech.streamfusion"));
  }

  @ParameterizedTest
  @CsvSource({
    "false,0,false",
    "true,0,false",
    "false,3600000,false",
    "true,3600000,false",
    "false,0,true",
    "true,0,true",
    "false,3600000,true",
    "true,3600000,true"
  })
  void actualStockTtlHintOverridesTheJobDefault(boolean compiled, long defaultTtl, boolean append)
      throws Exception {
    var table = environment(append ? "hint-append" : "hint");
    table.getConfig().set("table.exec.state.ttl", defaultTtl + " ms");
    String sql =
        "INSERT INTO output_table SELECT /*+ STATE_TTL('input_table'='1 s') */"
            + " k,SUM(v),AVG(v) FROM fixture.db.input_table GROUP BY k";
    var planner = (PlannerBase) ((TableEnvironmentImpl) table).getPlanner();
    var transformations =
        compiled
            ? planner.translatePlan(
                planner.loadPlan(
                    PlanReference.fromJsonString(table.compilePlanSql(sql).asJsonString())))
            : CanonicalRegularJoinPlanTest.translate(planner, sql);
    var graph = CanonicalRegularJoinPlanTest.graph(planner, table, transformations);
    var operator =
        graph.getStreamNodes().stream()
            .filter(node -> node.getOperatorFactory() instanceof SimpleOperatorFactory)
            .map(node -> ((SimpleOperatorFactory<?>) node.getOperatorFactory()).getOperator())
            .filter(NativeColumnarGroupAggregateOperator.class::isInstance)
            .findFirst()
            .orElseThrow();
    assertEquals(1000L, GroupAggregateStockTestFixture.field(operator, "stateTtlMillis"));
    var contract =
        assertInstanceOf(
            StockGroupArithmeticStateContract.class,
            GroupAggregateStockTestFixture.field(operator, "stockStateContract"));
    assertEquals(true, GroupAggregateStockTestFixture.field(contract, "ttl"));
    assertEquals(
        append ? -1 : 4, GroupAggregateStockTestFixture.field(contract, "recordCountField"));
  }

  private static TableEnvironment environment(String mode) throws Exception {
    var config = new Configuration();
    config.setString("streamfusion.native.enabled", "true");
    var table = NativePlannerTestEnvironment.create(config);
    table.getConfig().set("table.exec.mini-batch.enabled", "false");
    table.getConfig().set("table.optimizer.agg-phase-strategy", "ONE_PHASE");
    table.registerCatalog(
        "fixture",
        new org.apache.flink.table.catalog.GenericInMemoryCatalog("fixture", "db") {
          @Override
          public java.util.Optional<org.apache.flink.table.factories.Factory> getFactory() {
            return java.util.Optional.of(new org.example.canonical.CanonicalFixtureFactory());
          }
        });
    table.useCatalog("fixture");
    String valueType =
        mode.equals("floating")
            ? "DOUBLE"
            : mode.equals("integer") || mode.equals("min-integer") ? "INT" : "BIGINT";
    table.executeSql(
        "CREATE TABLE input_table(k BIGINT,v "
            + valueType
            + ") WITH"
            + " ('connector'='canonical-fixture','input'='unused.csv','ready'='unused.ready',"
            + "'insert-only'='"
            + mode.endsWith("append")
            + "')");
    table.useCatalog("default_catalog");
    table.executeSql(
        "CREATE TABLE output_table(k BIGINT,a BIGINT,b "
            + (mode.equals("floating") ? "DOUBLE" : "BIGINT")
            + ") WITH ('connector'='blackhole')");
    return table;
  }
}
