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
        "min",
        "distinct",
        "filter",
        "mini",
        "two-phase",
        "keyless",
        "composite",
        "append",
        "floating",
        "integer"
      })
  void compiledContractsOutsideTheQualifiedStateLayoutKeepStockAggregation(String mode)
      throws Exception {
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
    if (mode.equals("keyless"))
      query = "SELECT CAST(0 AS BIGINT),SUM(v),AVG(v) FROM fixture.db.input_table";
    else if (mode.equals("composite"))
      query = "SELECT k,v,COUNT(*) FROM fixture.db.input_table GROUP BY k,v";
    else
      query =
          "SELECT k,COUNT(*),"
              + (mode.equals("min")
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
    assertFalse(
        graph.getStreamNodes().stream()
            .anyMatch(
                node ->
                    node.getOperatorFactory() instanceof SimpleOperatorFactory
                        && ((SimpleOperatorFactory<?>) node.getOperatorFactory()).getOperator()
                            instanceof NativeColumnarGroupAggregateOperator),
        graph.getStreamingPlanAsJSON());
    assertTrue(
        graph.getStreamNodes().stream()
            .anyMatch(node -> node.getOperatorName().contains("GroupAggregate")),
        graph.getStreamingPlanAsJSON());
    assertFalse(plan.asJsonString().contains("tech.streamfusion"));
  }

  @ParameterizedTest
  @CsvSource({"false,0", "true,0", "false,3600000", "true,3600000"})
  void actualStockTtlHintOverridesTheJobDefault(boolean compiled, long defaultTtl)
      throws Exception {
    var table = environment("hint");
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
        mode.equals("floating") ? "DOUBLE" : mode.equals("integer") ? "INT" : "BIGINT";
    table.executeSql(
        "CREATE TABLE input_table(k BIGINT,v "
            + valueType
            + ") WITH"
            + " ('connector'='canonical-fixture','input'='unused.csv','ready'='unused.ready',"
            + "'insert-only'='"
            + mode.equals("append")
            + "')");
    table.useCatalog("default_catalog");
    table.executeSql(
        "CREATE TABLE output_table(k BIGINT,a BIGINT,b "
            + (mode.equals("floating") ? "DOUBLE" : "BIGINT")
            + ") WITH ('connector'='blackhole')");
    return table;
  }
}
