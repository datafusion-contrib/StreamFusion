package tech.streamfusion.planner.stream;

import static org.junit.jupiter.api.Assertions.*;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.table.api.PlanReference;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import tech.streamfusion.NativePlannerTestEnvironment;
import tech.streamfusion.operator.AbstractNativeStatefulOperator;
import tech.streamfusion.operator.NativeColumnarTopNOperator;
import tech.streamfusion.state.StockRetractingTopNStateContract;

class CanonicalRetractingRankProctimePlanTest {
  @ParameterizedTest
  @CsvSource({
    "false,false,ALWAYS", "false,true,ALWAYS", "true,false,ALWAYS", "true,true,ALWAYS",
    "false,false,DISABLED", "false,true,DISABLED", "true,false,DISABLED", "true,true,DISABLED"
  })
  void retainedIndicatorBindsButOrdinaryTimestampKeepsItsStockOwner(
      boolean compiled, boolean ordinaryTimestamp, String identities) throws Exception {
    var config = new Configuration();
    config.setString("streamfusion.native.enabled", "true");
    config.setString("table.exec.uid.generation", identities);
    var table =
        NativePlannerTestEnvironment.create(
            config, org.example.canonical.CanonicalFixtureFactory.class);
    table.executeSql(
        "CREATE TABLE input_table(k BIGINT,v BIGINT,pt "
            + (ordinaryTimestamp ? "TIMESTAMP_LTZ(3)" : "AS PROCTIME()")
            + ") WITH ('connector'='canonical-fixture','input'='unused.csv')");
    table.executeSql(
        "CREATE TABLE output_table(k BIGINT,v BIGINT,n BIGINT) WITH ('connector'='blackhole')");
    String query =
        "INSERT INTO output_table SELECT k,v,UNIX_TIMESTAMP(CAST(pt AS STRING))"
            + " FROM (SELECT *,ROW_NUMBER() OVER (PARTITION BY k ORDER BY v ASC) AS rn"
            + " FROM input_table) WHERE rn<=3";
    var planner = (PlannerBase) ((TableEnvironmentImpl) table).getPlanner();
    var plan = compiled ? table.compilePlanSql(query) : null;
    String portable = plan == null ? "" : plan.asJsonString();
    assertFalse(portable.contains("tech.streamfusion"));
    var transforms =
        compiled
            ? planner.translatePlan(planner.loadPlan(PlanReference.fromJsonString(portable)))
            : CanonicalRegularJoinPlanTest.translate(planner, query);
    var graph = CanonicalRegularJoinPlanTest.graph(planner, table, transforms);
    var owners =
        graph.getStreamNodes().stream()
            .filter(
                node ->
                    node.getOperatorFactory() instanceof SimpleOperatorFactory
                        && ((SimpleOperatorFactory<?>) node.getOperatorFactory()).getOperator()
                            instanceof NativeColumnarTopNOperator)
            .collect(java.util.stream.Collectors.toList());
    assertEquals(ordinaryTimestamp ? 0 : 1, owners.size(), graph.getStreamingPlanAsJSON());
    if (!ordinaryTimestamp) {
      var contractField =
          AbstractNativeStatefulOperator.class.getDeclaredField("stockStateContract");
      contractField.setAccessible(true);
      var contract =
          assertInstanceOf(
              StockRetractingTopNStateContract.class,
              contractField.get(
                  ((SimpleOperatorFactory<?>) owners.get(0).getOperatorFactory()).getOperator()));
      var inputField = StockRetractingTopNStateContract.class.getDeclaredField("rowType");
      inputField.setAccessible(true);
      var input = (RowType) inputField.get(contract);
      assertTrue(
          input.getChildren().stream().anyMatch(LogicalTypeChecks::isProctimeAttribute),
          input.toString());
      assertEquals(
          java.util.Set.of("data-state", "sorted-map"), contract.stateDescriptors().keySet());
      assertTrue(graph.getStreamingPlanAsJSON().contains("RowDataToArrow"));
      assertTrue(graph.getStreamingPlanAsJSON().contains("ArrowToRowData"));
    }
    if (compiled) assertEquals(portable, plan.asJsonString());
  }
}
