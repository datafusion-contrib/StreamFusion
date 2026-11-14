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
import tech.streamfusion.operator.NativeColumnarTopNOperator;
import tech.streamfusion.state.StockAppendTopNStateContract;
import tech.streamfusion.state.StockRetractingTopNStateContract;
import tech.streamfusion.state.StockUpdatingTopNStateContract;

class CanonicalRankRowKeyPlanTest {
  static Stream<Arguments> cases() {
    var cases = new ArrayList<Arguments>();
    for (String algorithm : new String[] {"append", "retract", "update"})
      for (String mode :
          new String[] {"fixed", "offset", "variable", "ttl", "array-key", "row-sort"})
        for (boolean nested : new boolean[] {false, true})
          for (boolean compiled : new boolean[] {false, true})
            cases.add(Arguments.of(algorithm, mode, nested, compiled));
    return tech.streamfusion.CanonicalTestMatrix.select(cases.stream());
  }

  @ParameterizedTest
  @MethodSource("cases")
  void rowPartitionsUseTheCapturedAlgorithmAndSelectiveInput(
      String algorithm, String mode, boolean nested, boolean compiled) throws Exception {
    var config = new Configuration();
    config.setString("streamfusion.native.enabled", "true");
    config.setString("table.exec.state.ttl", mode.equals("ttl") ? "1 h" : "0 ms");
    var table =
        NativePlannerTestEnvironment.create(
            config, org.example.canonical.CanonicalFixtureFactory.class);
    table.executeSql(
        "CREATE TABLE input_table(k "
            + (mode.equals("array-key")
                ? "ROW<i BIGINT,a ARRAY<BIGINT>>"
                : CanonicalRowKeyFixture.sql(nested))
            + ",v BIGINT,id BIGINT) WITH ('connector'='canonical-fixture','input'='unused.csv',"
            + "'insert-only'='"
            + !algorithm.equals("retract")
            + "')");
    table.executeSql(
        "CREATE TABLE output_table(k BIGINT,id BIGINT,r BIGINT)"
            + " WITH ('connector'='blackhole')");
    String input =
        algorithm.equals("update")
            ? "(SELECT k,v,id,COUNT(*) AS c FROM input_table GROUP BY k,v,id)"
            : "input_table";
    String bound = mode.equals("variable") ? "N" : "3";
    if (mode.equals("variable"))
      input = "(SELECT *,COALESCE(MOD(k.i,3)+1,0) AS N FROM " + input + ")";
    String query =
        "INSERT INTO output_table SELECT k.i,id,rn FROM (SELECT *,ROW_NUMBER() OVER"
            + " (PARTITION BY k ORDER BY "
            + (algorithm.equals("update") ? "c DESC," : "")
            + (mode.equals("row-sort") ? "k" : "v ASC NULLS LAST,id")
            + ") AS rn FROM "
            + input
            + ") WHERE rn<="
            + bound
            + (mode.equals("offset") ? " AND rn>=2" : "");
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
            .filter(NativeColumnarTopNOperator.class::isInstance)
            .collect(tech.streamfusion.compat.ListCollectors.toList());
    boolean admitted =
        !mode.equals("array-key")
            && !mode.equals("row-sort")
            && !(algorithm.equals("append") && mode.equals("offset"))
            && !(algorithm.equals("update") && mode.equals("ttl"));
    assertEquals(admitted ? 1 : 0, owners.size(), graph.getStreamingPlanAsJSON());
    if (admitted) {
      var owner = owners.get(0);
      var contract = field(owner, "stockStateContract");
      assertInstanceOf(
          algorithm.equals("append")
              ? StockAppendTopNStateContract.class
              : algorithm.equals("update")
                  ? StockUpdatingTopNStateContract.class
                  : StockRetractingTopNStateContract.class,
          contract);
      assertArrayEquals(
          (int[]) field(owner, "partitionColumns"), (int[]) field(contract, "partitionColumns"));
      assertTrue(
          graph.getStreamNodes().stream()
              .filter(node -> node.getOperatorFactory() instanceof SimpleOperatorFactory)
              .map(node -> ((SimpleOperatorFactory<?>) node.getOperatorFactory()).getOperator())
              .filter(tech.streamfusion.operator.RowDataToArrowOperator.class::isInstance)
              .anyMatch(
                  entry -> {
                    try {
                      return org.apache.arrow.vector.types.pojo.Schema.deserializeMessage(
                              java.nio.ByteBuffer.wrap((byte[]) field(entry, "arrowSchema")))
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
