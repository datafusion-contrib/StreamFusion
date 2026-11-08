package tech.streamfusion.planner;

import static org.junit.jupiter.api.Assertions.*;

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
import tech.streamfusion.operator.NativeColumnarTopNOperator;

class CanonicalUpdateRankAdmissionTest {
  static Stream<Arguments> rejected() {
    return Stream.of("ttl", "offset", "three-sort", "wide", "variable")
        .flatMap(mode -> Stream.of(false, true).map(compiled -> Arguments.of(mode, compiled)));
  }

  @ParameterizedTest
  @MethodSource("rejected")
  void unqualifiedUpdatingStateRemainsOnTheStockRank(String mode, boolean compiled)
      throws Exception {
    var config = new Configuration();
    config.setString("streamfusion.native.enabled", "true");
    config.setString("table.exec.state.ttl", mode.equals("ttl") ? "1 h" : "0 ms");
    var table = NativePlannerTestEnvironment.create(config);
    table.executeSql("CREATE TABLE input_table(k BIGINT,v BIGINT) WITH ('connector'='datagen')");
    table.executeSql(
        "CREATE TABLE output_table(k BIGINT,v BIGINT,n BIGINT) WITH ('connector'='blackhole')");
    String group =
        "SELECT k,v,COUNT(*) AS c"
            + (mode.equals("wide") ? ",SUM(CAST(v AS DOUBLE)) AS d" : "")
            + (mode.equals("variable") ? ",COALESCE(MOD(ABS(k),3)+1,2) AS b" : "")
            + " FROM input_table GROUP BY k,v";
    String query =
        "INSERT INTO output_table SELECT k,"
            + (mode.equals("wide") ? "CAST(d AS BIGINT)" : "v")
            + ",rn FROM (SELECT *,"
            + "ROW_NUMBER() OVER (PARTITION BY k ORDER BY c DESC"
            + (mode.equals("three-sort") ? ",k,v" : "")
            + ") AS rn FROM ("
            + group
            + ")) WHERE "
            + (mode.equals("offset") ? "rn>=2 AND " : "")
            + "rn<="
            + (mode.equals("variable") ? "b" : "3");
    var planner = (PlannerBase) ((TableEnvironmentImpl) table).getPlanner();
    var plan = compiled ? table.compilePlanSql(query) : null;
    var transformations =
        compiled
            ? planner.translatePlan(
                planner.loadPlan(PlanReference.fromJsonString(plan.asJsonString())))
            : CanonicalRegularJoinPlanTest.translate(planner, query);
    var graph = CanonicalRegularJoinPlanTest.graph(planner, table, transformations);
    assertFalse(
        graph.getStreamNodes().stream()
            .anyMatch(
                node ->
                    node.getOperatorFactory() instanceof SimpleOperatorFactory
                        && ((SimpleOperatorFactory<?>) node.getOperatorFactory()).getOperator()
                            instanceof NativeColumnarTopNOperator),
        graph.getStreamingPlanAsJSON());
    assertTrue(
        graph.getStreamNodes().stream().anyMatch(node -> node.getOperatorName().contains("Rank")),
        graph.getStreamingPlanAsJSON());
  }
}
