package tech.streamfusion.planner.stream;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Base64;
import java.util.Map;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.table.api.PlanReference;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.util.InstantiationUtil;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import tech.streamfusion.NativePlannerTestEnvironment;
import tech.streamfusion.operator.NativeColumnarFirstNOperator;
import tech.streamfusion.operator.NativeColumnarUpdatingJoinOperator;

class CanonicalSharedFirstNPlanTest {
  @ParameterizedTest
  @CsvSource({"false,true", "true,true", "false,false", "true,false"})
  void sharedCountersRetainTheirActualStockOwners(boolean compiled, boolean sameLimit)
      throws Exception {
    var config = new Configuration();
    config.setString("streamfusion.native.enabled", "true");
    config.setString("parallelism.default", "2");
    config.setString("pipeline.max-parallelism", "257");
    config.setString("table.exec.uid.generation", "DISABLED");
    var table = NativePlannerTestEnvironment.create(config);
    table.executeSql(
        "CREATE TABLE input_table(k BIGINT,v BIGINT,pt AS PROCTIME()) WITH"
            + " ('connector'='datagen')");
    table.executeSql(
        "CREATE TABLE output_table(k BIGINT,l BIGINT,r BIGINT) WITH ('connector'='blackhole')");
    table.executeSql(
        "CREATE TEMPORARY VIEW first_input AS SELECT k,v FROM (SELECT k,v,ROW_NUMBER() OVER"
            + " (PARTITION BY k ORDER BY pt) AS rn FROM input_table) WHERE rn<=3");
    table.executeSql(
        "CREATE TEMPORARY VIEW other_input AS SELECT k,v FROM (SELECT k,v,ROW_NUMBER() OVER"
            + " (PARTITION BY k ORDER BY pt) AS rn FROM input_table) WHERE rn<="
            + (sameLimit ? 3 : 2));
    String query =
        "INSERT INTO output_table SELECT l.k,l.v,r.v FROM first_input l JOIN other_input r ON"
            + " l.k=r.k";
    var planner = (PlannerBase) ((TableEnvironmentImpl) table).getPlanner();
    var transformations =
        compiled
            ? planner.translatePlan(
                planner.loadPlan(
                    PlanReference.fromJsonString(table.compilePlanSql(query).asJsonString())))
            : CanonicalRegularJoinPlanTest.translate(planner, query);
    var graph = CanonicalRegularJoinPlanTest.graph(planner, table, transformations);
    long counters =
        graph.getStreamNodes().stream()
            .filter(
                node ->
                    node.getOperatorFactory() instanceof SimpleOperatorFactory
                        && ((SimpleOperatorFactory<?>) node.getOperatorFactory()).getOperator()
                            instanceof NativeColumnarFirstNOperator)
            .count();
    assertEquals(sameLimit ? 1 : 2, counters, graph.getStreamingPlanAsJSON());
    assertEquals(
        1,
        graph.getStreamNodes().stream()
            .filter(
                node ->
                    node.getOperatorFactory() instanceof SimpleOperatorFactory
                        && ((SimpleOperatorFactory<?>) node.getOperatorFactory()).getOperator()
                            instanceof NativeColumnarUpdatingJoinOperator)
            .count(),
        graph.getStreamingPlanAsJSON());
    Map<?, ?> registrations =
        InstantiationUtil.deserializeObject(
            Base64.getDecoder()
                .decode(
                    table
                        .getConfig()
                        .getConfiguration()
                        .getString("streamfusion.canonical.state-contracts", "")),
            getClass().getClassLoader());
    assertEquals(sameLimit ? 2 : 3, registrations.size());
    assertTrue(
        Boolean.parseBoolean(
            table
                .getConfig()
                .getConfiguration()
                .getString("streamfusion.canonical.metadata-projection-active", "false")));
  }
}
