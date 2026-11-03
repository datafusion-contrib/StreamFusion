package tech.streamfusion.planner;

import static org.junit.jupiter.api.Assertions.*;

import java.util.stream.Stream;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.table.api.PlanReference;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.runtime.operators.rank.RetractableTopNFunction;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import tech.streamfusion.NativePlannerTestEnvironment;
import tech.streamfusion.operator.NativeColumnarTopNOperator;

class CanonicalRetractingRankAdmissionTest {
  static Stream<Arguments> cases() {
    return Stream.of("wide", "three-sort", "nullable-bound", "unaligned")
        .flatMap(mode -> Stream.of(false, true).map(compiled -> Arguments.of(mode, compiled)));
  }

  @ParameterizedTest
  @MethodSource("cases")
  void unqualifiedDirectChangelogRanksKeepTheirOriginalStockState(String mode, boolean compiled)
      throws Exception {
    var config = new Configuration();
    config.setString("streamfusion.native.enabled", "true");
    if (mode.equals("unaligned"))
      config.setString("execution.checkpointing.unaligned.enabled", "true");
    var table = NativePlannerTestEnvironment.create(config);
    table.registerCatalog(
        "fixture_catalog",
        new org.apache.flink.table.catalog.GenericInMemoryCatalog("fixture_catalog", "db") {
          @Override
          public java.util.Optional<org.apache.flink.table.factories.Factory> getFactory() {
            return java.util.Optional.of(new org.example.canonical.CanonicalFixtureFactory());
          }
        });
    table.useCatalog("fixture_catalog");
    table.executeSql(
        "CREATE TABLE input_table(k BIGINT,v "
            + (mode.equals("wide") ? "DOUBLE" : "BIGINT")
            + ",b BIGINT) WITH"
            + " ('connector'='canonical-fixture','input'='unused.csv','ready'='unused.ready')");
    table.useCatalog("default_catalog");
    table.executeSql(
        "CREATE TABLE output_table(k BIGINT,v BIGINT,n BIGINT) WITH ('connector'='blackhole')");
    String query =
        "INSERT INTO output_table SELECT k,CAST(v AS BIGINT),rn FROM (SELECT *,ROW_NUMBER() OVER"
            + " (PARTITION BY k ORDER BY v"
            + (mode.equals("three-sort") ? ",b,k" : "")
            + ") AS rn FROM fixture_catalog.db.input_table) WHERE rn<="
            + (mode.equals("nullable-bound") ? "b" : "3");
    var planner = (PlannerBase) ((TableEnvironmentImpl) table).getPlanner();
    var plan = compiled ? table.compilePlanSql(query) : null;
    var transforms =
        compiled
            ? planner.translatePlan(
                planner.loadPlan(PlanReference.fromJsonString(plan.asJsonString())))
            : CanonicalRegularJoinPlanTest.translate(planner, query);
    var graph = CanonicalRegularJoinPlanTest.graph(planner, table, transforms);
    assertFalse(
        graph.getStreamNodes().stream()
            .anyMatch(
                node ->
                    node.getOperatorFactory() instanceof SimpleOperatorFactory
                        && ((SimpleOperatorFactory<?>) node.getOperatorFactory()).getOperator()
                            instanceof NativeColumnarTopNOperator),
        graph.getStreamingPlanAsJSON());
    assertTrue(
        graph.getStreamNodes().stream()
            .anyMatch(
                node -> {
                  if (!(node.getOperatorFactory() instanceof SimpleOperatorFactory)) return false;
                  Object operator =
                      ((SimpleOperatorFactory<?>) node.getOperatorFactory()).getOperator();
                  return operator instanceof KeyedProcessOperator
                      && ((KeyedProcessOperator<?, ?, ?>) operator).getUserFunction()
                          instanceof RetractableTopNFunction;
                }),
        graph.getStreamingPlanAsJSON());
  }
}
