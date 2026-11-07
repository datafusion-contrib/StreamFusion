package tech.streamfusion.planner;

import static org.junit.jupiter.api.Assertions.*;

import java.util.stream.Stream;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.table.api.PlanReference;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.runtime.operators.sort.RowTimeSortOperator;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import tech.streamfusion.NativePlannerTestEnvironment;
import tech.streamfusion.operator.NativeColumnarTemporalSortOperator;

class CanonicalTemporalSortAdmissionTest {
  static Stream<Arguments> cases() {
    return Stream.of("secondary", "proctime", "async", "unaligned")
        .flatMap(
            mode ->
                Stream.of(false, true)
                    .flatMap(
                        compiled ->
                            Stream.of("DISABLED", "ALWAYS")
                                .map(uids -> Arguments.of(mode, compiled, uids))));
  }

  @ParameterizedTest
  @MethodSource("cases")
  void unqualifiedContractsKeepTheirStockSortPath(String mode, boolean compiled, String uids)
      throws Exception {
    var config = new Configuration();
    config.setString("streamfusion.native.enabled", "true");
    config.setString("table.exec.uid.generation", uids);
    if (mode.equals("async")) config.setString("table.exec.async-state.enabled", "true");
    if (mode.equals("unaligned"))
      config.setString("execution.checkpointing.unaligned.enabled", "true");
    var table = NativePlannerTestEnvironment.create(config);
    String time = "TIMESTAMP(3)";
    table.executeSql(
        "CREATE TABLE sort_input(k BIGINT,v BIGINT,rt "
            + time
            + ",pt AS PROCTIME(),WATERMARK FOR rt AS rt) WITH ('connector'='datagen')");
    table.executeSql(
        "CREATE TABLE sort_output(k BIGINT,v BIGINT,rt "
            + time
            + ") WITH ('connector'='blackhole')");
    String query =
        "INSERT INTO sort_output SELECT k,v,rt FROM sort_input ORDER BY "
            + (mode.equals("proctime") ? "pt" : "rt")
            + (mode.equals("secondary") ? ",v DESC" : "");
    var plan = table.compilePlanSql(query);
    String portable = plan.asJsonString();
    var planner = (PlannerBase) ((TableEnvironmentImpl) table).getPlanner();
    var transforms =
        compiled
            ? planner.translatePlan(planner.loadPlan(PlanReference.fromJsonString(portable)))
            : CanonicalRegularJoinPlanTest.translate(planner, query);
    var graph = CanonicalRegularJoinPlanTest.graph(planner, table, transforms);
    assertFalse(
        graph.getStreamNodes().stream()
            .anyMatch(
                node ->
                    node.getOperatorFactory() instanceof SimpleOperatorFactory
                        && ((SimpleOperatorFactory<?>) node.getOperatorFactory()).getOperator()
                            instanceof NativeColumnarTemporalSortOperator),
        graph.getStreamingPlanAsJSON());
    if (!mode.equals("proctime"))
      assertTrue(
          graph.getStreamNodes().stream()
              .anyMatch(
                  node ->
                      node.getOperatorFactory() instanceof SimpleOperatorFactory
                          && ((SimpleOperatorFactory<?>) node.getOperatorFactory()).getOperator()
                              instanceof RowTimeSortOperator),
          graph.getStreamingPlanAsJSON());
    assertEquals(portable, plan.asJsonString());
    assertFalse(portable.contains("tech.streamfusion"));
  }

  static Stream<Arguments> invalidTimeAttributes() {
    return Stream.of(false, true)
        .flatMap(
            nativeEnabled ->
                Stream.of("TIMESTAMP(4)", "TIMESTAMP_LTZ(6)")
                    .map(type -> Arguments.of(nativeEnabled, type)));
  }

  @ParameterizedTest
  @MethodSource("invalidTimeAttributes")
  void unsupportedWatermarkPrecisionKeepsReleasedValidation(boolean nativeEnabled, String type) throws Exception {
    var config = new Configuration();
    config.setString("streamfusion.native.enabled", Boolean.toString(nativeEnabled));
    var table = NativePlannerTestEnvironment.create(config);
    var failure =
        assertThrows(
            org.apache.flink.table.api.ValidationException.class,
            () ->
                table.executeSql(
                    "CREATE TABLE invalid_time(k BIGINT,rt "
                        + type
                        + ",WATERMARK FOR rt AS rt) WITH ('connector'='datagen')"));
    assertTrue(failure.getMessage().contains("from 0 to 3"), failure.getMessage());
  }
}
