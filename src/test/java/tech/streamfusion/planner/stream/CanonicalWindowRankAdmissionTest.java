package tech.streamfusion.planner.stream;

import static org.junit.jupiter.api.Assertions.*;

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
import tech.streamfusion.operator.NativeColumnarWindowRankOperator;

class CanonicalWindowRankAdmissionTest {
  static Stream<Arguments> cases() {
    var cases = new ArrayList<Arguments>();
    for (String mode : new String[] {"async", "unaligned", "offset"})
      for (boolean dedup : new boolean[] {false, true})
        if (!mode.equals("offset") || !dedup)
          for (boolean compiled : new boolean[] {false, true})
            for (String uids : new String[] {"DISABLED", "ALWAYS"})
              cases.add(Arguments.of(mode, dedup, compiled, uids));
    return cases.stream();
  }

  @ParameterizedTest
  @MethodSource("cases")
  void unqualifiedWindowStateKeepsOriginalStockOwner(
      String mode, boolean dedup, boolean compiled, String uids) throws Exception {
    var config = new Configuration();
    config.setString("streamfusion.native.enabled", "true");
    config.setString("table.exec.uid.generation", uids);
    if (mode.equals("async")) config.setString("table.exec.async-state.enabled", "true");
    if (mode.equals("unaligned"))
      config.setString("execution.checkpointing.unaligned.enabled", "true");
    var table = NativePlannerTestEnvironment.create(config);
    table.executeSql(
        "CREATE TABLE wa_input(k BIGINT,v BIGINT,rt TIMESTAMP(3),WATERMARK FOR rt AS rt) WITH"
            + " ('connector'='datagen')");
    table.executeSql("CREATE TABLE wa_output(k BIGINT,v BIGINT) WITH ('connector'='blackhole')");
    String query =
        "INSERT INTO wa_output SELECT k,v FROM (SELECT k,v,ROW_NUMBER() OVER (PARTITION BY"
            + " window_start,window_end,k ORDER BY "
            + (dedup ? "rt" : "v")
            + ") AS rn FROM TABLE(TUMBLE(TABLE wa_input,DESCRIPTOR(rt),INTERVAL '3' SECOND))) WHERE"
            + " "
            + (mode.equals("offset") ? "rn BETWEEN 2 AND 4" : "rn <= " + (dedup ? "1" : "3"));
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
                            instanceof NativeColumnarWindowRankOperator),
        graph.getStreamingPlanAsJSON());
    assertTrue(
        graph.getStreamNodes().stream()
            .anyMatch(
                node ->
                    node.getOperatorFactory() instanceof SimpleOperatorFactory
                        && tech.streamfusion.compat.FlinkPlanCompat.windowOperator()
                            .isInstance(
                                ((SimpleOperatorFactory<?>) node.getOperatorFactory())
                                    .getOperator())),
        graph.getStreamingPlanAsJSON());
    assertEquals(portable, plan.asJsonString());
    assertFalse(portable.contains("tech.streamfusion"));
  }
}
