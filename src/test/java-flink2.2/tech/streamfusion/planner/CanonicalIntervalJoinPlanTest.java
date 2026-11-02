package tech.streamfusion.planner;

import static org.junit.jupiter.api.Assertions.*;

import java.util.*;
import java.util.stream.Stream;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.PlanReference;
import org.apache.flink.table.api.TableEnvironment;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.NativePlannerTestEnvironment;

class CanonicalIntervalJoinPlanTest {
  static Stream<Arguments> cases() {
    var cases = new ArrayList<Arguments>();
    for (String kind : List.of("INNER", "LEFT", "RIGHT", "FULL"))
      for (String clock : List.of("TIMESTAMP(3)", "TIMESTAMP_LTZ(3)", "PROCTIME"))
        for (boolean residual : new boolean[] {false, true})
          cases.add(Arguments.of(kind, clock, residual));
    return cases.stream();
  }

  @ParameterizedTest
  @MethodSource("cases")
  void portablePlanBindsItsOriginalStockState(String kind, String clock, boolean residual)
      throws Exception {
    var table = environment(true, clock);
    var plan =
        table.compilePlanSql(
            query(kind, clock.equals("PROCTIME") ? "pt" : "rt", false)
                + (residual ? " AND l.v>r.v" : ""));
    String portable = plan.asJsonString();
    assertTrue(portable.contains("stream-exec-interval-join"), portable);
    assertFalse(portable.contains("tech.streamfusion"), portable);
    String explain = plan.explain();
    assertTrue(explain.contains("NativeIntervalJoin"), explain);
    assertTrue(explain.contains("RowDataToArrow"), explain);
    assertTrue(explain.contains("ArrowToRowData"), explain);
    assertEquals(portable, plan.asJsonString());
    var stock = environment(false, clock).loadPlan(PlanReference.fromJsonString(portable));
    assertFalse(stock.explain().contains("NativeIntervalJoin"), stock.explain());
  }

  @ParameterizedTest
  @ValueSource(strings = {"unaligned", "cleanup", "negative", "changelog"})
  void unqualifiedStateModesRetainTheirStockOwner(String mode) throws Exception {
    var table = environment(true, "TIMESTAMP(3)");
    if (mode.equals("unaligned")) {
      table.getConfig().set("execution.checkpointing.interval", "1 s");
      table.getConfig().set("execution.checkpointing.unaligned.enabled", "true");
    } else if (mode.equals("cleanup"))
      table.getConfig().set("table.exec.interval-join.min-cleanup-interval", "1 s");
    else if (mode.equals("changelog")) table.getConfig().set("state.changelog.enabled", "true");
    var plan = table.compilePlanSql(query("INNER", "rt", mode.equals("negative")));
    assertFalse(plan.explain().contains("NativeIntervalJoin"), plan.explain());
    assertFalse(plan.asJsonString().contains("tech.streamfusion"));
  }

  @Test
  void repeatedExplainsAndDisabledJobsCannotReuseAnEarlierStateBinding() throws Exception {
    var table = environment(true, "TIMESTAMP(3)");
    var plan = table.compilePlanSql(query("FULL", "rt", false));
    String portable = plan.asJsonString();
    assertTrue(plan.explain().contains("NativeIntervalJoin"));
    assertTrue(plan.explain().contains("NativeIntervalJoin"));
    table.getConfig().set("streamfusion.native.enabled", "false");
    assertFalse(plan.explain().contains("NativeIntervalJoin"));
    assertEquals(
        "",
        table
            .getConfig()
            .getConfiguration()
            .getString("streamfusion.canonical.state-contracts", ""));
    table.getConfig().set("streamfusion.native.enabled", "true");
    assertTrue(plan.explain().contains("NativeIntervalJoin"));
    assertFalse(
        table.compilePlanSql(query("INNER", "rt", true)).explain().contains("NativeIntervalJoin"));
    assertEquals(
        "",
        table
            .getConfig()
            .getConfiguration()
            .getString("streamfusion.canonical.state-contracts", ""));
    assertEquals(portable, plan.asJsonString());
  }

  private static String query(String kind, String clock, boolean negative) {
    return "INSERT INTO output_table SELECT l.k,l.v,r.v FROM left_input l "
        + kind
        + " JOIN right_input r ON l.k=r.k AND l."
        + clock
        + " BETWEEN r."
        + clock
        + (negative ? " + " : " - ")
        + "INTERVAL '1' SECOND AND r."
        + clock
        + (negative ? " - " : " + ")
        + "INTERVAL '1' SECOND";
  }

  private static TableEnvironment environment(boolean nativeEnabled, String clock)
      throws Exception {
    var config = new Configuration();
    config.setString("streamfusion.native.enabled", Boolean.toString(nativeEnabled));
    config.setString("parallelism.default", "2");
    var table = NativePlannerTestEnvironment.create(config);
    for (String name : List.of("left_input", "right_input"))
      table.executeSql(
          "CREATE TABLE "
              + name
              + "(k BIGINT,v BIGINT,rt "
              + (clock.equals("PROCTIME") ? "TIMESTAMP(3)" : clock)
              + ",pt AS PROCTIME(),WATERMARK FOR rt AS rt) WITH ('connector'='datagen')");
    table.executeSql(
        "CREATE TABLE output_table(k BIGINT,l BIGINT,r BIGINT) WITH ('connector'='blackhole')");
    return table;
  }
}
