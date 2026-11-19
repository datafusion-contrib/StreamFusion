package tech.streamfusion.planner.stream;

import static org.junit.jupiter.api.Assertions.*;

import java.util.*;
import java.util.stream.Stream;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.PlanReference;
import org.apache.flink.table.api.TableEnvironment;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import tech.streamfusion.NativePlannerTestEnvironment;

class CanonicalWindowJoinPlanTest {
  static Stream<Arguments> cases() {
    var cases = new ArrayList<Arguments>();
    for (String kind : List.of("INNER", "LEFT", "RIGHT", "FULL"))
      for (String shape : List.of("TUMBLE", "HOP", "CUMULATE"))
        for (boolean keyless : new boolean[] {false, true})
          for (String clock : List.of("TIMESTAMP(3)", "TIMESTAMP_LTZ(3)"))
            cases.add(Arguments.of(kind, shape, keyless, clock));
    return cases.stream();
  }

  @ParameterizedTest
  @MethodSource("cases")
  void portableStockWindowJoinBindsItsCanonicalContract(
      String kind, String shape, boolean keyless, String clock) throws Exception {
    var table = environment(true, clock);
    String window =
        shape.equals("TUMBLE") ? "INTERVAL '3' SECOND" : "INTERVAL '1' SECOND, INTERVAL '3' SECOND";
    String query =
        "INSERT INTO output_table SELECT l.k,l.v,r.v FROM (SELECT * FROM TABLE("
            + shape
            + "(TABLE left_input, DESCRIPTOR(rt), "
            + window
            + "))) l "
            + kind
            + " JOIN (SELECT * FROM TABLE("
            + shape
            + "(TABLE right_input, DESCRIPTOR(rt), "
            + window
            + "))) r"
            + " ON l.window_start=r.window_start AND l.window_end=r.window_end"
            + (keyless ? "" : " AND l.k=r.k");
    var plan = table.compilePlanSql(query);
    String portable = plan.asJsonString();
    assertTrue(portable.contains("stream-exec-window-join"), portable);
    assertFalse(portable.contains("tech.streamfusion"), portable);
    String explain = plan.explain();
    assertTrue(explain.contains("NativeWindowJoin"), explain);
    assertTrue(explain.contains("RowDataToArrow"), explain);
    assertTrue(explain.contains("ArrowToRowData"), explain);
    assertEquals(portable, plan.asJsonString());
    var stock = environment(false, clock).loadPlan(PlanReference.fromJsonString(portable));
    assertFalse(stock.explain().contains("NativeWindowJoin"), stock.explain());
  }

  @ParameterizedTest
  @org.junit.jupiter.params.provider.ValueSource(strings = {"TIMESTAMP(3)", "TIMESTAMP_LTZ(3)"})
  void unqualifiedUnalignedExchangeRetainsItsStockWindowStateOwner(String clock) throws Exception {
    var table = environment(true, clock);
    table.getConfig().set("execution.checkpointing.interval", "1 s");
    table.getConfig().set("execution.checkpointing.unaligned.enabled", "true");
    var plan =
        table.compilePlanSql(
            "INSERT INTO output_table SELECT l.k,l.v,r.v FROM (SELECT * FROM TABLE(TUMBLE(TABLE"
                + " left_input,DESCRIPTOR(rt),INTERVAL '3' SECOND))) l JOIN (SELECT * FROM"
                + " TABLE(TUMBLE(TABLE right_input,DESCRIPTOR(rt),INTERVAL '3' SECOND))) r ON"
                + " l.k=r.k AND l.window_start=r.window_start AND l.window_end=r.window_end");
    assertTrue(plan.asJsonString().contains("stream-exec-window-join"));
    assertFalse(plan.explain().contains("NativeWindowJoin"), plan.explain());
  }

  private static TableEnvironment environment(boolean nativeEnabled, String clock)
      throws Exception {
    var config = new Configuration();
    config.setString("streamfusion.native.enabled", Boolean.toString(nativeEnabled));
    config.setString("parallelism.default", "2");
    config.setString("table.local-time-zone", "America/New_York");
    var table = NativePlannerTestEnvironment.create(config);
    for (String name : List.of("left_input", "right_input"))
      table.executeSql(
          "CREATE TABLE "
              + name
              + "(k BIGINT,v BIGINT,rt "
              + clock
              + ",WATERMARK FOR rt AS rt) WITH ('connector'='datagen')");
    table.executeSql(
        "CREATE TABLE output_table(k BIGINT,l BIGINT,r BIGINT) WITH ('connector'='blackhole')");
    return table;
  }
}
