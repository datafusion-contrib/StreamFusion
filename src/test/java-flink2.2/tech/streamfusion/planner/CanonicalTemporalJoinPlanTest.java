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

class CanonicalTemporalJoinPlanTest {
  static Stream<Arguments> cases() {
    var result = new ArrayList<Arguments>();
    for (String kind : List.of("INNER", "LEFT"))
      for (String clock : List.of("TIMESTAMP(3)", "TIMESTAMP_LTZ(3)"))
        for (boolean residual : new boolean[] {false, true})
          for (boolean ttl : new boolean[] {false, true})
            result.add(Arguments.of(kind, clock, residual, ttl));
    return result.stream();
  }

  @ParameterizedTest
  @MethodSource("cases")
  void portableTemporalPlansBindOriginalStockState(
      String kind, String clock, boolean residual, boolean ttl) throws Exception {
    var table = environment(true, clock);
    if (ttl) table.getConfig().set("table.exec.state.ttl", "1 h");
    var plan = table.compilePlanSql(query(kind, residual));
    String portable = plan.asJsonString();
    assertTrue(portable.contains("stream-exec-temporal-join"), portable);
    assertFalse(portable.contains("tech.streamfusion"), portable);
    String explain = plan.explain();
    assertTrue(explain.contains("NativeTemporalJoin"), explain);
    assertTrue(explain.contains("RowDataToArrow"), explain);
    assertTrue(explain.contains("ArrowToRowData"), explain);
    assertEquals(portable, plan.asJsonString());
    var stock = environment(false, clock).loadPlan(PlanReference.fromJsonString(portable));
    assertFalse(stock.explain().contains("NativeTemporalJoin"), stock.explain());
  }

  @ParameterizedTest
  @ValueSource(strings = {"unaligned", "changelog", "disabled", "async"})
  void unqualifiedModesRetainTheOriginalStockOwner(String mode) throws Exception {
    var table = environment(true, "TIMESTAMP(3)");
    if (mode.equals("unaligned")) {
      table.getConfig().set("execution.checkpointing.interval", "1 s");
      table.getConfig().set("execution.checkpointing.unaligned.enabled", "true");
    } else if (mode.equals("changelog")) table.getConfig().set("state.changelog.enabled", "true");
    else if (mode.equals("disabled"))
      table.getConfig().set("streamfusion.operator.temporalJoin.enabled", "false");
    else table.getConfig().set("table.exec.async-state.enabled", "true");
    var plan = table.compilePlanSql(query("INNER", false));
    assertFalse(plan.explain().contains("NativeTemporalJoin"), plan.explain());
    assertFalse(plan.asJsonString().contains("tech.streamfusion"));
  }

  @Test
  void repeatedExplainsCannotReuseAnEarlierBinding() throws Exception {
    var table = environment(true, "TIMESTAMP(3)");
    var plan = table.compilePlanSql(query("LEFT", true));
    String portable = plan.asJsonString();
    assertTrue(plan.explain().contains("NativeTemporalJoin"));
    assertTrue(plan.explain().contains("NativeTemporalJoin"));
    table.getConfig().set("streamfusion.native.enabled", "false");
    assertFalse(plan.explain().contains("NativeTemporalJoin"));
    assertEquals(
        "",
        table
            .getConfig()
            .getConfiguration()
            .getString("streamfusion.canonical.state-contracts", ""));
    table.getConfig().set("streamfusion.native.enabled", "true");
    assertTrue(plan.explain().contains("NativeTemporalJoin"));
    assertEquals(portable, plan.asJsonString());
  }

  @Test
  void legacyProcessingTimeFunctionKeepsItsReleasedStockStateOwner() throws Exception {
    var config = new Configuration();
    config.setString("streamfusion.native.enabled", "true");
    var table = NativePlannerTestEnvironment.create(config);
    table.executeSql(
        "CREATE TABLE left_pt(k BIGINT,v BIGINT,pt AS PROCTIME()) WITH ('connector'='datagen')");
    table.executeSql(
        "CREATE TABLE right_pt(k BIGINT NOT NULL,v BIGINT,pt AS PROCTIME(),PRIMARY KEY(k) NOT"
            + " ENFORCED) WITH ('connector'='datagen')");
    table.executeSql(
        "CREATE TABLE output_table(k BIGINT,l BIGINT,r BIGINT) WITH ('connector'='blackhole')");
    table.createTemporarySystemFunction(
        "Rates",
        table
            .from("right_pt")
            .createTemporalTableFunction(
                org.apache.flink.table.api.Expressions.$("pt"),
                org.apache.flink.table.api.Expressions.$("k")));
    var plan =
        table.compilePlanSql(
            "INSERT INTO output_table SELECT l.k,l.v,r.v FROM left_pt l, LATERAL TABLE(Rates(l.pt))"
                + " r WHERE l.k=r.k");
    assertTrue(plan.asJsonString().contains("stream-exec-temporal-join"));
    assertFalse(plan.explain().contains("NativeTemporalJoin"), plan.explain());
    assertFalse(plan.asJsonString().contains("tech.streamfusion"));
  }

  @Test
  void rootRetentionConfigurationIsPreservedInTheStockStateContract() throws Exception {
    var table = environment(true, "TIMESTAMP(3)", true);
    assertTrue(
        table.compilePlanSql(query("INNER", false)).explain().contains("NativeTemporalJoin"));
    String encoded =
        table
            .getConfig()
            .getConfiguration()
            .getString("streamfusion.canonical.state-contracts", "");
    assertFalse(encoded.isEmpty());
    Map<?, ?> contracts =
        org.apache.flink.util.InstantiationUtil.deserializeObject(
            Base64.getDecoder().decode(encoded), getClass().getClassLoader());
    int temporal = 0;
    for (var binding : contracts.values()) {
      var owner = binding.getClass().getDeclaredField("operatorClass");
      owner.setAccessible(true);
      if (!owner.get(binding).equals("NativeTemporalJoinOperator")) continue;
      var value = binding.getClass().getDeclaredField("contract");
      value.setAccessible(true);
      var contract = (tech.streamfusion.state.StockStateContract) value.get(binding);
      assertTrue(contract.stateDescriptors().containsKey("cleanup-timestamp"));
      temporal++;
    }
    assertEquals(1, temporal);
  }

  private static String query(String kind, boolean residual) {
    return "INSERT INTO output_table SELECT l.k,l.v,r.v FROM left_input l "
        + kind
        + " JOIN right_input FOR SYSTEM_TIME AS OF l.rt AS r ON l.k=r.k"
        + (residual ? " AND l.v>r.v" : "");
  }

  private static TableEnvironment environment(boolean enabled, String clock) throws Exception {
    return environment(enabled, clock, false);
  }

  private static TableEnvironment environment(boolean enabled, String clock, boolean rootTtl)
      throws Exception {
    var config = new Configuration();
    if (rootTtl) config.setString("table.exec.state.ttl", "1 h");
    config.setString("streamfusion.native.enabled", Boolean.toString(enabled));
    config.setString("parallelism.default", "2");
    var table = NativePlannerTestEnvironment.create(config);
    table.executeSql(
        "CREATE TABLE left_input(k BIGINT,v BIGINT,rt "
            + clock
            + ",WATERMARK FOR rt AS rt) WITH ('connector'='datagen')");
    table.executeSql(
        "CREATE TABLE right_input(k BIGINT NOT NULL,v BIGINT,rt "
            + clock
            + ",WATERMARK FOR rt AS rt,PRIMARY KEY(k) NOT ENFORCED) WITH ('connector'='datagen')");
    table.executeSql(
        "CREATE TABLE output_table(k BIGINT,l BIGINT,r BIGINT) WITH ('connector'='blackhole')");
    return table;
  }
}
