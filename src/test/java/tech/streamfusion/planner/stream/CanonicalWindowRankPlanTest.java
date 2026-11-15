package tech.streamfusion.planner.stream;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.Base64;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.streaming.api.graph.StreamGraphHasherV2;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.table.api.PlanReference;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.util.InstantiationUtil;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import tech.streamfusion.NativePlannerTestEnvironment;
import tech.streamfusion.operator.NativeColumnarWindowRankOperator;
import tech.streamfusion.state.StockWindowRankStateContract;

class CanonicalWindowRankPlanTest {
  static Stream<Arguments> cases() {
    var result = new ArrayList<Arguments>();
    for (String shape : new String[] {"TUMBLE", "HOP", "CUMULATE"})
      for (String zone : new String[] {"UTC", "GMT+05:45", "Europe/Berlin", "America/New_York"})
        for (boolean keyed : new boolean[] {false, true})
          for (String mode : new String[] {"rank-first", "rank-last", "dedup-first", "dedup-last"})
            for (boolean compiled : new boolean[] {false, true})
              for (String uids : new String[] {"DISABLED", "ALWAYS"})
                result.add(Arguments.of(shape, zone, keyed, mode, compiled, uids));
    return result.stream();
  }

  @ParameterizedTest
  @MethodSource("cases")
  void originalStockWindowOwnerBindsAutomatically(
      String shape, String zone, boolean keyed, String mode, boolean compiled, String uids)
      throws Exception {
    boolean dedup = mode.startsWith("dedup");
    boolean last = mode.endsWith("last");
    int maximum = uids.equals("DISABLED") ? 257 : 128;
    var config = new Configuration();
    config.setString("streamfusion.native.enabled", "true");
    config.setString("table.exec.uid.generation", uids);
    config.setString("table.local-time-zone", zone);
    config.setString("parallelism.default", "3");
    config.setString("pipeline.max-parallelism", Integer.toString(maximum));
    var table = NativePlannerTestEnvironment.create(config);
    table.getConfig().set("streamfusion.native.enabled", "false");
    table.executeSql(
        "CREATE TABLE wr_input(k BIGINT,v BIGINT,rt TIMESTAMP_LTZ(3),WATERMARK FOR rt AS rt) WITH"
            + " ('connector'='datagen')");
    table.executeSql(
        "CREATE TABLE wr_output(k BIGINT,v BIGINT,rt TIMESTAMP_LTZ(3),window_start"
            + " TIMESTAMP(3),window_end TIMESTAMP(3)"
            + (dedup ? "" : ",rn BIGINT")
            + ") WITH ('connector'='blackhole')");
    String arguments =
        shape.equals("TUMBLE") ? "INTERVAL '6' SECOND" : "INTERVAL '2' SECOND, INTERVAL '6' SECOND";
    String window =
        shape + "(TABLE wr_input, DESCRIPTOR(rt), " + arguments + ", INTERVAL '1' SECOND)";
    String fields = "k,v,rt,window_start,window_end";
    String query =
        "INSERT INTO wr_output SELECT "
            + fields
            + (dedup ? "" : ",rn")
            + " FROM (SELECT "
            + fields
            + ",ROW_NUMBER() OVER (PARTITION BY window_start,window_end"
            + (keyed ? ",k" : "")
            + " ORDER BY "
            + (dedup ? "rt" : "v")
            + (last ? " DESC" : " ASC")
            + ") AS rn FROM TABLE("
            + window
            + ")) WHERE rn <= "
            + (dedup ? "1" : "3");
    var plan = table.compilePlanSql(query);
    String portable = plan.asJsonString();
    assertTrue(
        portable.contains(dedup ? "stream-exec-window-deduplicate" : "stream-exec-window-rank"));
    assertFalse(portable.contains("tech.streamfusion"));
    var planner = (PlannerBase) ((TableEnvironmentImpl) table).getPlanner();
    var baseline =
        CanonicalRegularJoinPlanTest.graph(
            planner,
            table,
            planner.translatePlan(planner.loadPlan(PlanReference.fromJsonString(portable))));
    var original =
        baseline.getStreamNodes().stream()
            .filter(
                node ->
                    node.getOperatorFactory() instanceof SimpleOperatorFactory
                        && tech.streamfusion.compat.FlinkPlanCompat.windowOperator()
                            .isInstance(
                                ((SimpleOperatorFactory<?>) node.getOperatorFactory())
                                    .getOperator()))
            .findFirst()
            .orElseThrow();
    var stockOperator = ((SimpleOperatorFactory<?>) original.getOperatorFactory()).getOperator();
    var processor = GroupAggregateStockTestFixture.field(stockOperator, "windowProcessor");
    table.getConfig().set("streamfusion.native.enabled", "true");
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
                            instanceof NativeColumnarWindowRankOperator)
            .collect(java.util.stream.Collectors.toList());
    assertEquals(1, owners.size(), graph.getStreamingPlanAsJSON());
    var owner = owners.get(0);
    var operator = ((SimpleOperatorFactory<?>) owner.getOperatorFactory()).getOperator();
    var contract = GroupAggregateStockTestFixture.field(operator, "stockStateContract");
    assertInstanceOf(StockWindowRankStateContract.class, contract);
    assertEquals(dedup, GroupAggregateStockTestFixture.field(contract, "deduplicate"));
    assertEquals(
        GroupAggregateStockTestFixture.field(processor, "shiftTimeZone"),
        GroupAggregateStockTestFixture.field(contract, "zone"));
    assertEquals(
        tech.streamfusion.compat.FlinkPlanCompat.maximumParallelism(original),
        tech.streamfusion.compat.FlinkPlanCompat.maximumParallelism(owner));
    assertEquals(
        keyed ? maximum : 1, tech.streamfusion.compat.FlinkPlanCompat.maximumParallelism(owner));
    assertEquals(keyed ? 3 : 1, owner.getParallelism());
    assertEquals(uids.equals("DISABLED"), owner.getTransformationUID() == null);
    if (compiled) assertEquals(original.getTransformationUID(), owner.getTransformationUID());
    assertEquals(last && dedup, GroupAggregateStockTestFixture.field(operator, "keepLastOnTie"));
    assertTrue(graph.getStreamingPlanAsJSON().contains("RowDataToArrow"));
    assertTrue(graph.getStreamingPlanAsJSON().contains("ArrowToRowData"));
    Map<?, ?> registrations =
        InstantiationUtil.deserializeObject(
            Base64.getDecoder()
                .decode(
                    table
                        .getConfig()
                        .getConfiguration()
                        .getString("streamfusion.canonical.state-contracts", "")),
            getClass().getClassLoader());
    var hashes = new StreamGraphHasherV2().traverseStreamGraphAndGenerateHashes(graph);
    assertEquals(1, registrations.size());
    assertTrue(registrations.containsKey(new OperatorID(hashes.get(owner.getId())).toString()));
    assertTrue(
        Boolean.parseBoolean(
            table
                .getConfig()
                .getConfiguration()
                .getString("streamfusion.canonical.metadata-projection-active", "false")));
    assertEquals(portable, plan.asJsonString());
    table.getConfig().set("streamfusion.native.enabled", "false");
    assertFalse(CanonicalRegularJoinPlanTest.translate(planner, query).isEmpty());
    assertEquals(
        "",
        table
            .getConfig()
            .getConfiguration()
            .getString("streamfusion.canonical.state-contracts", ""));
  }
}
