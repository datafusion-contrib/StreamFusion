package tech.streamfusion.planner;

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
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.InstantiationUtil;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import tech.streamfusion.NativePlannerTestEnvironment;
import tech.streamfusion.operator.NativeRowWindowOperatorCore;
import tech.streamfusion.state.StockSessionWindowStateContract;
import tech.streamfusion.state.StockSlicingWindowStateContract;

class CanonicalWindowAggregateBindingPlanTest {
  static Stream<Arguments> cases() {
    var cases = new ArrayList<Arguments>();
    for (String shape : new String[] {"TUMBLE", "HOP", "CUMULATE", "SESSION"})
      for (String phase : new String[] {"ONE_PHASE", "TWO_PHASE"})
        for (boolean compiled : new boolean[] {false, true})
          cases.add(Arguments.of(shape, phase, compiled));
    return cases.stream()
        .filter(
            test ->
                !test.get()[0].equals("SESSION")
                    || tech.streamfusion.compat.CanonicalTestCompat.flink22());
  }

  @ParameterizedTest
  @MethodSource("cases")
  void normalSubmissionBindsActualStockWindowState(String shape, String phase, boolean compiled)
      throws Exception {
    verify(shape, phase, compiled, false, false, "TIMESTAMP_LTZ(3)", "UTC", "DISABLED");
  }

  static Stream<Arguments> broadCases() {
    var cases = new ArrayList<Arguments>();
    for (String shape : new String[] {"TUMBLE", "HOP", "CUMULATE", "SESSION"})
      for (String phase : new String[] {"ONE_PHASE", "TWO_PHASE"})
        for (boolean compiled : new boolean[] {false, true})
          for (boolean keyless : new boolean[] {false, true})
            for (boolean distinct : new boolean[] {false, true})
              for (String clock : new String[] {"TIMESTAMP_LTZ(3)", "PROCTIME"})
                for (String zone : new String[] {"UTC", "Europe/Berlin"})
                  for (String uids : new String[] {"DISABLED", "ALWAYS"})
                    cases.add(
                        Arguments.of(shape, phase, compiled, keyless, distinct, clock, zone, uids));
    return cases.stream()
        .filter(
            test ->
                !test.get()[0].equals("SESSION")
                    || tech.streamfusion.compat.CanonicalTestCompat.flink22());
  }

  @ParameterizedTest
  @MethodSource("broadCases")
  void keyedGlobalDistinctClockAndUidVariantsBindStockState(
      String shape,
      String phase,
      boolean compiled,
      boolean keyless,
      boolean distinct,
      String clock,
      String zone,
      String uids)
      throws Exception {
    verify(shape, phase, compiled, keyless, distinct, clock, zone, uids);
  }

  static Stream<Arguments> plainCases() {
    return Stream.of("TUMBLE", "HOP", "CUMULATE", "SESSION")
        .filter(
            shape ->
                !shape.equals("SESSION") || tech.streamfusion.compat.CanonicalTestCompat.flink22())
        .flatMap(
            shape ->
                Stream.of(false, true)
                    .flatMap(
                        compiled ->
                            Stream.of("DISABLED", "ALWAYS")
                                .map(uids -> Arguments.of(shape, compiled, uids))));
  }

  @ParameterizedTest
  @MethodSource("plainCases")
  void plainTimestampsKeepUtcStateDespiteSessionZone(String shape, boolean compiled, String uids)
      throws Exception {
    verify(shape, "ONE_PHASE", compiled, false, false, "TIMESTAMP(3)", "America/New_York", uids);
  }

  private void verify(
      String shape,
      String phase,
      boolean compiled,
      boolean keyless,
      boolean distinct,
      String clock,
      String zone,
      String uids)
      throws Exception {
    var config = new Configuration();
    config.setString("streamfusion.native.enabled", "true");
    config.setString("table.exec.uid.generation", uids);
    config.setString("table.local-time-zone", zone);
    config.setString("table.optimizer.agg-phase-strategy", phase);
    config.setString("pipeline.max-parallelism", "257");
    config.setString("parallelism.default", "3");
    var table = NativePlannerTestEnvironment.create(config);
    table.getConfig().set("streamfusion.native.enabled", "false");
    table.executeSql(
        "CREATE TABLE wa_input(k BIGINT,v BIGINT,"
            + (clock.equals("PROCTIME")
                ? "rt AS PROCTIME()"
                : "rt " + clock + ",WATERMARK FOR rt AS rt")
            + ") WITH ('connector'='datagen')");
    table.executeSql(
        "CREATE TABLE wa_output("
            + (keyless ? "" : "k BIGINT,")
            + "n BIGINT,total BIGINT,mean BIGINT,ws TIMESTAMP(3),we TIMESTAMP(3)) WITH"
            + " ('connector'='blackhole')");
    String args =
        shape.equals("TUMBLE") || shape.equals("SESSION")
            ? "INTERVAL '6' SECOND"
            : "INTERVAL '2' SECOND,INTERVAL '6' SECOND";
    String query =
        "INSERT INTO wa_output SELECT "
            + (keyless ? "" : "k,")
            + "COUNT("
            + (distinct ? "DISTINCT v" : "v")
            + "),SUM(v),AVG(v),window_start,window_end FROM TABLE("
            + shape
            + "(TABLE wa_input"
            + (shape.equals("SESSION") && !keyless ? " PARTITION BY k" : "")
            + ",DESCRIPTOR(rt),"
            + args
            + ")) GROUP BY "
            + (keyless ? "" : "k,")
            + "window_start,window_end";
    var plan = table.compilePlanSql(query);
    String portable = plan.asJsonString();
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
    var processor =
        GroupAggregateStockTestFixture.field(
            ((SimpleOperatorFactory<?>) original.getOperatorFactory()).getOperator(),
            "windowProcessor");
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
                            instanceof NativeRowWindowOperatorCore)
            .collect(java.util.stream.Collectors.toList());
    assertEquals(1, owners.size(), graph.getStreamingPlanAsJSON());
    var owner = owners.get(0);
    var operator = ((SimpleOperatorFactory<?>) owner.getOperatorFactory()).getOperator();
    var contract = GroupAggregateStockTestFixture.field(operator, "stockStateContract");
    if (shape.equals("SESSION")) assertInstanceOf(StockSessionWindowStateContract.class, contract);
    else assertInstanceOf(StockSlicingWindowStateContract.class, contract);
    assertEquals(
        GroupAggregateStockTestFixture.field(processor, "accSerializer"),
        new RowDataSerializer(
            (RowType) GroupAggregateStockTestFixture.field(contract, "accumulators")));
    assertEquals(
        keyless ? 0 : 1,
        ((RowType) GroupAggregateStockTestFixture.field(contract, "keyType")).getFieldCount());
    assertEquals(
        !clock.equals("PROCTIME"), GroupAggregateStockTestFixture.field(processor, "isEventTime"));
    assertEquals(
        tech.streamfusion.compat.FlinkPlanCompat.maximumParallelism(original),
        tech.streamfusion.compat.FlinkPlanCompat.maximumParallelism(owner));
    assertEquals(original.getParallelism(), owner.getParallelism());
    assertEquals(uids.equals("DISABLED"), owner.getTransformationUID() == null);
    if (compiled) assertEquals(original.getTransformationUID(), owner.getTransformationUID());
    if (portable.contains("stream-exec-global-window-aggregate"))
      assertTrue(
          graph.getStreamNodes().stream()
              .anyMatch(
                  node ->
                      node.getOperatorFactory() instanceof SimpleOperatorFactory
                          && ((SimpleOperatorFactory<?>) node.getOperatorFactory()).getOperator()
                              instanceof
                              tech.streamfusion.operator
                                  .NativeColumnarLocalWindowAggregateOperator),
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
    var hashes = new StreamGraphHasherV2().traverseStreamGraphAndGenerateHashes(graph);
    assertTrue(registrations.containsKey(new OperatorID(hashes.get(owner.getId())).toString()));
    assertEquals(portable, plan.asJsonString());
    assertTrue(graph.getStreamingPlanAsJSON().contains("RowDataToArrow"));
    assertTrue(graph.getStreamingPlanAsJSON().contains("ArrowToRowData"));
  }
}
