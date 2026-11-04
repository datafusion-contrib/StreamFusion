package tech.streamfusion.planner;

import static org.junit.jupiter.api.Assertions.*;

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
import tech.streamfusion.operator.NativeOverAggregateOperator;
import tech.streamfusion.state.StockOverEventRowsStateContract;
import tech.streamfusion.state.StockOverProcessingRowsStateContract;
import tech.streamfusion.state.StockOverRangeStateContract;

class CanonicalBoundedOverPlanTest {
  static Stream<Arguments> cases() {
    return Stream.of("processing-rows", "event-rows", "event-range")
        .flatMap(
            mode ->
                Stream.of("BIGINT", "FLOAT", "DOUBLE", "DECIMAL(38,2)")
                    .flatMap(
                        type ->
                            Stream.of(false, true)
                                .flatMap(
                                    keyless ->
                                        Stream.of(false, true)
                                            .flatMap(
                                                compiled ->
                                                    Stream.of("DISABLED", "ALWAYS")
                                                        .flatMap(
                                                            uids ->
                                                                (mode.equals("event-rows")
                                                                        ? Stream.of(0L)
                                                                        : Stream.of(0L, 1000L))
                                                                    .map(
                                                                        ttl ->
                                                                            Arguments.of(
                                                                                mode, type, keyless,
                                                                                compiled, uids,
                                                                                ttl)))))));
  }

  @ParameterizedTest
  @MethodSource("cases")
  void submissionsCaptureActualBoundedStateAndStockIdentity(
      String mode, String type, boolean keyless, boolean compiled, String uids, long ttl)
      throws Exception {
    assertPlan(mode, type, keyless, compiled, uids, ttl, false);
  }

  static Stream<Arguments> constantCases() {
    return Stream.of("processing-rows", "event-rows", "event-range")
        .flatMap(
            mode ->
                Stream.of(false, true)
                    .flatMap(
                        keyless ->
                            Stream.of(false, true)
                                .flatMap(
                                    compiled ->
                                        Stream.of("DISABLED", "ALWAYS")
                                            .map(
                                                uids ->
                                                    Arguments.of(mode, keyless, compiled, uids)))));
  }

  @ParameterizedTest
  @MethodSource("constantCases")
  void zeroPrecedingBoundsKeepExplicitCountStarAndLiteralArguments(
      String mode, boolean keyless, boolean compiled, String uids) throws Exception {
    assertPlan(mode, "BIGINT", keyless, compiled, uids, 0, true);
  }

  void assertPlan(
      String mode,
      String type,
      boolean keyless,
      boolean compiled,
      String uids,
      long ttl,
      boolean starAndLiteral)
      throws Exception {
    var config = new Configuration();
    config.setString("streamfusion.native.enabled", "true");
    config.setString("parallelism.default", "2");
    config.setString("pipeline.max-parallelism", "257");
    config.setString("table.exec.uid.generation", uids);
    config.setString("table.exec.state.ttl", ttl + " ms");
    var table = NativePlannerTestEnvironment.create(config);
    table.executeSql(
        "CREATE TABLE over_input(k BIGINT,v "
            + type
            + ",rt TIMESTAMP(3),pt AS PROCTIME(),WATERMARK FOR rt AS rt) WITH"
            + " ('connector'='datagen')");
    table.executeSql(
        "CREATE TABLE over_output(k BIGINT,v "
            + type
            + ",rt TIMESTAMP(3),s "
            + type
            + ",c BIGINT,a "
            + type
            + ") WITH ('connector'='blackhole')");
    boolean unbounded = mode.startsWith("processing-unbounded");
    boolean processing = mode.startsWith("processing"), rows = !mode.endsWith("range");
    String query =
        "INSERT INTO over_output SELECT k,v,rt,"
            + (starAndLiteral
                ? "COUNT(*) OVER w,SUM(CAST(7 AS BIGINT)) OVER w,AVG(v) OVER w"
                : "SUM(v) OVER w,COUNT(v) OVER w,AVG(v) OVER w")
            + " FROM over_input WINDOW w AS ("
            + (keyless ? "" : "PARTITION BY k ")
            + "ORDER BY "
            + (processing ? "pt" : "rt")
            + (unbounded
                ? (rows ? " ROWS" : " RANGE") + " BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)"
                : starAndLiteral
                    ? (rows ? " ROWS BETWEEN 0" : " RANGE BETWEEN INTERVAL '0' SECOND")
                        + " PRECEDING AND CURRENT ROW)"
                    : rows
                        ? " ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)"
                        : " RANGE BETWEEN INTERVAL '0.032' SECOND PRECEDING AND CURRENT ROW)");
    var planner = (PlannerBase) ((TableEnvironmentImpl) table).getPlanner();
    var plan = compiled ? table.compilePlanSql(query) : null;
    String portable = plan == null ? "" : plan.asJsonString();
    assertFalse(portable.contains("tech.streamfusion"));
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
                            instanceof NativeOverAggregateOperator)
            .collect(java.util.stream.Collectors.toList());
    assertEquals(1, owners.size(), graph.getStreamingPlanAsJSON());
    var owner = owners.get(0);
    var operator = ((SimpleOperatorFactory<?>) owner.getOperatorFactory()).getOperator();
    var contract = GroupAggregateStockTestFixture.field(operator, "stockStateContract");
    assertNotNull(contract);
    if (starAndLiteral && !unbounded)
      assertEquals(-1, GroupAggregateStockTestFixture.field(contract, "hiddenCount"));
    assertEquals(
        unbounded
            ? tech.streamfusion.state.StockOverFoldStateContract.class
            : processing
                ? StockOverProcessingRowsStateContract.class
                : rows ? StockOverEventRowsStateContract.class : StockOverRangeStateContract.class,
        contract.getClass());
    assertEquals(keyless ? 1 : 257, owner.getMaxParallelism());
    assertEquals(keyless ? 1 : 2, owner.getParallelism());
    assertEquals(uids.equals("DISABLED"), owner.getTransformationUID() == null);
    assertEquals(ttl, GroupAggregateStockTestFixture.field(operator, "stateTtlMillis"));
    assertEquals(
        keyless ? 0 : 1,
        ((org.apache.flink.table.types.logical.RowType)
                GroupAggregateStockTestFixture.field(contract, "keyType"))
            .getFieldCount());
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
    if (compiled) assertEquals(portable, plan.asJsonString());
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
