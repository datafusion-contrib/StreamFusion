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
import tech.streamfusion.Native;
import tech.streamfusion.NativePlannerTestEnvironment;
import tech.streamfusion.operator.NativeColumnarGroupAggregateOperator;
import tech.streamfusion.state.StockGroupArithmeticStateContract;

class CanonicalGroupArithmeticPlanTest {
  static Stream<Arguments> cases() {
    var bigKeys =
        Stream.of("arithmetic", "hidden", "count", "star")
            .flatMap(
                mode ->
                    Stream.of(false, true)
                        .flatMap(
                            compiled ->
                                Stream.of(0L, 1000L)
                                    .flatMap(
                                        ttl ->
                                            Stream.of("DISABLED", "ALWAYS")
                                                .flatMap(
                                                    uids ->
                                                        Stream.of(false, true)
                                                            .map(
                                                                append ->
                                                                    Arguments.of(
                                                                        mode, compiled, ttl, uids,
                                                                        append, false))))));
    var intKeys =
        Stream.of("arithmetic", "hidden", "count", "star")
            .flatMap(
                mode ->
                    Stream.of(false, true)
                        .flatMap(
                            compiled ->
                                Stream.of(0L, 1000L)
                                    .flatMap(
                                        ttl ->
                                            Stream.of(false, true)
                                                .map(
                                                    append ->
                                                        Arguments.of(
                                                            mode,
                                                            compiled,
                                                            ttl,
                                                            "DISABLED",
                                                            append,
                                                            true)))));
    return Stream.concat(bigKeys, intKeys);
  }

  @ParameterizedTest
  @MethodSource("cases")
  void normalSubmissionCapturesPhysicalStateAndOriginalIdentity(
      String mode, boolean compiled, long ttl, String uids, boolean append, boolean integerKey)
      throws Exception {
    assertPlan(mode, compiled, ttl, uids, append, integerKey, false);
  }

  void assertPlan(
      String mode,
      boolean compiled,
      long ttl,
      String uids,
      boolean append,
      boolean integerKey,
      boolean global)
      throws Exception {
    String keyType = integerKey ? "INT" : "BIGINT";
    var config = new Configuration();
    config.setString("streamfusion.native.enabled", "true");
    config.setString("parallelism.default", "2");
    config.setString("pipeline.max-parallelism", "257");
    config.setString("table.exec.uid.generation", uids);
    config.setString("table.exec.state.ttl", ttl + " ms");
    var table = NativePlannerTestEnvironment.create(config);
    table.getConfig().set("table.exec.mini-batch.enabled", "false");
    table.getConfig().set("table.optimizer.agg-phase-strategy", "ONE_PHASE");
    table.registerCatalog(
        "fixture",
        new org.apache.flink.table.catalog.GenericInMemoryCatalog("fixture", "db") {
          @Override
          public java.util.Optional<org.apache.flink.table.factories.Factory> getFactory() {
            return java.util.Optional.of(new org.example.canonical.CanonicalFixtureFactory());
          }
        });
    table.useCatalog("fixture");
    table.executeSql(
        "CREATE TABLE input_table(k "
            + keyType
            + ",v BIGINT) WITH"
            + " ('connector'='canonical-fixture','input'='unused.csv','ready'='unused.ready','insert-only'='"
            + append
            + "')");
    table.useCatalog("default_catalog");
    String fields =
        mode.equals("extrema")
            ? "MIN(v) AS a,MAX(v) AS b"
            : mode.equals("mixed-extrema")
                ? "COUNT(*) AS a,COUNT(v) AS b,SUM(v) AS c,AVG(v) AS d,MIN(v) AS e,MAX(v) AS f"
                : mode.equals("arithmetic")
                    ? "COUNT(*) AS a,COUNT(v) AS b,SUM(v) AS c,AVG(v) AS d"
                    : mode.equals("hidden")
                        ? "SUM(v) AS c,AVG(v) AS d"
                        : mode.equals("count") ? "COUNT(v) AS a" : "COUNT(*) AS a";
    table.executeSql(
        "CREATE TABLE output_table("
            + (global ? "" : "k " + keyType + ",")
            + (mode.equals("extrema")
                ? "a BIGINT,b BIGINT"
                : mode.equals("mixed-extrema")
                    ? "a BIGINT,b BIGINT,c BIGINT,d BIGINT,e BIGINT,f BIGINT"
                    : mode.equals("arithmetic")
                        ? "a BIGINT,b BIGINT,c BIGINT,d BIGINT"
                        : mode.equals("hidden") ? "c BIGINT,d BIGINT" : "a BIGINT")
            + ") WITH ('connector'='blackhole')");
    String query =
        "INSERT INTO output_table SELECT "
            + (global ? "" : "k,")
            + fields
            + " FROM fixture.db.input_table"
            + (global ? "" : " GROUP BY k");
    var planner = (PlannerBase) ((TableEnvironmentImpl) table).getPlanner();
    var plan = compiled ? table.compilePlanSql(query) : null;
    String portable = plan == null ? "" : plan.asJsonString();
    assertFalse(portable.contains("tech.streamfusion"));
    var transformations =
        compiled
            ? planner.translatePlan(planner.loadPlan(PlanReference.fromJsonString(portable)))
            : CanonicalRegularJoinPlanTest.translate(planner, query);
    var graph = CanonicalRegularJoinPlanTest.graph(planner, table, transformations);
    var owners =
        graph.getStreamNodes().stream()
            .filter(
                node ->
                    node.getOperatorFactory() instanceof SimpleOperatorFactory
                        && ((SimpleOperatorFactory<?>) node.getOperatorFactory()).getOperator()
                            instanceof NativeColumnarGroupAggregateOperator)
            .collect(java.util.stream.Collectors.toList());
    assertEquals(1, owners.size(), graph.getStreamingPlanAsJSON());
    var owner = owners.get(0);
    assertEquals(global ? 1 : 257, owner.getMaxParallelism());
    assertEquals(global ? 1 : 2, owner.getParallelism());
    assertEquals(uids.equals("DISABLED"), owner.getTransformationUID() == null);
    var operator = ((SimpleOperatorFactory<?>) owner.getOperatorFactory()).getOperator();
    var contract =
        assertInstanceOf(
            StockGroupArithmeticStateContract.class,
            GroupAggregateStockTestFixture.field(operator, "stockStateContract"));
    assertEquals(
        global ? 0 : 1,
        ((org.apache.flink.table.types.logical.RowType)
                GroupAggregateStockTestFixture.field(contract, "keyType"))
            .getFieldCount());
    int[] kinds =
        mode.equals("extrema")
            ? new int[] {10, 11}
            : mode.equals("mixed-extrema")
                ? new int[] {3, 3, 22, 10, 11}
                : mode.equals("arithmetic")
                    ? new int[] {3, 3, 22}
                    : mode.equals("hidden") ? new int[] {0, 4} : new int[] {3};
    assertArrayEquals(kinds, (int[]) GroupAggregateStockTestFixture.field(contract, "kinds"));
    assertArrayEquals(
        mode.equals("extrema")
            ? new int[] {0, 1}
            : mode.equals("mixed-extrema")
                ? new int[] {0, 1, 2, 3, 4}
                : mode.equals("arithmetic")
                    ? new int[] {0, 1, 2}
                    : mode.equals("hidden")
                        ? (append ? new int[] {0, 1} : new int[] {0, 2})
                        : new int[] {0},
        (int[]) GroupAggregateStockTestFixture.field(contract, "scalarFields"));
    assertArrayEquals(
        mode.equals("extrema")
            ? new int[] {-1, -1}
            : mode.equals("mixed-extrema")
                ? new int[] {-1, -1, -1, -1, -1}
                : mode.equals("hidden")
                    ? (append ? new int[] {-1, 2} : new int[] {1, 3})
                    : mode.equals("arithmetic") ? new int[] {-1, -1, -1} : new int[] {-1},
        (int[]) GroupAggregateStockTestFixture.field(contract, "countFields"));
    assertEquals(
        append ? -1 : mode.equals("hidden") ? 4 : mode.equals("count") ? 1 : 0,
        GroupAggregateStockTestFixture.field(contract, "recordCountField"));
    assertEquals(ttl, GroupAggregateStockTestFixture.field(operator, "stateTtlMillis"));
    assertFalse((boolean) GroupAggregateStockTestFixture.field(operator, "miniBatch"));
    assertArrayEquals(
        global
            ? new int[] {Native.STOCK_KEY_PROJECTION}
            : new int[] {Native.STOCK_KEY_PROJECTION, -1},
        (int[]) GroupAggregateStockTestFixture.field(operator, "keyTimestampPrecisions"));
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
