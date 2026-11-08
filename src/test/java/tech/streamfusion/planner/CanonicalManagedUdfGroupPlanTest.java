package tech.streamfusion.planner;

import static org.junit.jupiter.api.Assertions.*;
import static tech.streamfusion.planner.GroupAggregateStockTestFixture.field;

import java.util.*;
import java.util.stream.Stream;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.operators.*;
import org.apache.flink.table.api.PlanReference;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.operators.aggregate.GroupAggFunction;
import org.apache.flink.table.types.logical.LogicalType;
import org.example.canonical.CanonicalFixtureFactory;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.*;
import tech.streamfusion.NativePlannerTestEnvironment;
import tech.streamfusion.operator.NativeColumnarJvmGroupOperator;

class CanonicalManagedUdfGroupPlanTest {
  static Stream<Arguments> cases() {
    return Stream.of(false, true)
        .flatMap(
            compiled ->
                Stream.of(false, true)
                    .flatMap(
                        append ->
                            Stream.of("DISABLED", "ALWAYS")
                                .flatMap(
                                    uid ->
                                        Stream.of(0, 16)
                                            .flatMap(
                                                cache ->
                                                    Stream.of(false, true)
                                                        .map(
                                                            mini ->
                                                                Arguments.of(
                                                                    compiled, append, uid, cache,
                                                                    mini))))));
  }

  @ParameterizedTest(name = "compiled={0},append={1},uid={2},cache={3},mini={4}")
  @MethodSource("cases")
  void capturedHandlerKeepsStockAccumulatorKeyAndPortablePlan(
      boolean compiled, boolean append, String uid, int cache, boolean mini) throws Exception {
    String previous = System.getProperty("streamfusion.experimental.jvmAccumulatorCacheEntries");
    System.setProperty(
        "streamfusion.experimental.jvmAccumulatorCacheEntries", Integer.toString(cache));
    try {
      var stock = graph(false, compiled, append, uid, mini);
      var nativeGraph = graph(true, compiled, append, uid, mini);
      var stockNode =
          stock.getStreamNodes().stream()
              .filter(
                  n -> {
                    if (!(n.getOperatorFactory() instanceof SimpleOperatorFactory)) return false;
                    var op = ((SimpleOperatorFactory<?>) n.getOperatorFactory()).getOperator();
                    return mini
                        ? op.getClass()
                            == org.apache.flink.table.runtime.operators.bundle
                                .KeyedMapBundleOperator.class
                        : op instanceof KeyedProcessOperator
                            && ((KeyedProcessOperator<?, ?, ?>) op).getUserFunction()
                                instanceof GroupAggFunction;
                  })
              .findFirst()
              .orElseThrow();
      var stockOperator = ((SimpleOperatorFactory<?>) stockNode.getOperatorFactory()).getOperator();
      var original =
          mini
              ? field(stockOperator, "function")
              : ((KeyedProcessOperator<?, ?, ?>) stockOperator).getUserFunction();
      var owners =
          nativeGraph.getStreamNodes().stream()
              .filter(
                  n ->
                      n.getOperatorFactory() instanceof SimpleOperatorFactory
                          && ((SimpleOperatorFactory<?>) n.getOperatorFactory()).getOperator()
                              instanceof NativeColumnarJvmGroupOperator)
              .collect(java.util.stream.Collectors.toList());
      assertEquals(1, owners.size(), nativeGraph.getStreamingPlanAsJSON());
      var node = owners.get(0);
      assertTrue(node.getOperatorName().startsWith("NativeColumnarJvmGroup"));
      var operator = ((SimpleOperatorFactory<?>) node.getOperatorFactory()).getOperator();
      var captured = field(operator, mini ? "miniBatchFunction" : "function");
      assertEquals(
          mini
              ? org.apache.flink.table.runtime.operators.aggregate.MiniBatchGroupAggFunction.class
              : GroupAggFunction.class,
          captured.getClass());
      if (mini) {
        assertEquals(
            field(field(stockOperator, "bundleTrigger"), "maxCount"),
            field(field(operator, "miniBatchTrigger"), "maxCount"));
      }
      assertArrayEquals(
          (LogicalType[]) field(original, "accTypes"), (LogicalType[]) field(captured, "accTypes"));
      assertEquals(
          ((RowDataKeySelector) stockNode.getStatePartitioners()[0]).getProducedType(),
          ((RowDataKeySelector) field(operator, "keySelector")).getProducedType());
      assertEquals(257, tech.streamfusion.compat.FlinkPlanCompat.maximumParallelism(node));
      assertEquals(2, node.getParallelism());
      assertEquals(cache, field(operator, "cacheEntries"));
      assertEquals(uid.equals("DISABLED"), node.getTransformationUID() == null);
      assertTrue(
          nativeGraph.getStreamNodes().stream()
              .anyMatch(n -> n.getOperatorName().contains("RowDataToArrow")));
      assertTrue(
          nativeGraph.getStreamNodes().stream()
              .anyMatch(n -> n.getOperatorName().contains("ArrowToRowData")));
    } finally {
      if (previous == null)
        System.clearProperty("streamfusion.experimental.jvmAccumulatorCacheEntries");
      else System.setProperty("streamfusion.experimental.jvmAccumulatorCacheEntries", previous);
    }
  }

  static StreamGraph graph(
      boolean nativeRun, boolean compiled, boolean append, String uid, boolean mini)
      throws Exception {
    return graph(nativeRun, compiled, append, uid, mini, false);
  }

  static StreamGraph graph(
      boolean nativeRun,
      boolean compiled,
      boolean append,
      String uid,
      boolean mini,
      boolean twoPhase)
      throws Exception {
    return graph(
        nativeRun,
        compiled,
        append,
        uid,
        mini,
        twoPhase,
        org.apache.flink.table.types.logical.RowType.of(
            new org.apache.flink.table.types.logical.BigIntType()));
  }

  static StreamGraph graph(
      boolean nativeRun,
      boolean compiled,
      boolean append,
      String uid,
      boolean mini,
      boolean twoPhase,
      org.apache.flink.table.types.logical.RowType keys)
      throws Exception {
    var config = new Configuration();
    config.setString("streamfusion.native.enabled", Boolean.toString(nativeRun));
    config.setString("parallelism.default", "2");
    config.setString("pipeline.max-parallelism", "257");
    config.setString("table.exec.uid.generation", uid);
    var table = NativePlannerTestEnvironment.create(config);
    table.getConfig().set("table.exec.mini-batch.enabled", Boolean.toString(mini));
    table.getConfig().set("table.exec.mini-batch.size", "5");
    table.getConfig().set("table.exec.mini-batch.allow-latency", "1 h");
    table
        .getConfig()
        .set("table.optimizer.agg-phase-strategy", twoPhase ? "TWO_PHASE" : "ONE_PHASE");
    table.createTemporarySystemFunction(
        "fixture_history",
        twoPhase
            ? CanonicalFixtureFactory.MergeHistoryAggregate.class
            : CanonicalFixtureFactory.HistoryAggregate.class);
    table.registerCatalog(
        "fixture",
        new org.apache.flink.table.catalog.GenericInMemoryCatalog("fixture", "db") {
          @Override
          public Optional<org.apache.flink.table.factories.Factory> getFactory() {
            return Optional.of(new CanonicalFixtureFactory());
          }
        });
    table.useCatalog("fixture");
    table.executeSql(
        "CREATE TABLE input_table("
            + ManagedUdfGroupStockFixture.keyDdl(keys)
            + ",v BIGINT) WITH ('connector'='canonical-fixture',"
            + "'input'='unused.csv','ready'='unused.ready','insert-only'='"
            + append
            + "')");
    table.useCatalog("default_catalog");
    table.executeSql(
        "CREATE TABLE output_table("
            + ManagedUdfGroupStockFixture.keyDdl(keys)
            + ",h BIGINT,n BIGINT) WITH ('connector'='blackhole')");
    String sql =
        "INSERT INTO output_table SELECT "
            + ManagedUdfGroupStockFixture.keyColumns(keys)
            + ",fixture_history(v),COUNT(*) FROM fixture.db.input_table GROUP BY "
            + ManagedUdfGroupStockFixture.keyColumns(keys);
    var planner = (PlannerBase) ((TableEnvironmentImpl) table).getPlanner();
    var portable = compiled ? table.compilePlanSql(sql) : null;
    String json = portable == null ? "" : portable.asJsonString();
    assertFalse(json.contains("tech.streamfusion"));
    var transformations =
        compiled
            ? planner.translatePlan(planner.loadPlan(PlanReference.fromJsonString(json)))
            : CanonicalRegularJoinPlanTest.translate(planner, sql);
    var graph = CanonicalRegularJoinPlanTest.graph(planner, table, transformations);
    if (compiled) assertEquals(json, portable.asJsonString());
    return graph;
  }
}
