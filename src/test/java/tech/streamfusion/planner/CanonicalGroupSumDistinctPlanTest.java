package tech.streamfusion.planner;

import static org.junit.jupiter.api.Assertions.*;
import static tech.streamfusion.planner.GroupAggregateStockTestFixture.field;

import java.util.*;
import java.util.stream.Stream;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.streaming.api.graph.StreamGraphHasherV2;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.table.api.PlanReference;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.types.logical.*;
import org.apache.flink.util.InstantiationUtil;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.*;
import tech.streamfusion.NativePlannerTestEnvironment;
import tech.streamfusion.operator.NativeColumnarGroupAggregateOperator;
import tech.streamfusion.state.StockGroupDistinctView;
import tech.streamfusion.state.StockGroupTypedStateContract;

class CanonicalGroupSumDistinctPlanTest {
  static Stream<Arguments> cases() {
    return CanonicalGroupSumDistinctTest.types()
        .flatMap(
            type ->
                Stream.of(false, true)
                    .flatMap(
                        compiled ->
                            Stream.of(false, true)
                                .flatMap(
                                    mini ->
                                        Stream.of("DISABLED", "ALWAYS")
                                            .map(uid -> Arguments.of(type, compiled, mini, uid)))));
  }

  @ParameterizedTest(name = "{0},compiled={1},mini={2},uid={3}")
  @MethodSource("cases")
  void stockIdentityAndSharedSumCountSchemaBindInBothSubmissionForms(
      String type, boolean compiled, boolean mini, String uid) throws Exception {
    var config = new Configuration();
    config.setString("streamfusion.native.enabled", "true");
    config.setString("parallelism.default", "2");
    config.setString("pipeline.max-parallelism", "257");
    config.setString("table.exec.uid.generation", uid);
    var table = NativePlannerTestEnvironment.create(config);
    table.getConfig().set("table.exec.mini-batch.enabled", Boolean.toString(mini));
    if (mini) {
      table.getConfig().set("table.exec.mini-batch.size", "5");
      table.getConfig().set("table.exec.mini-batch.allow-latency", "1 h");
    }
    table.getConfig().set("table.optimizer.agg-phase-strategy", "ONE_PHASE");
    table.registerCatalog(
        "fixture",
        new org.apache.flink.table.catalog.GenericInMemoryCatalog("fixture", "db") {
          @Override
          public Optional<org.apache.flink.table.factories.Factory> getFactory() {
            return Optional.of(new org.example.canonical.CanonicalFixtureFactory());
          }
        });
    table.useCatalog("fixture");
    table.executeSql(
        "CREATE TABLE input_table(k STRING,t TIMESTAMP(3),v "
            + type
            + ",s STRING,f BOOLEAN) WITH"
            + " ('connector'='canonical-fixture','input'='unused.csv','ready'='unused.ready','insert-only'='true')");
    table.useCatalog("default_catalog");
    table.executeSql(
        "CREATE TABLE output_table(k STRING,t TIMESTAMP(3),n BIGINT,sd "
            + type
            + ",sf "
            + type
            + ",dn BIGINT,df BIGINT,av "
            + (type.equals("FLOAT") || type.equals("DOUBLE") ? "DOUBLE" : "BIGINT")
            + ",ds BIGINT) WITH ('connector'='blackhole')");
    String sql =
        "INSERT INTO output_table SELECT k,t,COUNT(*),SUM(DISTINCT v),"
            + "SUM(DISTINCT v) FILTER(WHERE f),COUNT(DISTINCT v),COUNT(DISTINCT v) FILTER(WHERE f),"
            + "AVG(v),COUNT(DISTINCT s) FROM fixture.db.input_table GROUP BY k,t";
    var planner = (PlannerBase) ((TableEnvironmentImpl) table).getPlanner();
    var portable = compiled ? table.compilePlanSql(sql) : null;
    String json = portable == null ? "" : portable.asJsonString();
    assertFalse(json.contains("tech.streamfusion"));
    var transformations =
        compiled
            ? planner.translatePlan(planner.loadPlan(PlanReference.fromJsonString(json)))
            : CanonicalRegularJoinPlanTest.translate(planner, sql);
    var graph = CanonicalRegularJoinPlanTest.graph(planner, table, transformations);
    var owners =
        graph.getStreamNodes().stream()
            .filter(
                n ->
                    n.getOperatorFactory() instanceof SimpleOperatorFactory
                        && ((SimpleOperatorFactory<?>) n.getOperatorFactory()).getOperator()
                            instanceof NativeColumnarGroupAggregateOperator)
            .collect(java.util.stream.Collectors.toList());
    assertEquals(1, owners.size(), graph.getStreamingPlanAsJSON());
    var node = owners.get(0);
    var operator = ((SimpleOperatorFactory<?>) node.getOperatorFactory()).getOperator();
    var contract =
        assertInstanceOf(StockGroupTypedStateContract.class, field(operator, "stockStateContract"));
    assertArrayEquals(new int[] {3, 9, 9, 7, 7, 4, 7}, (int[]) field(contract, "kinds"));
    assertEquals(2, ((List<?>) field(contract, "views")).size());
    assertEquals(5, contract.stateDescriptors().size());
    assertEquals(mini, field(operator, "miniBatch"));
    assertEquals(257, tech.streamfusion.compat.FlinkPlanCompat.maximumParallelism(node));
    assertEquals(2, node.getParallelism());
    assertEquals(uid.equals("DISABLED"), node.getTransformationUID() == null);
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
    assertTrue(registrations.containsKey(new OperatorID(hashes.get(node.getId())).toString()));
    assertTrue(
        graph.getStreamNodes().stream()
            .anyMatch(n -> n.getOperatorName().contains("RowDataToArrow")));
    assertTrue(
        graph.getStreamNodes().stream()
            .anyMatch(n -> n.getOperatorName().contains("ArrowToRowData")));
    if (compiled) assertEquals(json, portable.asJsonString());
  }

  @ParameterizedTest
  @ValueSource(strings = {"retract", "ttl", "decimal", "separate-count"})
  @SuppressWarnings("unchecked")
  void unqualifiedSumContractsFailBeforeNativeInitialization(String reason) throws Exception {
    try (var fixture = CanonicalGroupSumDistinctTest.fixture("INT", false, false)) {
      var source = fixture.contract;
      int[] kinds = ((int[]) field(source, "kinds")).clone();
      int[] scalars = ((int[]) field(source, "scalarFields")).clone();
      int[] counts = ((int[]) field(source, "countFields")).clone();
      RowType accumulator = (RowType) field(source, "accumulatorType");
      if (reason.equals("decimal")) {
        var fields = accumulator.getChildren().toArray(new LogicalType[0]);
        fields[scalars[1]] = new DecimalType(38, 6);
        accumulator = RowType.of(fields);
      }
      if (reason.equals("separate-count")) counts[1] = 0;
      RowType finalAccumulator = accumulator;
      assertThrows(
          IllegalArgumentException.class,
          () ->
              new StockGroupTypedStateContract(
                  source.keySerializer(),
                  (RowType) field(source, "keyType"),
                  finalAccumulator,
                  kinds,
                  scalars,
                  counts,
                  reason.equals("retract") ? 0 : -1,
                  reason.equals("ttl"),
                  (List<StockGroupDistinctView>) field(source, "views")));
    }
  }
}
