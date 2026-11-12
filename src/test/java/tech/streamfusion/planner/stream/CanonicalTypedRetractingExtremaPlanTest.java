package tech.streamfusion.planner.stream;

import static org.junit.jupiter.api.Assertions.*;
import static tech.streamfusion.planner.stream.GroupAggregateStockTestFixture.field;

import java.util.stream.Stream;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.table.api.PlanReference;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.runtime.typeutils.ExternalSerializer;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.utils.LogicalTypeParser;
import org.apache.flink.table.types.utils.DataTypeUtils;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import tech.streamfusion.NativePlannerTestEnvironment;
import tech.streamfusion.operator.NativeColumnarGroupAggregateOperator;
import tech.streamfusion.state.StockGroupExtremaStateContract;

class CanonicalTypedRetractingExtremaPlanTest {
  static Stream<Arguments> cases() {
    return CanonicalTypedRetractingExtremaTest.types()
        .flatMap(
            type ->
                Stream.of(false, true)
                    .flatMap(
                        compiled ->
                            Stream.of("BIGINT", "INT", "global")
                                .flatMap(
                                    key ->
                                        Stream.of("DISABLED", "ALWAYS")
                                            .flatMap(
                                                uid ->
                                                    Stream.of(0L, 1000L)
                                                        .map(
                                                            ttl ->
                                                                Arguments.of(
                                                                    type, compiled, key, uid,
                                                                    ttl))))));
  }

  @ParameterizedTest(name = "{0},compiled={1},key={2},uid={3},ttl={4}")
  @MethodSource("cases")
  void actualSubmissionCapturesTypedViewsAndStockIdentity(
      String type, boolean compiled, String key, String uid, long ttl) throws Exception {
    assertPlan(type, compiled, key, uid, ttl, false);
  }

  @ParameterizedTest(name = "counted:{0},compiled={1},key={2},uid={3},ttl={4}")
  @MethodSource("cases")
  void typedCountSharesOnlyItsActualScalarWithTheViewContract(
      String type, boolean compiled, String key, String uid, long ttl) throws Exception {
    assertPlan(type, compiled, key, uid, ttl, true);
  }

  private void assertPlan(
      String type, boolean compiled, String key, String uid, long ttl, boolean counted)
      throws Exception {
    boolean global = key.equals("global");
    var config = new Configuration();
    config.setString("streamfusion.native.enabled", "true");
    config.setString("parallelism.default", "2");
    config.setString("pipeline.max-parallelism", "257");
    config.setString("table.exec.uid.generation", uid);
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
            + (global ? "BIGINT" : key)
            + ",v "
            + type
            + ") WITH"
            + " ('connector'='canonical-fixture','input'='unused.csv','ready'='unused.ready','insert-only'='false')");
    table.useCatalog("default_catalog");
    table.executeSql(
        "CREATE TABLE output_table("
            + (global ? "" : "k " + key + ",")
            + (counted ? "c BIGINT," : "")
            + "a "
            + type
            + ",b "
            + type
            + ") WITH ('connector'='blackhole')");
    String query =
        "INSERT INTO output_table SELECT "
            + (global ? "" : "k,")
            + (counted ? "COUNT(v)," : "")
            + "MIN(v),MAX(v) FROM fixture.db.input_table"
            + (global ? "" : " GROUP BY k");
    var planner = (PlannerBase) ((TableEnvironmentImpl) table).getPlanner();
    var plan = compiled ? table.compilePlanSql(query) : null;
    if (plan != null) assertFalse(plan.asJsonString().contains("tech.streamfusion"));
    var transformations =
        compiled
            ? planner.translatePlan(
                planner.loadPlan(PlanReference.fromJsonString(plan.asJsonString())))
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
    assertEquals(
        global ? 1 : 257, tech.streamfusion.compat.FlinkPlanCompat.maximumParallelism(owner));
    assertEquals(global ? 1 : 2, owner.getParallelism());
    assertEquals(uid.equals("DISABLED"), owner.getTransformationUID() == null);
    var operator = ((SimpleOperatorFactory<?>) owner.getOperatorFactory()).getOperator();
    var contract =
        assertInstanceOf(
            StockGroupExtremaStateContract.class, field(operator, "stockStateContract"));
    assertArrayEquals(
        counted ? new int[] {3, 38, 39} : new int[] {38, 39}, (int[]) field(contract, "kinds"));
    assertEquals(global ? 0 : 1, ((RowType) field(contract, "keyType")).getFieldCount());
    // Released FlinkTypeFactory normalizes SQL TIME precision to zero.
    var logical =
        type.startsWith("TIME(")
            ? new org.apache.flink.table.types.logical.TimeType(0)
            : LogicalTypeParser.parse(type, getClass().getClassLoader());
    var serializers =
        (org.apache.flink.api.common.typeutils.TypeSerializer<?>[])
            field(contract, "mapKeySerializers");
    var values =
        (org.apache.flink.api.common.typeutils.TypeSerializer<?>[])
            field(contract, "mapValueSerializers");
    for (int i = counted ? 1 : 0; i < serializers.length; i++) {
      assertEquals(
          DataTypeUtils.toInternalDataType(logical.copy(false)),
          assertInstanceOf(ExternalSerializer.class, serializers[i]).getDataType());
      assertEquals(
          org.apache.flink.table.api.DataTypes.BIGINT(),
          assertInstanceOf(ExternalSerializer.class, values[i]).getDataType());
    }
    assertEquals(ttl > 0, field(contract, "ttl"));
  }
}
