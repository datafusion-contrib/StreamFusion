package tech.streamfusion.planner.stream;

import static org.junit.jupiter.api.Assertions.*;
import static tech.streamfusion.planner.stream.GroupAggregateStockTestFixture.field;

import java.util.*;
import java.util.stream.Stream;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.graph.StreamGraphHasherV2;
import org.apache.flink.table.api.PlanReference;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.StringUtils;
import org.example.canonical.CanonicalFixtureFactory;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.*;
import tech.streamfusion.NativePlannerTestEnvironment;
import tech.streamfusion.operator.NativeColumnarJvmGroupOperator;
import tech.streamfusion.state.OperatorIdentityProjection;

class CanonicalManagedUdfGeneratedUidPlanTest {
  static Stream<Arguments> cases() {
    return Stream.of("BIGINT", "DECIMAL(38,2)", "TIMESTAMP(9)")
        .flatMap(
            type ->
                Stream.of(0, 1, 2)
                    .flatMap(
                        mode ->
                            Stream.of("ALWAYS", "DISABLED", "PLAN_ONLY")
                                .flatMap(
                                    uid ->
                                        Stream.of(false, true)
                                            .flatMap(
                                                compiled ->
                                                    Stream.of(false, true)
                                                        .map(
                                                            calc ->
                                                                Arguments.of(
                                                                    type, mode, uid, compiled,
                                                                    calc))))));
  }

  @ParameterizedTest(name = "{0}:mode={1},uid={2},compiled={3},calc={4}")
  @MethodSource("cases")
  void canonicalIdentitiesMatchStockPlanningInAFreshWorker(
      String type, int mode, String uid, boolean compiled, boolean calc) throws Exception {
    // Fresh isolated workers each begin with Flink's initial execution-node counter.
    ExecNodeContext.resetIdCounter();
    var stock = graph(false, type, mode, uid, compiled, calc);
    ExecNodeContext.resetIdCounter();
    var actual = graph(true, type, mode, uid, compiled, calc);
    var owner =
        actual.graph.getStreamNodes().stream()
            .filter(
                n ->
                    n.getOperatorFactory()
                            instanceof
                            org.apache.flink.streaming.api.operators.SimpleOperatorFactory
                        && ((org.apache.flink.streaming.api.operators.SimpleOperatorFactory<?>)
                                    n.getOperatorFactory())
                                .getOperator()
                                .getClass()
                            == NativeColumnarJvmGroupOperator.class)
            .findFirst()
            .orElseThrow(() -> new AssertionError(actual.graph.getStreamingPlanAsJSON()));
    assertEquals(
        uid.equals("DISABLED") || uid.equals("PLAN_ONLY") && !compiled,
        owner.getTransformationUID() == null);
    assertTrue(
        actual.graph.getStreamNodes().stream()
            .anyMatch(n -> n.getOperatorName().contains("Writer")));
    assertTrue(
        actual.graph.getStreamNodes().stream()
            .anyMatch(n -> n.getOperatorName().contains("Committer")));
    Map<?, ?> exports = (Map<?, ?>) field(actual.identities, "exportIds");
    var expected = new HashSet<OperatorID>();
    new StreamGraphHasherV2()
        .traverseStreamGraphAndGenerateHashes(stock.graph)
        .values()
        .forEach(hash -> expected.add(new OperatorID(hash)));
    assertEquals(
        expected,
        new HashSet<>(exports.values()),
        "canonical stock IDs must match a fresh stock worker, including connector owners; stock="
            + roles(stock.graph)
            + "; native="
            + roles(actual.graph));
  }

  private static Map<String, String> roles(StreamGraph graph) {
    var hashes = new StreamGraphHasherV2().traverseStreamGraphAndGenerateHashes(graph);
    var roles = new TreeMap<String, String>();
    graph
        .getStreamNodes()
        .forEach(
            n ->
                roles.put(
                    n.getOperatorName() + ":uid=" + n.getTransformationUID(),
                    StringUtils.byteToHexString(hashes.get(n.getId()))));
    return roles;
  }

  private static final class Planned {
    final StreamGraph graph;
    final OperatorIdentityProjection identities;

    Planned(StreamGraph graph, OperatorIdentityProjection identities) {
      this.graph = graph;
      this.identities = identities;
    }
  }

  private static Planned graph(
      boolean nativeRun, String type, int mode, String uid, boolean compiled, boolean calc)
      throws Exception {
    var config = new Configuration();
    config.setString("streamfusion.native.enabled", Boolean.toString(nativeRun));
    config.setString("parallelism.default", "3");
    config.setString("pipeline.max-parallelism", "128");
    config.setString("table.exec.uid.generation", uid);
    var table = NativePlannerTestEnvironment.create(config, CanonicalFixtureFactory.class);
    table.getConfig().set("table.exec.mini-batch.enabled", Boolean.toString(mode > 0));
    table.getConfig().set("table.exec.mini-batch.size", "5");
    table.getConfig().set("table.exec.mini-batch.allow-latency", "1 h");
    table
        .getConfig()
        .set("table.optimizer.agg-phase-strategy", mode == 2 ? "TWO_PHASE" : "ONE_PHASE");
    table.createTemporarySystemFunction(
        "fixture_history",
        mode == 2
            ? CanonicalFixtureFactory.MergeHistoryAggregate.class
            : CanonicalFixtureFactory.HistoryAggregate.class);
    table.executeSql(
        "CREATE TABLE input_table(k "
            + (calc ? "BIGINT" : type)
            + ",v BIGINT) WITH"
            + " ('connector'='canonical-fixture','input'='unused.csv','ready'='unused.ready','parallelism'='1')");
    table.executeSql(
        "CREATE TABLE output_table(k "
            + (calc ? "BIGINT" : type)
            + ",h BIGINT,n BIGINT) WITH"
            + " ('connector'='canonical-fixture','output'='unused-output','parallelism'='1')");
    String key =
        type.equals("TIMESTAMP(9)")
            ? "CAST(CONCAT('1969-12-31 23:59:59.000000',CAST(k AS STRING),'1') AS TIMESTAMP(9))"
            : "CAST(k AS " + type + ")";
    String sql =
        calc
            ? "INSERT INTO output_table SELECT MAX(original_key),fixture_history(v),COUNT(*) FROM"
                + " (SELECT k AS original_key,v,"
                + key
                + " AS g FROM input_table) GROUP BY g"
            : "INSERT INTO output_table SELECT k,fixture_history(v),COUNT(*) FROM input_table GROUP"
                + " BY k";
    var planner = (PlannerBase) ((TableEnvironmentImpl) table).getPlanner();
    var plan = compiled ? table.compilePlanSql(sql) : null;
    var transformations =
        compiled
            ? planner.translatePlan(
                planner.loadPlan(PlanReference.fromJsonString(plan.asJsonString())))
            : CanonicalRegularJoinPlanTest.translate(planner, sql);
    var graph = CanonicalRegularJoinPlanTest.graph(planner, table, transformations);
    String encoded =
        table
            .getConfig()
            .getConfiguration()
            .getString("streamfusion.canonical.operator-identities", "");
    var identities =
        encoded.isEmpty()
            ? null
            : (OperatorIdentityProjection)
                InstantiationUtil.deserializeObject(
                    Base64.getDecoder().decode(encoded),
                    CanonicalManagedUdfGeneratedUidPlanTest.class.getClassLoader());
    if (nativeRun) assertNotNull(identities);
    return new Planned(graph, identities);
  }
}
