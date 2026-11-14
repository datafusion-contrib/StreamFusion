package tech.streamfusion.planner.stream;

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
import org.example.canonical.CanonicalFixtureFactory;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import tech.streamfusion.CanonicalNestedJoinFixture;
import tech.streamfusion.NativePlannerTestEnvironment;
import tech.streamfusion.operator.AbstractNativeStatefulOperator;

class CanonicalNestedJoinPlanTest {
  static Stream<Arguments> cases() {
    return Stream.of("UpdatingJoin", "IntervalJoin", "TemporalJoin", "WindowJoin")
        .flatMap(family -> Stream.of(false, true).map(compiled -> Arguments.of(family, compiled)));
  }

  @ParameterizedTest
  @MethodSource("cases")
  void canonicalBindingRetainsTheCompleteNestedRows(String family, boolean compiled)
      throws Exception {
    var config = new Configuration();
    config.setString("streamfusion.native.enabled", "true");
    config.setString("parallelism.default", "2");
    var table = NativePlannerTestEnvironment.create(config, CanonicalFixtureFactory.class);
    table.createTemporarySystemFunction("nested_checksum", CanonicalFixtureFactory.NestedJoinChecksum.class);
    for (String name : new String[] {"input_table", "right_input_table"})
      table.executeSql("CREATE TABLE " + name + "("
          + CanonicalNestedJoinFixture.columns(family.equals("TemporalJoin") && name.startsWith("right"))
          + ") WITH ('connector'='canonical-fixture','input'='unused.csv','insert-only'='true')");
    table.executeSql("CREATE TABLE output_table(k BIGINT,n BIGINT,total BIGINT) WITH ('connector'='blackhole')");
    var planner = (PlannerBase) ((TableEnvironmentImpl) table).getPlanner();
    String query = CanonicalNestedJoinFixture.query(family);
    var plan = compiled ? table.compilePlanSql(query) : null;
    String portable = compiled ? plan.asJsonString() : "";
    var transformations = compiled
        ? planner.translatePlan(planner.loadPlan(PlanReference.fromJsonString(portable)))
        : CanonicalRegularJoinPlanTest.translate(planner, query);
    var graph = CanonicalRegularJoinPlanTest.graph(planner, table, transformations);
    var owners = graph.getStreamNodes().stream()
        .filter(node -> node.getOperatorFactory() instanceof SimpleOperatorFactory
            && ((SimpleOperatorFactory<?>) node.getOperatorFactory()).getOperator()
                instanceof AbstractNativeStatefulOperator)
        .collect(java.util.stream.Collectors.toList());
    assertEquals(1, owners.size(), graph.getStreamingPlanAsJSON());
    var owner = owners.get(0);
    var operator = ((SimpleOperatorFactory<?>) owner.getOperatorFactory()).getOperator();
    assertNotNull(GroupAggregateStockTestFixture.field(operator, "stockStateContract"));
    for (String field : new String[] {"leftType", "rightType"}) {
      var input = (org.apache.flink.table.types.logical.RowType)
          GroupAggregateStockTestFixture.field(operator, field);
      assertTrue(input.getChildren().stream().anyMatch(type ->
          type.is(org.apache.flink.table.types.logical.LogicalTypeFamily.CONSTRUCTED)), input.toString());
    }
    assertTrue(owner.getOperatorName().contains(family), owner.getOperatorName());
    Map<?, ?> registrations = InstantiationUtil.deserializeObject(Base64.getDecoder().decode(
        table.getConfig().getConfiguration().getString("streamfusion.canonical.state-contracts", "")),
        getClass().getClassLoader());
    var hashes = new StreamGraphHasherV2().traverseStreamGraphAndGenerateHashes(graph);
    assertTrue(registrations.containsKey(new OperatorID(hashes.get(owner.getId())).toString()));
    assertFalse(portable.contains("tech.streamfusion"));
    if (compiled) assertEquals(portable, plan.asJsonString());
  }
}
