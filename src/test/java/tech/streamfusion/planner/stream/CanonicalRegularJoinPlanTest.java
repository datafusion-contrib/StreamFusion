package tech.streamfusion.planner.stream;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.graph.StreamGraphHasherV2;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.table.api.PlanReference;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;
import org.apache.flink.table.operations.ModifyOperation;
import org.apache.flink.table.planner.delegation.DefaultExecutor;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.util.InstantiationUtil;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import tech.streamfusion.NativePlannerTestEnvironment;
import tech.streamfusion.operator.AbstractNativeStatefulOperator;
import tech.streamfusion.operator.NativeColumnarUpdatingJoinOperator;
import tech.streamfusion.state.StockJoinStateContract;

class CanonicalRegularJoinPlanTest {
  static Stream<Arguments> cases() {
    var cases = new ArrayList<Arguments>();
    for (String kind : List.of("INNER", "LEFT", "RIGHT", "FULL", "SEMI", "ANTI"))
      for (String shape : List.of("none", "join", "other"))
        for (boolean compiled : new boolean[] {false, true})
          cases.add(Arguments.of(kind, shape, compiled));
    return cases.stream();
  }

  @ParameterizedTest
  @MethodSource("cases")
  void stockContractsBindToActualNativeOwners(String kind, String shape, boolean compiled)
      throws Exception {
    var table = environment(shape, false);
    String query = query(kind);
    var planner = (PlannerBase) ((TableEnvironmentImpl) table).getPlanner();
    var plan = compiled ? table.compilePlanSql(query) : null;
    String portable = plan == null ? "" : plan.asJsonString();
    assertFalse(portable.contains("tech.streamfusion"));
    List<Transformation<?>> transformations =
        compiled
            ? planner.translatePlan(planner.loadPlan(PlanReference.fromJsonString(portable)))
            : translate(planner, query);
    var graph = graph(planner, table, transformations);
    var owners =
        graph.getStreamNodes().stream()
            .filter(node -> node.getOperatorName().contains("NativeColumnarUpdatingJoin"))
            .collect(java.util.stream.Collectors.toList());
    assertEquals(1, owners.size(), graph.getStreamingPlanAsJSON());
    var owner = owners.get(0);
    assertEquals(257, tech.streamfusion.compat.FlinkPlanCompat.maximumParallelism(owner));
    assertNull(owner.getTransformationUID());
    var operator = ((SimpleOperatorFactory<?>) owner.getOperatorFactory()).getOperator();
    assertInstanceOf(NativeColumnarUpdatingJoinOperator.class, operator);
    var field = AbstractNativeStatefulOperator.class.getDeclaredField("stockStateContract");
    field.setAccessible(true);
    var contract = assertInstanceOf(StockJoinStateContract.class, field.get(operator));
    var expected = shape.equals("join") ? StateDescriptor.Type.VALUE : StateDescriptor.Type.MAP;
    assertEquals(
        Map.of("left-records", expected, "right-records", expected), contract.stateDescriptors());
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
    assertFalse(translate(planner, query).isEmpty());
    assertEquals(
        "",
        table
            .getConfig()
            .getConfiguration()
            .getString("streamfusion.canonical.state-contracts", ""));
  }

  @ParameterizedTest
  @CsvSource({"none,false", "join,false", "other,false", "join,true"})
  void ordinaryBindingRetainsItsMiniBatchMode(String shape, boolean compiled) throws Exception {
    var table = environment(shape, true);
    var planner = (PlannerBase) ((TableEnvironmentImpl) table).getPlanner();
    var transformations =
        compiled
            ? planner.translatePlan(
                planner.loadPlan(
                    PlanReference.fromJsonString(
                        table.compilePlanSql(query("INNER")).asJsonString())))
            : translate(planner, query("INNER"));
    var graph = graph(planner, table, transformations);
    var owner =
        graph.getStreamNodes().stream()
            .filter(node -> node.getOperatorName().contains("NativeColumnarUpdatingJoin"))
            .findFirst()
            .orElseThrow();
    var operator = ((SimpleOperatorFactory<?>) owner.getOperatorFactory()).getOperator();
    var field = NativeColumnarUpdatingJoinOperator.class.getDeclaredField("miniBatch");
    field.setAccessible(true);
    assertEquals(
        !compiled || tech.streamfusion.compat.FlinkPlanCompat.releasedMiniBatchJoins(),
        field.get(operator));
    var binding = AbstractNativeStatefulOperator.class.getDeclaredField("stockStateContract");
    binding.setAccessible(true);
    assertInstanceOf(StockJoinStateContract.class, binding.get(operator));
  }

  @ParameterizedTest
  @CsvSource({"1,false", "172,false", "1,true", "172,true"})
  void omittedMaximumUsesTheReleasedFlinkDefault(int parallelism, boolean compiled)
      throws Exception {
    var table = environment("none", false, null);
    var planner = (PlannerBase) ((TableEnvironmentImpl) table).getPlanner();
    planner.getExecEnv().setParallelism(parallelism);
    table
        .getConfig()
        .getConfiguration()
        .set(org.apache.flink.configuration.CoreOptions.DEFAULT_PARALLELISM, parallelism);
    var transformations =
        compiled
            ? planner.translatePlan(
                planner.loadPlan(
                    PlanReference.fromJsonString(
                        table.compilePlanSql(query("INNER")).asJsonString())))
            : translate(planner, query("INNER"));
    var graph = graph(planner, table, transformations);
    var owner =
        graph.getStreamNodes().stream()
            .filter(node -> node.getOperatorName().contains("NativeColumnarUpdatingJoin"))
            .findFirst()
            .orElseThrow();
    assertEquals(
        org.apache.flink.runtime.state.KeyGroupRangeAssignment.computeDefaultMaxParallelism(
            parallelism),
        tech.streamfusion.compat.FlinkPlanCompat.maximumParallelism(owner));
    var operator = ((SimpleOperatorFactory<?>) owner.getOperatorFactory()).getOperator();
    var binding = AbstractNativeStatefulOperator.class.getDeclaredField("stockStateContract");
    binding.setAccessible(true);
    assertInstanceOf(StockJoinStateContract.class, binding.get(operator));
  }

  @ParameterizedTest
  @CsvSource({"false,false", "true,false", "false,true", "true,true"})
  void sharedSourceCalculationsKeepOneStockIdentity(boolean compiled, boolean watermarks)
      throws Exception {
    var table = environment("none", false);
    table.executeSql(
        "CREATE TABLE shared_source(k BIGINT,v BIGINT"
            + (watermarks
                ? ",rt AS TIMESTAMPADD(SECOND,CAST(k AS INT),TIMESTAMP '2026-01-01 00:00:00'),"
                    + "WATERMARK FOR rt AS rt"
                : "")
            + ") WITH ('connector'='datagen')");
    table.executeSql(
        "CREATE TEMPORARY VIEW shared_input AS SELECT k*2 AS k,v+7 AS v FROM shared_source"
            + " WHERE v>0");
    String query =
        "INSERT INTO output_table SELECT l.k,l.v,r.v FROM shared_input l"
            + " JOIN shared_input r ON l.k=r.k";
    var planner = (PlannerBase) ((TableEnvironmentImpl) table).getPlanner();
    var transformations =
        compiled
            ? planner.translatePlan(
                planner.loadPlan(
                    PlanReference.fromJsonString(table.compilePlanSql(query).asJsonString())))
            : translate(planner, query);
    var graph = graph(planner, table, transformations);
    assertEquals(
        1,
        graph.getStreamNodes().stream()
            .filter(node -> node.getOperatorName().contains("NativeColumnarUpdatingJoin"))
            .count());
    if (compiled && watermarks) {
      var sharedHost =
          graph.getStreamNodes().stream()
              .filter(node -> node.getOperatorName().startsWith("Calc["))
              .collect(java.util.stream.Collectors.toList());
      assertEquals(1, sharedHost.size(), graph.getStreamingPlanAsJSON());
      assertEquals(
          2,
          graph.getStreamNodes().stream()
              .filter(node -> node.getOperatorName().contains("RowDataToArrow"))
              .flatMap(node -> node.getInEdges().stream())
              .filter(edge -> edge.getSourceId() == sharedHost.get(0).getId())
              .count());
      assertEquals(
          1,
          graph.getStreamNodes().stream()
              .filter(node -> node.getOperatorName().startsWith("WatermarkAssigner["))
              .count());
    } else {
      assertTrue(
          graph.getStreamNodes().stream()
              .anyMatch(node -> node.getOperatorName().contains("NativeShare")),
          graph.getStreamingPlanAsJSON());
    }
    assertFalse(
        table
            .getConfig()
            .getConfiguration()
            .getString("streamfusion.canonical.state-contracts", "")
            .isEmpty());
    assertTrue(
        Boolean.parseBoolean(
            table
                .getConfig()
                .getConfiguration()
                .getString("streamfusion.canonical.metadata-projection-active", "false")));
  }

  @ParameterizedTest
  @CsvSource({"false,INNER", "true,INNER", "false,LEFT", "true,LEFT", "false,FULL", "true,FULL"})
  void keylessBindingPreservesTheExistingInnerJoinAdmission(boolean compiled, String kind)
      throws Exception {
    var table = environment("none", false);
    String query =
        "INSERT INTO output_table SELECT l.k,l.v,r.v FROM left_input l "
            + kind
            + " JOIN right_input r ON l.v<r.v";
    var planner = (PlannerBase) ((TableEnvironmentImpl) table).getPlanner();
    var transformations =
        compiled
            ? planner.translatePlan(
                planner.loadPlan(
                    PlanReference.fromJsonString(table.compilePlanSql(query).asJsonString())))
            : translate(planner, query);
    var graph = graph(planner, table, transformations);
    var nativeJoins =
        graph.getStreamNodes().stream()
            .filter(node -> node.getOperatorName().contains("NativeColumnarUpdatingJoin"))
            .collect(java.util.stream.Collectors.toList());
    assertEquals(kind.equals("INNER") ? 1 : 0, nativeJoins.size(), graph.getStreamingPlanAsJSON());
    if (kind.equals("INNER")) {
      var owner = nativeJoins.get(0);
      assertEquals(1, owner.getParallelism());
      assertEquals(1, tech.streamfusion.compat.FlinkPlanCompat.maximumParallelism(owner));
      var operator = ((SimpleOperatorFactory<?>) owner.getOperatorFactory()).getOperator();
      var binding = AbstractNativeStatefulOperator.class.getDeclaredField("stockStateContract");
      binding.setAccessible(true);
      assertInstanceOf(StockJoinStateContract.class, binding.get(operator));
    }
  }

  @ParameterizedTest
  @CsvSource({"false", "true"})
  void keylessJoinWithAnUpdatingInputKeepsItsStockOwner(boolean compiled) throws Exception {
    var table = environment("none", false);
    table.executeSql(
        "CREATE TEMPORARY VIEW updates AS SELECT k,SUM(v) AS v FROM left_input GROUP BY k");
    String query =
        "INSERT INTO output_table SELECT l.k,l.v,r.v FROM updates l JOIN right_input r ON l.v<r.v";
    var planner = (PlannerBase) ((TableEnvironmentImpl) table).getPlanner();
    var transformations =
        compiled
            ? planner.translatePlan(
                planner.loadPlan(
                    PlanReference.fromJsonString(table.compilePlanSql(query).asJsonString())))
            : translate(planner, query);
    var graph = graph(planner, table, transformations);
    assertTrue(
        graph.getStreamNodes().stream()
            .noneMatch(node -> node.getOperatorName().contains("NativeColumnarUpdatingJoin")),
        graph.getStreamingPlanAsJSON());
  }

  static List<Transformation<?>> translate(PlannerBase planner, String sql) {
    return planner.translate(
        planner.getParser().parse(sql).stream()
            .map(operation -> (ModifyOperation) operation)
            .collect(java.util.stream.Collectors.toList()));
  }

  static StreamGraph graph(
      PlannerBase planner, TableEnvironment table, List<Transformation<?>> transformations) {
    return (StreamGraph)
        new DefaultExecutor(planner.getExecEnv())
            .createPipeline(transformations, table.getConfig().getConfiguration(), null);
  }

  private static TableEnvironment environment(String shape, boolean miniBatch) throws Exception {
    return environment(shape, miniBatch, 257);
  }

  private static TableEnvironment environment(String shape, boolean miniBatch, Integer maximum)
      throws Exception {
    var config = new Configuration();
    config.setString("streamfusion.native.enabled", "true");
    config.setString("parallelism.default", "2");
    if (maximum != null) config.setString("pipeline.max-parallelism", maximum.toString());
    config.setString("table.exec.uid.generation", "DISABLED");
    if (miniBatch) {
      config.setString("table.exec.mini-batch.enabled", "true");
      config.setString("table.exec.mini-batch.allow-latency", "1 s");
      config.setString("table.exec.mini-batch.size", "3");
    }
    var table = NativePlannerTestEnvironment.create(config);
    String columns =
        "k BIGINT"
            + (shape.equals("join") ? " NOT NULL" : "")
            + ",v BIGINT,x BIGINT"
            + (shape.equals("other") ? " NOT NULL" : "")
            + (shape.equals("none")
                ? ""
                : ",PRIMARY KEY(" + (shape.equals("join") ? "k" : "x") + ") NOT ENFORCED");
    for (String name : List.of("left_input", "right_input"))
      table.executeSql("CREATE TABLE " + name + "(" + columns + ") WITH ('connector'='datagen')");
    table.executeSql(
        "CREATE TABLE output_table(k BIGINT,l BIGINT,r BIGINT) WITH ('connector'='blackhole')");
    return table;
  }

  private static String query(String kind) {
    boolean semi = kind.equals("SEMI") || kind.equals("ANTI");
    if (semi)
      return "INSERT INTO output_table SELECT l.k,l.v,l.x FROM left_input l WHERE "
          + (kind.equals("ANTI") ? "NOT " : "")
          + "EXISTS (SELECT * FROM right_input r WHERE l.k=r.k AND l.v>=r.v AND r.x>=0)";
    return "INSERT INTO output_table SELECT l.k,l.x,r.x FROM left_input l "
        + kind
        + " JOIN right_input r ON l.k=r.k AND l.v>=r.v AND r.x>=0";
  }
}
