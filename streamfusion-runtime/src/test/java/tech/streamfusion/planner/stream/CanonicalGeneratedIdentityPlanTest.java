package tech.streamfusion.planner.stream;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Base64;
import java.util.Map;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.graph.StreamGraphHasherV2;
import org.apache.flink.table.api.PlanReference;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;
import org.apache.flink.table.operations.ModifyOperation;
import org.apache.flink.table.planner.delegation.DefaultExecutor;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.util.InstantiationUtil;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import tech.streamfusion.NativePlannerTestEnvironment;

class CanonicalGeneratedIdentityPlanTest {
  @ParameterizedTest
  @CsvSource(
      value = {
        "TemporalJoin|DECIMAL(25,2)", "IntervalJoin|DECIMAL(25,2)", "WindowJoin|DECIMAL(25,2)",
        "TemporalJoin|TIMESTAMP(9)", "IntervalJoin|TIMESTAMP(9)", "WindowJoin|TIMESTAMP(9)"
      },
      delimiter = '|')
  void unqualifiedKeyLayoutKeepsTheExistingNativeRoute(String family, String keyType)
      throws Exception {
    var config = new Configuration();
    config.setString("streamfusion.native.enabled", "true");
    var table = NativePlannerTestEnvironment.create(config);
    table.executeSql(
        "CREATE TABLE left_input(k "
            + keyType
            + ",v BIGINT,rt TIMESTAMP(3),"
            + "WATERMARK FOR rt AS rt) WITH ('connector'='datagen')");
    table.executeSql(
        "CREATE TABLE right_input(k "
            + keyType
            + " NOT NULL,v BIGINT,rt TIMESTAMP(3),"
            + "WATERMARK FOR rt AS rt,PRIMARY KEY(k) NOT ENFORCED) WITH ('connector'='datagen')");
    table.executeSql(
        "CREATE TABLE output_table(k "
            + keyType
            + ",l BIGINT,r BIGINT) WITH ('connector'='blackhole')");
    String query;
    if (family.equals("TemporalJoin"))
      query =
          "SELECT l.k,l.v,r.v FROM left_input l LEFT JOIN right_input FOR SYSTEM_TIME AS OF l.rt"
              + " AS r ON l.k=r.k";
    else if (family.equals("IntervalJoin"))
      query =
          "SELECT l.k,l.v,r.v FROM left_input l FULL JOIN right_input r ON l.k=r.k AND"
              + " l.rt BETWEEN r.rt - INTERVAL '1' SECOND AND r.rt + INTERVAL '1' SECOND";
    else
      query =
          "SELECT l.k,l.v,r.v FROM (SELECT * FROM TABLE(TUMBLE(TABLE"
              + " left_input,DESCRIPTOR(rt),INTERVAL '3' SECOND))) l FULL JOIN (SELECT * FROM"
              + " TABLE(TUMBLE(TABLE right_input,DESCRIPTOR(rt),INTERVAL '3' SECOND))) r ON l.k=r.k"
              + " AND l.window_start=r.window_start AND l.window_end=r.window_end";
    var planner = (PlannerBase) ((TableEnvironmentImpl) table).getPlanner();
    var transformations =
        planner.translate(
            planner.getParser().parse("INSERT INTO output_table " + query).stream()
                .map(operation -> (ModifyOperation) operation)
                .collect(java.util.stream.Collectors.toList()));
    var graph =
        (StreamGraph)
            new DefaultExecutor(planner.getExecEnv())
                .createPipeline(transformations, table.getConfig().getConfiguration(), null);
    assertTrue(
        graph.getStreamNodes().stream()
            .anyMatch(node -> node.getOperatorName().contains("Native" + family)),
        graph.getStreamingPlanAsJSON());
    assertEquals(
        "",
        table
            .getConfig()
            .getConfiguration()
            .getString("streamfusion.canonical.state-contracts", ""));
    assertFalse(
        Boolean.parseBoolean(
            table
                .getConfig()
                .getConfiguration()
                .getString("streamfusion.canonical.metadata-projection-active", "false")));
  }

  @ParameterizedTest
  @CsvSource({
    "TemporalJoin,DISABLED,true", "IntervalJoin,DISABLED,true", "WindowJoin,DISABLED,true",
    "TemporalJoin,ALWAYS,true", "IntervalJoin,ALWAYS,true", "WindowJoin,ALWAYS,true",
    "TemporalJoin,DISABLED,false", "IntervalJoin,DISABLED,false", "WindowJoin,DISABLED,false",
    "TemporalJoin,ALWAYS,false", "IntervalJoin,ALWAYS,false", "WindowJoin,ALWAYS,false",
    "TemporalJoin,PLAN_ONLY,false", "IntervalJoin,PLAN_ONLY,false", "WindowJoin,PLAN_ONLY,false"
  })
  void stateContractUsesTheActualRuntimePrimaryId(
      String family, String uidGeneration, boolean compiled) throws Exception {
    var config = new Configuration();
    config.setString("streamfusion.native.enabled", "true");
    config.setString("parallelism.default", "2");
    config.setString("table.exec.uid.generation", uidGeneration);
    var table = NativePlannerTestEnvironment.create(config);
    table.executeSql(
        "CREATE TABLE left_input(k BIGINT,v BIGINT,rt TIMESTAMP(3),WATERMARK FOR rt AS rt)"
            + " WITH ('connector'='datagen')");
    table.executeSql(
        "CREATE TABLE right_input(k BIGINT NOT NULL,v BIGINT,rt TIMESTAMP(3),"
            + "WATERMARK FOR rt AS rt,PRIMARY KEY(k) NOT ENFORCED) WITH ('connector'='datagen')");
    table.executeSql(
        "CREATE TABLE output_table(k BIGINT,l BIGINT,r BIGINT) WITH ('connector'='blackhole')");
    String query;
    if (family.equals("TemporalJoin"))
      query =
          "SELECT l.k,l.v,r.v FROM left_input l LEFT JOIN right_input FOR SYSTEM_TIME AS OF l.rt"
              + " AS r ON l.k=r.k";
    else if (family.equals("IntervalJoin"))
      query =
          "SELECT l.k,l.v,r.v FROM left_input l FULL JOIN right_input r ON l.k=r.k AND"
              + " l.rt BETWEEN r.rt - INTERVAL '1' SECOND AND r.rt + INTERVAL '1' SECOND";
    else
      query =
          "SELECT l.k,l.v,r.v FROM (SELECT * FROM TABLE(TUMBLE(TABLE"
              + " left_input,DESCRIPTOR(rt),INTERVAL '3' SECOND))) l FULL JOIN (SELECT * FROM"
              + " TABLE(TUMBLE(TABLE right_input,DESCRIPTOR(rt),INTERVAL '3' SECOND))) r ON l.k=r.k"
              + " AND l.window_start=r.window_start AND l.window_end=r.window_end";
    String insert = "INSERT INTO output_table " + query;
    var plan = compiled ? table.compilePlanSql(insert) : null;
    String portable = plan == null ? "" : plan.asJsonString();
    assertFalse(portable.contains("tech.streamfusion"));
    var planner = (PlannerBase) ((TableEnvironmentImpl) table).getPlanner();
    var transformations =
        compiled
            ? planner.translatePlan(planner.loadPlan(PlanReference.fromJsonString(portable)))
            : planner.translate(
                planner.getParser().parse(insert).stream()
                    .map(operation -> (ModifyOperation) operation)
                    .collect(java.util.stream.Collectors.toList()));
    var graph =
        (StreamGraph)
            new DefaultExecutor(planner.getExecEnv())
                .createPipeline(transformations, table.getConfig().getConfiguration(), null);
    var hashes = new StreamGraphHasherV2().traverseStreamGraphAndGenerateHashes(graph);
    Map<?, ?> contracts =
        InstantiationUtil.deserializeObject(
            Base64.getDecoder()
                .decode(
                    table
                        .getConfig()
                        .getConfiguration()
                        .getString("streamfusion.canonical.state-contracts", "")),
            getClass().getClassLoader());
    var owners =
        graph.getStreamNodes().stream()
            .filter(node -> node.getOperatorName().contains("Native" + family))
            .collect(java.util.stream.Collectors.toList());
    assertEquals(1, owners.size(), graph.getStreamingPlanAsJSON());
    var owner = owners.get(0);
    assertEquals(
        uidGeneration.equals("DISABLED") || !compiled && uidGeneration.equals("PLAN_ONLY"),
        owner.getTransformationUID() == null);
    assertEquals(1, contracts.size());
    assertTrue(contracts.containsKey(new OperatorID(hashes.get(owner.getId())).toString()));
    assertTrue(
        Boolean.parseBoolean(
            table
                .getConfig()
                .getConfiguration()
                .getString("streamfusion.canonical.metadata-projection-active", "false")));
    if (compiled) assertEquals(portable, plan.asJsonString());
    table.getConfig().set("streamfusion.native.enabled", "false");
    var stock =
        compiled
            ? planner.translatePlan(planner.loadPlan(PlanReference.fromJsonString(portable)))
            : planner.translate(
                planner.getParser().parse(insert).stream()
                    .map(operation -> (ModifyOperation) operation)
                    .collect(java.util.stream.Collectors.toList()));
    assertFalse(stock.isEmpty());
    assertEquals(
        "",
        table
            .getConfig()
            .getConfiguration()
            .getString("streamfusion.canonical.state-contracts", ""));
  }
}
