package tech.streamfusion.planner;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.config.OptimizerConfigOptions;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.FlinkConventions;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalDeltaJoin;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalJoin;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalUnion;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;

class DeltaJoinForceGateTest {

  private static final String JOIN_QUERY =
      "SELECT a.k, a.v, b.w FROM A AS a JOIN B AS b ON a.k = b.k";

  @Test
  void forceWithoutADeltaJoinLeavesThePlanForFlinkToReject() {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
    DataStream<Row> a =
        env.fromData(
            Types.ROW_NAMED(new String[] {"k", "v"}, Types.LONG, Types.LONG),
            Row.of(1L, 10L),
            Row.of(2L, 20L));
    DataStream<Row> b =
        env.fromData(
            Types.ROW_NAMED(new String[] {"k", "w"}, Types.LONG, Types.LONG),
            Row.of(1L, 100L),
            Row.of(2L, 200L));
    tEnv.createTemporaryView(
        "A",
        a,
        Schema.newBuilder().column("k", DataTypes.BIGINT()).column("v", DataTypes.BIGINT()).build());
    tEnv.createTemporaryView(
        "B",
        b,
        Schema.newBuilder().column("k", DataTypes.BIGINT()).column("w", DataTypes.BIGINT()).build());
    PhysicalPlanScan scan = NativePlanner.install(tEnv);
    tEnv.getConfig()
        .set(
            OptimizerConfigOptions.TABLE_OPTIMIZER_DELTA_JOIN_STRATEGY,
            OptimizerConfigOptions.DeltaJoinStrategy.FORCE);

    ValidationException failure =
        assertThrows(ValidationException.class, () -> tEnv.explainSql(JOIN_QUERY));

    assertTrue(
        failure.getMessage().contains(
            "The current sql doesn't support to do delta join optimization. The plan is:"),
        "expected Flink's own FORCE rejection, got: " + failure.getMessage());
    assertEquals(0, scan.substitutions(), scan::explainSummary);
    assertEquals(
        List.of(
            "delta join: table.optimizer.delta-join.strategy is FORCE but this optimizer block"
                + " contains a regular join and no delta join"),
        scan.fallbackReasons());
  }

  @Test
  void forceStillAcceleratesAPlanWithoutAJoin() {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
    DataStream<Row> a =
        env.fromData(
            Types.ROW_NAMED(new String[] {"k", "v"}, Types.LONG, Types.LONG),
            Row.of(1L, 10L),
            Row.of(2L, 20L));
    tEnv.createTemporaryView(
        "A",
        a,
        Schema.newBuilder().column("k", DataTypes.BIGINT()).column("v", DataTypes.BIGINT()).build());
    PhysicalPlanScan scan = NativePlanner.install(tEnv);
    tEnv.getConfig()
        .set(
            OptimizerConfigOptions.TABLE_OPTIMIZER_DELTA_JOIN_STRATEGY,
            OptimizerConfigOptions.DeltaJoinStrategy.FORCE);

    tEnv.explainSql("SELECT k, v * 2 FROM A");

    assertTrue(scan.substitutions() > 0, scan::explainSummary);
  }

  @Test
  void theDefaultStrategyAcceleratesTheSameJoin() {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
    DataStream<Row> a =
        env.fromData(
            Types.ROW_NAMED(new String[] {"k", "v"}, Types.LONG, Types.LONG),
            Row.of(1L, 10L),
            Row.of(2L, 20L));
    DataStream<Row> b =
        env.fromData(
            Types.ROW_NAMED(new String[] {"k", "w"}, Types.LONG, Types.LONG),
            Row.of(1L, 100L),
            Row.of(2L, 200L));
    tEnv.createTemporaryView(
        "A",
        a,
        Schema.newBuilder().column("k", DataTypes.BIGINT()).column("v", DataTypes.BIGINT()).build());
    tEnv.createTemporaryView(
        "B",
        b,
        Schema.newBuilder().column("k", DataTypes.BIGINT()).column("w", DataTypes.BIGINT()).build());
    PhysicalPlanScan scan = NativePlanner.install(tEnv);

    tEnv.explainSql(JOIN_QUERY);

    assertEquals(
        OptimizerConfigOptions.DeltaJoinStrategy.AUTO,
        tEnv.getConfig().get(OptimizerConfigOptions.TABLE_OPTIMIZER_DELTA_JOIN_STRATEGY));
    assertTrue(scan.substitutions() > 0, scan::explainSummary);
  }

  @Test
  void noneStrategyAcceleratesTheSameJoin() {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
    DataStream<Row> a =
        env.fromData(
            Types.ROW_NAMED(new String[] {"k", "v"}, Types.LONG, Types.LONG),
            Row.of(1L, 10L),
            Row.of(2L, 20L));
    DataStream<Row> b =
        env.fromData(
            Types.ROW_NAMED(new String[] {"k", "w"}, Types.LONG, Types.LONG),
            Row.of(1L, 100L),
            Row.of(2L, 200L));
    tEnv.createTemporaryView(
        "A",
        a,
        Schema.newBuilder().column("k", DataTypes.BIGINT()).column("v", DataTypes.BIGINT()).build());
    tEnv.createTemporaryView(
        "B",
        b,
        Schema.newBuilder().column("k", DataTypes.BIGINT()).column("w", DataTypes.BIGINT()).build());
    PhysicalPlanScan scan = NativePlanner.install(tEnv);
    tEnv.getConfig()
        .set(
            OptimizerConfigOptions.TABLE_OPTIMIZER_DELTA_JOIN_STRATEGY,
            OptimizerConfigOptions.DeltaJoinStrategy.NONE);

    tEnv.explainSql(JOIN_QUERY);

    assertTrue(scan.substitutions() > 0, scan::explainSummary);
  }

  @Test
  void forceLeavesARegularJoinBlockUnchangedWhileNoneAndAutoDoNotRejectIt() {
    TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inStreamingMode());
    tEnv.getConfig()
        .set(
            OptimizerConfigOptions.TABLE_OPTIMIZER_DELTA_JOIN_STRATEGY,
            OptimizerConfigOptions.DeltaJoinStrategy.FORCE);
    PlannerBase planner = (PlannerBase) ((TableEnvironmentImpl) tEnv).getPlanner();
    RelOptCluster cluster = planner.plannerContext().getCluster();
    RelTraitSet traits = cluster.traitSetOf(FlinkConventions.STREAM_PHYSICAL());
    RelDataType rowType = cluster.getTypeFactory().builder().add("k", SqlTypeName.BIGINT).build();
    RelNode left = LogicalValues.createEmpty(cluster, rowType);
    RelNode right = LogicalValues.createEmpty(cluster, rowType);
    RexNode condition =
        cluster.getRexBuilder()
            .makeCall(
                SqlStdOperatorTable.EQUALS,
                cluster.getRexBuilder().makeInputRef(left, 0),
                cluster.getRexBuilder().makeInputRef(rowType.getFieldList().get(0).getType(), 1));
    StreamPhysicalJoin regular =
        new StreamPhysicalJoin(
            cluster, traits, left, right, condition, JoinRelType.INNER, List.of());
    PhysicalPlanScan scan = new PhysicalPlanScan();

    assertTrue(PhysicalPlanScan.deltaJoinForceWouldReject(regular));
    assertSame(regular, scan.optimize(regular, null));
    assertEquals(0, scan.substitutions(), scan::explainSummary);
    assertEquals(
        List.of(
            "delta join: table.optimizer.delta-join.strategy is FORCE but this optimizer block"
                + " contains a regular join and no delta join"),
        scan.fallbackReasons());

    tEnv.getConfig()
        .set(
            OptimizerConfigOptions.TABLE_OPTIMIZER_DELTA_JOIN_STRATEGY,
            OptimizerConfigOptions.DeltaJoinStrategy.NONE);
    assertFalse(PhysicalPlanScan.deltaJoinForceWouldReject(regular));

    tEnv.getConfig()
        .set(
            OptimizerConfigOptions.TABLE_OPTIMIZER_DELTA_JOIN_STRATEGY,
            OptimizerConfigOptions.DeltaJoinStrategy.AUTO);
    assertFalse(PhysicalPlanScan.deltaJoinForceWouldReject(regular));
  }

  @Test
  void forceDoesNotRejectADeltaOnlyBlock() {
    TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inStreamingMode());
    tEnv.getConfig()
        .set(
            OptimizerConfigOptions.TABLE_OPTIMIZER_DELTA_JOIN_STRATEGY,
            OptimizerConfigOptions.DeltaJoinStrategy.FORCE);
    PlannerBase planner = (PlannerBase) ((TableEnvironmentImpl) tEnv).getPlanner();
    RelOptCluster cluster = planner.plannerContext().getCluster();
    RelTraitSet traits = cluster.traitSetOf(FlinkConventions.STREAM_PHYSICAL());
    RelDataType rowType = cluster.getTypeFactory().builder().add("k", SqlTypeName.BIGINT).build();
    RelNode left = LogicalValues.createEmpty(cluster, rowType);
    RelNode right = LogicalValues.createEmpty(cluster, rowType);
    RexNode condition =
        cluster.getRexBuilder()
            .makeCall(
                SqlStdOperatorTable.EQUALS,
                cluster.getRexBuilder().makeInputRef(left, 0),
                cluster.getRexBuilder().makeInputRef(rowType.getFieldList().get(0).getType(), 1));
    RelDataType outputType =
        cluster.getTypeFactory()
            .builder()
            .add("left_k", SqlTypeName.BIGINT)
            .add("right_k", SqlTypeName.BIGINT)
            .build();
    // Lookup specs are unused by the guard, which only inspects node types and inputs.
    StreamPhysicalDeltaJoin delta =
        new StreamPhysicalDeltaJoin(
            cluster, traits, List.of(), left, right, JoinRelType.INNER, condition,
            null, null, outputType);

    assertFalse(PhysicalPlanScan.deltaJoinForceWouldReject(delta));
    assertFalse(PhysicalPlanScan.deltaJoinForceWouldReject(left));
  }

  @Test
  void forceFindsNestedDeltaJoinsWithoutExemptingOtherBlocks() {
    TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inStreamingMode());
    tEnv.getConfig()
        .set(
            OptimizerConfigOptions.TABLE_OPTIMIZER_DELTA_JOIN_STRATEGY,
            OptimizerConfigOptions.DeltaJoinStrategy.FORCE);
    PlannerBase planner = (PlannerBase) ((TableEnvironmentImpl) tEnv).getPlanner();
    RelOptCluster cluster = planner.plannerContext().getCluster();
    RelTraitSet traits = cluster.traitSetOf(FlinkConventions.STREAM_PHYSICAL());
    RelDataType rowType = cluster.getTypeFactory().builder().add("k", SqlTypeName.BIGINT).build();
    RelNode left = LogicalValues.createEmpty(cluster, rowType);
    RelNode right = LogicalValues.createEmpty(cluster, rowType);
    RexNode condition =
        cluster.getRexBuilder()
            .makeCall(
                SqlStdOperatorTable.EQUALS,
                cluster.getRexBuilder().makeInputRef(left, 0),
                cluster.getRexBuilder().makeInputRef(rowType.getFieldList().get(0).getType(), 1));
    StreamPhysicalJoin regular =
        new StreamPhysicalJoin(
            cluster, traits, left, right, condition, JoinRelType.INNER, List.of());
    // No lookup execution is needed to exercise the production traversal.
    StreamPhysicalDeltaJoin delta =
        new StreamPhysicalDeltaJoin(
            cluster, traits, List.of(), left, right, JoinRelType.INNER, condition,
            null, null, regular.getRowType());
    StreamPhysicalUnion nested =
        new StreamPhysicalUnion(
            cluster, traits, List.of(regular, delta), true, regular.getRowType());
    StreamPhysicalUnion regularOnly =
        new StreamPhysicalUnion(
            cluster, traits, List.of(regular, regular), true, regular.getRowType());
    RexNode outerCondition =
        cluster.getRexBuilder()
            .makeCall(
                SqlStdOperatorTable.EQUALS,
                cluster.getRexBuilder().makeInputRef(regular, 0),
                cluster.getRexBuilder().makeInputRef(rowType.getFieldList().get(0).getType(), 2));
    StreamPhysicalJoin deltaOnRight =
        new StreamPhysicalJoin(
            cluster, traits, regular, nested, outerCondition, JoinRelType.INNER, List.of());
    StreamPhysicalJoin deltaOnLeft =
        new StreamPhysicalJoin(
            cluster, traits, nested, regular, outerCondition, JoinRelType.INNER, List.of());

    assertTrue(PhysicalPlanScan.deltaJoinForceWouldReject(regularOnly));
    assertFalse(PhysicalPlanScan.deltaJoinForceWouldReject(deltaOnRight));
    assertFalse(PhysicalPlanScan.deltaJoinForceWouldReject(deltaOnLeft));
    assertTrue(PhysicalPlanScan.deltaJoinForceWouldReject(regularOnly));
  }
}
