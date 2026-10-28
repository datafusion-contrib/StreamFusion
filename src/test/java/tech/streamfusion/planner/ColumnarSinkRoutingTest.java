package tech.streamfusion.planner;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalExchange;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.streaming.api.transformations.PartitionTransformation;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.config.ExecutionConfigOptions.SinkKeyedShuffle;
import org.junit.jupiter.api.Test;
import tech.streamfusion.operator.ArrowBatch;
import tech.streamfusion.operator.ArrowBatchTypeInformation;

class ColumnarSinkRoutingTest {
  private static final int[] KEY = {0};

  @Test
  void shuffleDecisionMatchesFlinksSinkKeyBy() {
    assertFalse(ColumnarSinkRouting.shuffles(SinkKeyedShuffle.NONE, KEY, 3, 2, false));
    assertFalse(ColumnarSinkRouting.shuffles(SinkKeyedShuffle.AUTO, KEY, 2, 2, false));
    assertTrue(ColumnarSinkRouting.shuffles(SinkKeyedShuffle.AUTO, KEY, 3, 2, false));
    assertFalse(ColumnarSinkRouting.shuffles(SinkKeyedShuffle.AUTO, KEY, 1, 2, false));
    assertTrue(ColumnarSinkRouting.shuffles(SinkKeyedShuffle.FORCE, KEY, 2, 2, false));
    assertFalse(ColumnarSinkRouting.shuffles(SinkKeyedShuffle.FORCE, KEY, 1, 2, false));
    assertFalse(ColumnarSinkRouting.shuffles(SinkKeyedShuffle.FORCE, new int[0], 3, 2, false));
    assertTrue(ColumnarSinkRouting.shuffles(SinkKeyedShuffle.NONE, KEY, 1, 1, true));
  }

  @Test
  void routingUsesTheProgramKeyGroupCountAtTheWriterParallelism() {
    var env = StreamExecutionEnvironment.getExecutionEnvironment();
    var partition =
        (PartitionTransformation<ArrowBatch>)
            ColumnarSinkRouting.route(input(env), env, KEY, new int[] {-1}, 256, "test");
    assertEquals(512, ((ColumnarKeyGroupPartitioner) partition.getPartitioner()).maxParallelism());
    assertEquals(256, partition.getParallelism());
    assertEquals(512, partition.getMaxParallelism());
    var split = (OneInputTransformation<?, ?>) partition.getInputs().get(0);
    assertEquals("test-primary-key-split", split.getName());
    assertEquals(2, split.getParallelism());
    env.getConfig().setMaxParallelism(1024);
    partition =
        (PartitionTransformation<ArrowBatch>)
            ColumnarSinkRouting.route(input(env), env, KEY, new int[] {-1}, 256, "test");
    assertEquals(1024, ((ColumnarKeyGroupPartitioner) partition.getPartitioner()).maxParallelism());
  }

  @Test
  void unalignedCheckpointsReassembleRecoveredKeyGroupsBeforeTheWriter() {
    var env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.enableCheckpointing(100);
    env.getCheckpointConfig().enableUnalignedCheckpoints();
    var reassembled =
        (OneInputTransformation<ArrowBatch, ArrowBatch>)
            ColumnarSinkRouting.route(input(env), env, KEY, new int[] {-1}, 3, "test");
    assertEquals("test-primary-key-reassemble", reassembled.getName());
    var partition = (PartitionTransformation<ArrowBatch>) reassembled.getInputs().get(0);
    assertTrue(((ColumnarKeyGroupPartitioner) partition.getPartitioner()).recoverable());
    assertEquals(3, reassembled.getParallelism());
    assertEquals(128, reassembled.getMaxParallelism());
  }

  @Test
  void onlyAHashExchangeLeavesFlinksSinkInputAtTheDefaultParallelism() {
    var cluster =
        RelOptCluster.create(
            new HepPlanner(HepProgram.builder().build()),
            new RexBuilder(new JavaTypeFactoryImpl()));
    RelDataType row = cluster.getTypeFactory().builder().add("k", SqlTypeName.INTEGER).build();
    RexNode always = cluster.getRexBuilder().makeLiteral(true);
    RelNode source = LogicalValues.createEmpty(cluster, row);
    RelNode hash = LogicalExchange.create(source, RelDistributions.hash(List.of(0)));
    RelNode singleton = LogicalExchange.create(hash, RelDistributions.SINGLETON);

    assertFalse(ColumnarSinkRouting.inheritsDefaultParallelism(source));
    assertFalse(
        ColumnarSinkRouting.inheritsDefaultParallelism(LogicalFilter.create(source, always)));
    assertTrue(ColumnarSinkRouting.inheritsDefaultParallelism(hash));
    assertTrue(ColumnarSinkRouting.inheritsDefaultParallelism(LogicalFilter.create(hash, always)));
    assertFalse(ColumnarSinkRouting.inheritsDefaultParallelism(singleton));
    assertFalse(
        ColumnarSinkRouting.inheritsDefaultParallelism(LogicalFilter.create(singleton, always)));
  }

  @Test
  void flinkComparesTheWriterWithTheUnresolvedInputParallelism() {
    var env = StreamExecutionEnvironment.getExecutionEnvironment();
    var input =
        env.fromCollection(List.<ArrowBatch>of(), ArrowBatchTypeInformation.INSTANCE)
            .getTransformation();
    input.setParallelism(2);

    assertEquals(2, ColumnarSinkRouting.flinkInputParallelism(input, false));
    assertEquals(-1, ColumnarSinkRouting.flinkInputParallelism(input, true));
  }

  @Test
  void changelogsWithoutAPrimaryKeyMustKeepTheInputParallelism() {
    int[] none = new int[0];

    assertDoesNotThrow(
        () ->
            ColumnarSinkRouting.requirePrimaryKeyForRedistributedChangelog("t", true, none, 3, -1));
    assertDoesNotThrow(
        () ->
            ColumnarSinkRouting.requirePrimaryKeyForRedistributedChangelog("t", false, KEY, 3, -1));
    assertDoesNotThrow(
        () ->
            ColumnarSinkRouting.requirePrimaryKeyForRedistributedChangelog(
                "t", false, none, -1, -1));
    var failure =
        assertThrows(
            TableException.class,
            () ->
                ColumnarSinkRouting.requirePrimaryKeyForRedistributedChangelog(
                    "db.t", false, none, 2, -1));
    assertEquals(
        "The sink for table 'db.t' has a configured parallelism of 2, while the input parallelism"
            + " is -1. Since the configured parallelism is different from the input's parallelism"
            + " and the changelog mode is not insert-only, a primary key is required but could not"
            + " be found.",
        failure.getMessage());
  }

  private static Transformation<ArrowBatch> input(StreamExecutionEnvironment env) {
    var input =
        env.fromCollection(List.<ArrowBatch>of(), ArrowBatchTypeInformation.INSTANCE)
            .getTransformation();
    input.setParallelism(2);
    return input;
  }
}
