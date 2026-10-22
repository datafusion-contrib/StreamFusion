package tech.streamfusion.planner;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.streaming.api.transformations.PartitionTransformation;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.Test;
import tech.streamfusion.operator.ArrowBatch;
import tech.streamfusion.operator.ArrowBatchTypeInformation;

class DorisSinkRuntimeTest {
  private static final RowType TYPE = RowType.of(new BigIntType());

  @Test
  void primaryKeyShufflePreservesTheSqlPolicy() {
    var env = StreamExecutionEnvironment.getExecutionEnvironment();
    var input = input(env);
    assertSame(input, prepare(input, env, 3, ExecutionConfigOptions.SinkKeyedShuffle.NONE));
    assertSame(input, prepare(input, env, 2, ExecutionConfigOptions.SinkKeyedShuffle.AUTO));
    assertInstanceOf(
        PartitionTransformation.class,
        prepare(input, env, 3, ExecutionConfigOptions.SinkKeyedShuffle.AUTO));
    assertInstanceOf(
        PartitionTransformation.class,
        prepare(input, env, 2, ExecutionConfigOptions.SinkKeyedShuffle.FORCE));
    assertSame(input, prepare(input, env, 1, ExecutionConfigOptions.SinkKeyedShuffle.FORCE));
  }

  @Test
  void largeAndExplicitParallelismKeepMatchingKeyGroupCounts() {
    var env = StreamExecutionEnvironment.getExecutionEnvironment();
    var input = input(env);
    var partition =
        (PartitionTransformation<ArrowBatch>)
            prepare(input, env, 256, ExecutionConfigOptions.SinkKeyedShuffle.AUTO);
    assertEquals(512, ((ColumnarKeyGroupPartitioner) partition.getPartitioner()).maxParallelism());
    assertEquals(256, partition.getParallelism());
    assertEquals(512, partition.getMaxParallelism());
    env.getConfig().setMaxParallelism(1024);
    partition =
        (PartitionTransformation<ArrowBatch>)
            prepare(input, env, 256, ExecutionConfigOptions.SinkKeyedShuffle.AUTO);
    assertEquals(1024, ((ColumnarKeyGroupPartitioner) partition.getPartitioner()).maxParallelism());
  }

  @Test
  void unalignedCheckpointsReassembleRecoverableKeyGroupsBeforeTheWriter() {
    var env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.enableCheckpointing(100);
    env.getCheckpointConfig().enableUnalignedCheckpoints();
    var reassembled =
        (OneInputTransformation<ArrowBatch, ArrowBatch>)
            prepare(input(env), env, 3, ExecutionConfigOptions.SinkKeyedShuffle.AUTO);
    var partition = (PartitionTransformation<ArrowBatch>) reassembled.getInputs().get(0);
    assertTrue(((ColumnarKeyGroupPartitioner) partition.getPartitioner()).recoverable());
    assertEquals(3, reassembled.getParallelism());
    assertEquals(128, reassembled.getMaxParallelism());
  }

  @Test
  void materializationAlwaysRoutesBySinkKeyEvenWithShuffleDisabledAndOneWriter() {
    var env = StreamExecutionEnvironment.getExecutionEnvironment();
    var config =
        new Configuration()
            .set(
                ExecutionConfigOptions.TABLE_EXEC_SINK_KEYED_SHUFFLE,
                ExecutionConfigOptions.SinkKeyedShuffle.NONE);
    for (int target : new int[] {1, 3}) {
      var result =
          (OneInputTransformation<ArrowBatch, ArrowBatch>)
              new DorisSinkRuntime(
                      null,
                      TYPE,
                      target,
                      new int[] {0},
                      new int[] {-1},
                      new String[1],
                      true,
                      new int[] {0})
                  .prepare(input(env), env, ExecNodeConfig.ofNodeConfig(config, false));
      assertEquals("native-sink-upsert-materialize", result.getName());
      assertEquals(target, result.getParallelism());
      assertEquals(128, result.getMaxParallelism());
      assertNotNull(result.getStateKeySelector());
      assertInstanceOf(PartitionTransformation.class, result.getInputs().get(0));
    }
  }

  @Test
  void adaptiveThresholdsRejectTheSameInvalidConfigurationsAsFlink() throws Exception {
    tech.streamfusion.operator.TaskOffHeapMemory.initialize(
        new Configuration()
            .set(
                org.apache.flink.configuration.TaskManagerOptions.TASK_OFF_HEAP_MEMORY,
                org.apache.flink.configuration.MemorySize.ofMebiBytes(256)));
    var env = StreamExecutionEnvironment.getExecutionEnvironment();
    for (Long[] thresholds : List.of(new Long[] {1L, 2L}, new Long[] {100L, null})) {
      var config =
          new Configuration()
              .set(
                  ExecutionConfigOptions.TABLE_EXEC_SINK_UPSERT_MATERIALIZE_STRATEGY,
                  ExecutionConfigOptions.SinkUpsertMaterializeStrategy.ADAPTIVE);
      config.set(
          ExecutionConfigOptions.TABLE_EXEC_SINK_UPSERT_MATERIALIZE_ADAPTIVE_THRESHOLD_HIGH,
          thresholds[0]);
      if (thresholds[1] != null)
        config.set(
            ExecutionConfigOptions.TABLE_EXEC_SINK_UPSERT_MATERIALIZE_ADAPTIVE_THRESHOLD_LOW,
            thresholds[1]);
      var transform =
          (OneInputTransformation<ArrowBatch, ArrowBatch>)
              new DorisSinkRuntime(
                      null,
                      TYPE,
                      1,
                      new int[] {0},
                      new int[] {-1},
                      new String[1],
                      true,
                      new int[] {0})
                  .prepare(input(env), env, ExecNodeConfig.ofNodeConfig(config, false));
      var factory =
          (org.apache.flink.streaming.api.operators.SimpleOperatorFactory<ArrowBatch>)
              transform.getOperatorFactory();
      var operator = (tech.streamfusion.operator.NativeSinkMaterializer) factory.getOperator();
      try (var harness =
          new org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness<
              Integer, ArrowBatch, ArrowBatch>(
              operator, b -> 0, org.apache.flink.api.common.typeinfo.Types.INT, 128, 1, 0)) {
        harness.setup(new tech.streamfusion.operator.ArrowBatchSerializer());
        assertThrows(IllegalArgumentException.class, harness::open);
      }
    }
  }

  private static Transformation<ArrowBatch> input(StreamExecutionEnvironment env) {
    var input =
        env.fromCollection(List.<ArrowBatch>of(), ArrowBatchTypeInformation.INSTANCE)
            .getTransformation();
    input.setParallelism(2);
    return input;
  }

  private static Transformation<ArrowBatch> prepare(
      Transformation<ArrowBatch> input,
      StreamExecutionEnvironment env,
      int target,
      ExecutionConfigOptions.SinkKeyedShuffle policy) {
    var config =
        new Configuration().set(ExecutionConfigOptions.TABLE_EXEC_SINK_KEYED_SHUFFLE, policy);
    return new DorisSinkRuntime(null, TYPE, target, new int[] {0}, new int[] {-1}, new String[1])
        .prepare(input, env, ExecNodeConfig.ofNodeConfig(config, false));
  }
}
