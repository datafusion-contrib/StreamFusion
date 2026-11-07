package tech.streamfusion.planner;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.StreamMap;
import org.apache.flink.streaming.api.transformations.PartitionTransformation;
import org.apache.flink.streaming.api.transformations.TwoInputTransformation;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.*;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import tech.streamfusion.operator.*;

class CanonicalTemporalJoinBoundaryTest {
  @ParameterizedTest
  @CsvSource({"17,false", "256,false"})
  @SuppressWarnings({"unchecked", "rawtypes"})
  void restoredMaximumControlsBothExchangesAndTheNativeStateOwner(int maximum, boolean keyless)
      throws Exception {
    var environment = StreamExecutionEnvironment.getExecutionEnvironment();
    environment.setParallelism(2);
    environment.setMaxParallelism(257);
    var table = tech.streamfusion.NativePlannerTestEnvironment.create(environment);
    var planner =
        (PlannerBase)
            ((org.apache.flink.table.api.internal.TableEnvironmentImpl) table).getPlanner();
    var config = new Configuration();
    var nodeConfig = ExecNodeConfig.ofNodeConfig(config, false);
    var type = RowType.of(new BigIntType(), new BigIntType(), new BigIntType());
    var input =
        environment
            .fromData(Types.INT, 1)
            .transform(
                "Arrow input",
                ArrowBatchTypeInformation.INSTANCE,
                new StreamMap<Integer, ArrowBatch>(ignored -> null))
            .setParallelism(2)
            .getTransformation();
    var source = new FixedInput(type, input);
    int[] keys = keyless ? new int[0] : new int[] {0};
    int[] descriptor = keyless ? new int[0] : new int[] {-1};
    var left =
        new NativeColumnarExchangeExecNode(
                config, InputProperty.DEFAULT, type, "left", keys, descriptor, keyless)
            .withStateMaxParallelism(maximum);
    left.setInputEdges(List.of(ExecEdge.builder().source(source).target(left).build()));
    var right =
        new NativeColumnarExchangeExecNode(
                config, InputProperty.DEFAULT, type, "right", keys, descriptor, keyless)
            .withStateMaxParallelism(maximum);
    right.setInputEdges(List.of(ExecEdge.builder().source(source).target(right).build()));
    var leftTransform = left.translateToPlanInternal(planner, nodeConfig);
    var rightTransform = right.translateToPlanInternal(planner, nodeConfig);
    for (var exchange : List.of(leftTransform, rightTransform)) {
      assertEquals(maximum, exchange.getMaxParallelism());
      assertEquals(keyless ? 1 : 2, exchange.getParallelism());
      var partition = (PartitionTransformation<ArrowBatch>) exchange;
      assertEquals(
          maximum, ((ColumnarKeyGroupPartitioner) partition.getPartitioner()).maxParallelism());
    }
    var leftNode = new FixedInput(type, leftTransform);
    var rightNode = new FixedInput(type, rightTransform);
    var output =
        RowType.of(
            new BigIntType(),
            new BigIntType(),
            new BigIntType(),
            new BigIntType(),
            new BigIntType(),
            new BigIntType());
    var join =
        new NativeTemporalJoinExecNode(
                config,
                InputProperty.DEFAULT,
                InputProperty.DEFAULT,
                output,
                "join",
                keys,
                keys,
                2,
                2,
                0,
                type,
                type,
                null,
                descriptor)
            .withStateMaxParallelism(maximum);
    join.setInputEdges(
        List.of(
            ExecEdge.builder().source(leftNode).target(join).build(),
            ExecEdge.builder().source(rightNode).target(join).build()));
    var transformation =
        (TwoInputTransformation<ArrowBatch, ArrowBatch, ArrowBatch>)
            join.translateToPlanInternal(planner, nodeConfig);
    assertEquals(maximum, transformation.getMaxParallelism());
    assertEquals(keyless ? 1 : 2, transformation.getParallelism());
    assertNotNull(transformation.getStateKeySelector1());
    assertNotNull(transformation.getStateKeySelector2());
    assertEquals(Types.INT, transformation.getStateKeyType());
  }

  private static final class FixedInput extends ExecNodeBase<ArrowBatch>
      implements org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecNode<ArrowBatch> {
    private final Transformation<ArrowBatch> transformation;

    FixedInput(RowType type, Transformation<ArrowBatch> transformation) {
      super(
          ExecNodeContext.newNodeId(),
          new ExecNodeContext("stream-exec-native-test-input_1"),
          new Configuration(),
          List.of(),
          type,
          "Fixed Arrow input");
      this.transformation = transformation;
    }

    @Override
    protected Transformation<ArrowBatch> translateToPlanInternal(
        PlannerBase planner, ExecNodeConfig config) {
      return transformation;
    }
  }
}
