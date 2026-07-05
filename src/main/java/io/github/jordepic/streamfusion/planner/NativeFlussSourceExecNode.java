package io.github.jordepic.streamfusion.planner;

import io.github.jordepic.streamfusion.fluss.NativeFlussSource;
import io.github.jordepic.streamfusion.operator.ArrowBatch;
import io.github.jordepic.streamfusion.operator.ArrowBatchTypeInformation;
import java.util.Collections;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecNode;
import org.apache.flink.table.types.logical.RowType;

/** Exec node that contributes the native fluss-rs log source transformation. */
public class NativeFlussSourceExecNode extends ExecNodeBase<ArrowBatch>
    implements StreamExecNode<ArrowBatch> {

  private static final String TRANSFORMATION = "native-fluss-source";

  private final NativeFlussSource source;

  public NativeFlussSourceExecNode(
      ReadableConfig tableConfig,
      RowType outputType,
      String description,
      NativeFlussSource source) {
    super(
        ExecNodeContext.newNodeId(),
        new ExecNodeContext("stream-exec-native-fluss-source_1"),
        tableConfig,
        Collections.emptyList(),
        outputType,
        description);
    this.source = source;
  }

  @Override
  protected Transformation<ArrowBatch> translateToPlanInternal(
      PlannerBase planner, ExecNodeConfig config) {
    StreamExecutionEnvironment env = planner.getExecEnv();
    DataStreamSource<ArrowBatch> stream =
        env.fromSource(
            source,
            WatermarkStrategy.noWatermarks(),
            TRANSFORMATION,
            ArrowBatchTypeInformation.INSTANCE);
    return stream.getTransformation();
  }
}
