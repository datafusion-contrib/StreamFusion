package tech.streamfusion.planner;

import java.util.Collections;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecNode;
import org.apache.flink.table.types.logical.RowType;
import tech.streamfusion.hudi.NativeHudiSource;
import tech.streamfusion.operator.ArrowBatch;
import tech.streamfusion.operator.ArrowBatchTypeInformation;
import tech.streamfusion.planner.common.NativePlanDetails;

final class NativeHudiSourceExecNode extends ExecNodeBase<ArrowBatch>
    implements StreamExecNode<ArrowBatch> {
  private final NativeHudiSource.Plan sourcePlan;

  NativeHudiSourceExecNode(
      ReadableConfig config, RowType output, String description, NativeHudiSource.Plan sourcePlan) {
    super(
        ExecNodeContext.newNodeId(),
        new ExecNodeContext("stream-exec-native-hudi-source_1"),
        config,
        Collections.emptyList(),
        output,
        description);
    this.sourcePlan = sourcePlan;
  }

  @Override
  protected Transformation<ArrowBatch> translateToPlanInternal(
      PlannerBase planner, ExecNodeConfig config) {
    var env = planner.getExecEnv();
    var source = sourcePlan.create(4096, env.getParallelism());
    var stream =
        env.fromSource(
            source,
            WatermarkStrategy.noWatermarks(),
            NativePlanDetails.name("NativeSource", getDescription()),
            ArrowBatchTypeInformation.INSTANCE);
    stream.setParallelism(source.readTasks());
    stream.getTransformation().setDescription(getDescription());
    return stream.getTransformation();
  }
}
