package tech.streamfusion.planner;

import java.util.Collections;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecNode;
import org.apache.flink.table.types.logical.RowType;
import tech.streamfusion.iceberg.IcebergSourcePlan;
import tech.streamfusion.iceberg.NativeIcebergSource;
import tech.streamfusion.operator.ArrowBatch;

final class NativeIcebergSourceExecNode extends ExecNodeBase<ArrowBatch>
    implements StreamExecNode<ArrowBatch> {
  private final IcebergSourcePlan plan;

  NativeIcebergSourceExecNode(
      ReadableConfig config, RowType output, String description, IcebergSourcePlan plan) {
    super(
        ExecNodeContext.newNodeId(),
        new ExecNodeContext("stream-exec-native-iceberg-source_1"),
        config,
        Collections.emptyList(),
        output,
        description);
    this.plan = plan;
  }

  @Override
  protected Transformation<ArrowBatch> translateToPlanInternal(
      PlannerBase planner, ExecNodeConfig config) {
    return NativeIcebergSource.read(
            planner.getExecEnv(),
            plan.table,
            plan.loader,
            plan.configuration,
            plan.options,
            plan.projection,
            plan.filters,
            4096)
        .getTransformation();
  }
}
