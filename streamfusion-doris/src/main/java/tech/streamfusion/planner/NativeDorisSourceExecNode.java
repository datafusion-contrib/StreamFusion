package tech.streamfusion.planner;

import java.util.List;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory$;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.*;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecNode;
import tech.streamfusion.doris.DorisSourceRuntime;
import tech.streamfusion.operator.ArrowBatch;

public final class NativeDorisSourceExecNode extends ExecNodeBase<ArrowBatch>
    implements StreamExecNode<ArrowBatch> {
  private final DorisSourceRuntime source;

  NativeDorisSourceExecNode(
      ReadableConfig config, RelDataType type, String description, DorisSourceRuntime source) {
    super(
        ExecNodeContext.newNodeId(),
        new ExecNodeContext("stream-exec-native-doris-source_1"),
        config,
        List.of(),
        FlinkTypeFactory$.MODULE$.toLogicalRowType(type),
        description);
    this.source = source;
  }

  @Override
  protected Transformation<ArrowBatch> translateToPlanInternal(
      PlannerBase planner, ExecNodeConfig config) {
    return source.addTo(planner.getExecEnv());
  }
}
