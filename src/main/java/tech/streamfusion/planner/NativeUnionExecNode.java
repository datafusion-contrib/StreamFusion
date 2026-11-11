package tech.streamfusion.planner;

import java.util.List;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecNode;
import org.apache.flink.table.types.logical.RowType;
import tech.streamfusion.operator.ArrowBatch;
import tech.streamfusion.planner.common.CommonUnionExecNode;

/** Streaming mode marker for the shared union implementation. */
public class NativeUnionExecNode extends CommonUnionExecNode implements StreamExecNode<ArrowBatch> {
  public NativeUnionExecNode(
      ReadableConfig config, List<InputProperty> inputs, RowType output, String description) {
    super(config, inputs, output, description);
  }

  NativeUnionExecNode(
      ReadableConfig config,
      List<InputProperty> inputs,
      RowType output,
      String description,
      boolean normalizeBatch) {
    super(config, inputs, output, description, normalizeBatch);
  }
}
