package tech.streamfusion.planner;

import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecNode;
import org.apache.flink.table.types.logical.RowType;
import tech.streamfusion.operator.ArrowBatch;
import tech.streamfusion.planner.common.CommonShareExecNode;

/** Streaming mode marker for the shared stateless implementation. */
public class NativeShareExecNode extends CommonShareExecNode implements StreamExecNode<ArrowBatch> {
  public NativeShareExecNode(
      ReadableConfig tableConfig,
      InputProperty inputProperty,
      RowType outputType,
      String description,
      int consumers) {
    super(tableConfig, inputProperty, outputType, description, consumers);
  }
}
