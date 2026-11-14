package tech.streamfusion.planner;

import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecNode;
import org.apache.flink.table.types.logical.RowType;
import tech.streamfusion.planner.common.CommonArrowToRowDataExecNode;

/** Streaming mode marker for the shared Arrow-to-row implementation. */
public class ArrowToRowDataExecNode extends CommonArrowToRowDataExecNode
    implements StreamExecNode<RowData> {
  public ArrowToRowDataExecNode(
      ReadableConfig config, InputProperty input, RowType output, String description) {
    super(config, input, output, description);
  }

  public ArrowToRowDataExecNode(
      ReadableConfig config,
      InputProperty input,
      RowType output,
      String description,
      boolean cachedBinaryRows) {
    super(config, input, output, description, cachedBinaryRows);
  }
}
