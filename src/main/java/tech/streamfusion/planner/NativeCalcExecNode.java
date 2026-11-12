package tech.streamfusion.planner;

import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecNode;
import org.apache.flink.table.types.logical.RowType;
import tech.streamfusion.operator.ArrowBatch;
import tech.streamfusion.operator.NativeUdf;
import tech.streamfusion.planner.common.CommonCalcExecNode;

/** Streaming mode marker for the shared stateless implementation. */
public class NativeCalcExecNode extends CommonCalcExecNode implements StreamExecNode<ArrowBatch> {
  public NativeCalcExecNode(
      ReadableConfig tableConfig,
      InputProperty inputProperty,
      RowType outputType,
      String description,
      int[] kinds,
      int[] payload,
      int[] childCounts,
      long[] longs,
      double[] doubles,
      String[] strings,
      int[] projectionRoots,
      int conditionRoot,
      String[] outputNames,
      NativeUdf.Binding udfBinding) {
    super(
        tableConfig,
        inputProperty,
        outputType,
        description,
        kinds,
        payload,
        childCounts,
        longs,
        doubles,
        strings,
        projectionRoots,
        conditionRoot,
        outputNames,
        udfBinding);
  }
}
