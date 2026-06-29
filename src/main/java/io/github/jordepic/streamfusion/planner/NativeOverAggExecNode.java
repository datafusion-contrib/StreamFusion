package io.github.jordepic.streamfusion.planner;

import io.github.jordepic.streamfusion.operator.ArrowBatch;
import io.github.jordepic.streamfusion.operator.ArrowBatchTypeInformation;
import io.github.jordepic.streamfusion.operator.NativeOverAggregateOperator;
import java.util.Collections;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.SingleTransformationTranslator;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.utils.ExecNodeUtil;
import org.apache.flink.table.types.logical.RowType;

/** Wraps the native columnar OVER aggregate operator into the plan; Arrow batches in and out. */
public class NativeOverAggExecNode extends ExecNodeBase<ArrowBatch>
    implements StreamExecNode<ArrowBatch>, SingleTransformationTranslator<ArrowBatch> {

  private static final String TRANSFORMATION = "native-over-aggregate";

  private final int timeColumn;
  private final int[] valueColumns;
  private final int[] keyColumns;
  private final int[] valueTypes;
  private final int[] aggregateKinds;
  private final int frameKind;
  private final long frameOffset;
  private final boolean proctime;

  public NativeOverAggExecNode(
      ReadableConfig tableConfig,
      InputProperty inputProperty,
      RowType outputType,
      String description,
      int timeColumn,
      int[] valueColumns,
      int[] keyColumns,
      int[] valueTypes,
      int[] aggregateKinds,
      int frameKind,
      long frameOffset,
      boolean proctime) {
    super(
        ExecNodeContext.newNodeId(),
        new ExecNodeContext("stream-exec-native-over-aggregate_1"),
        tableConfig,
        Collections.singletonList(inputProperty),
        outputType,
        description);
    this.timeColumn = timeColumn;
    this.valueColumns = valueColumns;
    this.keyColumns = keyColumns;
    this.valueTypes = valueTypes;
    this.aggregateKinds = aggregateKinds;
    this.frameKind = frameKind;
    this.frameOffset = frameOffset;
    this.proctime = proctime;
  }

  @Override
  @SuppressWarnings("unchecked")
  protected Transformation<ArrowBatch> translateToPlanInternal(
      PlannerBase planner, ExecNodeConfig config) {
    Transformation<ArrowBatch> input =
        (Transformation<ArrowBatch>) getInputEdges().get(0).translateToPlan(planner);
    return ExecNodeUtil.createOneInputTransformation(
        input,
        createTransformationMeta(TRANSFORMATION, config),
        new NativeOverAggregateOperator(
            timeColumn, valueColumns, keyColumns, valueTypes, aggregateKinds, frameKind, frameOffset,
            proctime),
        ArrowBatchTypeInformation.INSTANCE,
        input.getParallelism(),
        false);
  }
}
