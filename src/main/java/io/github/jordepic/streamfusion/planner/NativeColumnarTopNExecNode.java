package io.github.jordepic.streamfusion.planner;

import io.github.jordepic.streamfusion.operator.ArrowBatch;
import io.github.jordepic.streamfusion.operator.ArrowBatchTypeInformation;
import io.github.jordepic.streamfusion.operator.NativeColumnarTopNOperator;
import java.util.Collections;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.utils.ExecNodeUtil;
import org.apache.flink.table.types.logical.RowType;

/** Wraps the columnar append-only Top-N operator into the plan; Arrow batches in and out. */
public class NativeColumnarTopNExecNode extends ExecNodeBase<ArrowBatch>
    implements StreamExecNode<ArrowBatch> {

  private static final String TRANSFORMATION = "native-columnar-top-n";

  private final int[] partitionColumns;
  private final int[] sortIndices;
  private final int[] sortAscending;
  private final int[] sortNullsFirst;
  private final long limit;
  private final boolean outputRankNumber;

  public NativeColumnarTopNExecNode(
      ReadableConfig tableConfig,
      InputProperty inputProperty,
      RowType outputType,
      String description,
      int[] partitionColumns,
      int[] sortIndices,
      int[] sortAscending,
      int[] sortNullsFirst,
      long limit,
      boolean outputRankNumber) {
    super(
        ExecNodeContext.newNodeId(),
        new ExecNodeContext("stream-exec-native-columnar-top-n_1"),
        tableConfig,
        Collections.singletonList(inputProperty),
        outputType,
        description);
    this.partitionColumns = partitionColumns;
    this.sortIndices = sortIndices;
    this.sortAscending = sortAscending;
    this.sortNullsFirst = sortNullsFirst;
    this.limit = limit;
    this.outputRankNumber = outputRankNumber;
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
        new NativeColumnarTopNOperator(
            partitionColumns, sortIndices, sortAscending, sortNullsFirst, limit, outputRankNumber),
        ArrowBatchTypeInformation.INSTANCE,
        input.getParallelism(),
        false);
  }
}
