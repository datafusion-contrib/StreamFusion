package io.github.jordepic.streamfusion.planner;

import io.github.jordepic.streamfusion.operator.ArrowBatch;
import io.github.jordepic.streamfusion.operator.ArrowBatchTypeInformation;
import io.github.jordepic.streamfusion.operator.NativeLookupJoinOperator;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.calcite.plan.RelOptTable;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.functions.LookupFunction;
import org.apache.flink.table.functions.UserDefinedFunction;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.SingleTransformationTranslator;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.utils.ExecNodeUtil;
import org.apache.flink.table.planner.plan.utils.LookupJoinUtil;
import org.apache.flink.table.runtime.operators.join.lookup.ResultRetryStrategy;
import org.apache.flink.table.types.logical.RowType;

/**
 * Wraps the native lookup-join operator into the plan. Builds the connector's synchronous {@link
 * LookupFunction} the same way Flink's own lookup join does ({@link LookupJoinUtil#getLookupFunction})
 * and hands it to the operator, which calls it per probe row while the batches stay Arrow.
 */
public class NativeLookupJoinExecNode extends ExecNodeBase<ArrowBatch>
    implements StreamExecNode<ArrowBatch>, SingleTransformationTranslator<ArrowBatch> {

  private static final String TRANSFORMATION = "native-lookup-join";

  private final RelOptTable temporalTable;
  private final RowType probeType;
  private final RowType dimType;
  private final int[] orderedDimKeys;
  private final int[] probeKeyIndices;
  private final int joinType;

  public NativeLookupJoinExecNode(
      ReadableConfig tableConfig,
      InputProperty inputProperty,
      RowType outputType,
      String description,
      RelOptTable temporalTable,
      RowType probeType,
      RowType dimType,
      int[] orderedDimKeys,
      int[] probeKeyIndices,
      int joinType) {
    super(
        ExecNodeContext.newNodeId(),
        new ExecNodeContext("stream-exec-native-lookup-join_1"),
        tableConfig,
        Collections.singletonList(inputProperty),
        outputType,
        description);
    this.temporalTable = temporalTable;
    this.probeType = probeType;
    this.dimType = dimType;
    this.orderedDimKeys = orderedDimKeys;
    this.probeKeyIndices = probeKeyIndices;
    this.joinType = joinType;
  }

  @Override
  @SuppressWarnings("unchecked")
  protected Transformation<ArrowBatch> translateToPlanInternal(
      PlannerBase planner, ExecNodeConfig config) {
    Transformation<ArrowBatch> input =
        (Transformation<ArrowBatch>) getInputEdges().get(0).translateToPlan(planner);

    ClassLoader classLoader = planner.getFlinkContext().getClassLoader();
    List<Integer> keys = new ArrayList<>(orderedDimKeys.length);
    for (int key : orderedDimKeys) {
      keys.add(key);
    }
    UserDefinedFunction function =
        LookupJoinUtil.getLookupFunction(
            temporalTable,
            keys,
            classLoader,
            false,
            ResultRetryStrategy.NO_RETRY_STRATEGY,
            false);
    if (!(function instanceof LookupFunction)) {
      throw new IllegalStateException(
          "expected a synchronous LookupFunction but got " + function.getClass().getName());
    }

    return ExecNodeUtil.createOneInputTransformation(
        input,
        createTransformationMeta(TRANSFORMATION, config),
        new NativeLookupJoinOperator(
            (LookupFunction) function,
            probeType,
            dimType,
            (RowType) getOutputType(),
            probeKeyIndices,
            joinType),
        ArrowBatchTypeInformation.INSTANCE,
        input.getParallelism(),
        false);
  }
}
