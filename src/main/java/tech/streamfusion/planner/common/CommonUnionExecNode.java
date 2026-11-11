package tech.streamfusion.planner.common;

import java.util.ArrayList;
import java.util.List;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.transformations.UnionTransformation;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.SingleTransformationTranslator;
import org.apache.flink.table.types.logical.RowType;
import tech.streamfusion.operator.ArrowBatch;

/**
 * Lowers the native union into the plan. Like Flink's own {@code CommonExecUnion} this is not a
 * physical operator — it merges the inputs' transformations into a single {@link
 * UnionTransformation}, which forwards every input record (here, every Arrow batch) downstream
 * unchanged and aligns watermarks across the inputs. Batch inputs normalize their Arrow headers
 * before the union.
 */
public class CommonUnionExecNode extends ExecNodeBase<ArrowBatch>
    implements SingleTransformationTranslator<ArrowBatch> {
  private final List<String> commonBatchNames;

  public CommonUnionExecNode(
      ReadableConfig tableConfig,
      List<InputProperty> inputProperties,
      RowType outputType,
      String description) {
    this(tableConfig, inputProperties, outputType, description, false);
  }

  public CommonUnionExecNode(
      ReadableConfig tableConfig,
      List<InputProperty> inputProperties,
      RowType outputType,
      String description,
      boolean normalizeBatch) {
    super(
        ExecNodeContext.newNodeId(),
        new ExecNodeContext("stream-exec-native-union_1"),
        tableConfig,
        inputProperties,
        outputType,
        description);
    commonBatchNames = normalizeBatch ? outputType.getFieldNames() : null;
  }

  @Override
  protected String getSimplifiedName() {
    return "NativeUnionExecNode";
  }

  @Override
  @SuppressWarnings("unchecked")
  protected Transformation<ArrowBatch> translateToPlanInternal(
      PlannerBase planner, ExecNodeConfig config) {
    List<Transformation<ArrowBatch>> inputTransforms = new ArrayList<>();
    for (ExecEdge inputEdge : getInputEdges()) {
      var input = (Transformation<ArrowBatch>) inputEdge.translateToPlan(planner);
      if (commonBatchNames != null) {
        input =
            new org.apache.flink.streaming.api.transformations.OneInputTransformation<>(
                input,
                "NativeBatchUnionSchema",
                new tech.streamfusion.operator.BatchUnionSchemaOperator(commonBatchNames),
                tech.streamfusion.operator.ArrowBatchTypeInformation.INSTANCE,
                input.getParallelism());
      }
      inputTransforms.add(input);
    }
    return new UnionTransformation<>(inputTransforms);
  }
}
