package tech.streamfusion.planner.common;

import java.util.Collections;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.SingleTransformationTranslator;
import org.apache.flink.table.planner.plan.nodes.exec.utils.ExecNodeUtil;
import org.apache.flink.table.types.logical.RowType;
import tech.streamfusion.operator.ArrowBatch;
import tech.streamfusion.operator.ArrowBatchTypeInformation;
import tech.streamfusion.operator.NativeColumnarShareOperator;

/**
 * Exec node for the native share point: one chained pass-through operator that stamps the branch
 * count on each batch so the chained broadcast hands every branch its own retained view. Translated
 * once — the deduplicated sub-plan below it — no matter how many branches consume it.
 */
public class CommonShareExecNode extends ExecNodeBase<ArrowBatch>
    implements SingleTransformationTranslator<ArrowBatch> {

  private static final String TRANSFORMATION = "native-share";

  private final int consumers;

  public CommonShareExecNode(
      ReadableConfig tableConfig,
      InputProperty inputProperty,
      RowType outputType,
      String description,
      int consumers) {
    super(
        ExecNodeContext.newNodeId(),
        new ExecNodeContext("stream-exec-native-share_1"),
        tableConfig,
        Collections.singletonList(inputProperty),
        outputType,
        description);
    this.consumers = consumers;
  }

  @Override
  protected String getSimplifiedName() {
    return "NativeShareExecNode";
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
        new NativeColumnarShareOperator(consumers),
        ArrowBatchTypeInformation.INSTANCE,
        input.getParallelism(),
        false);
  }
}
