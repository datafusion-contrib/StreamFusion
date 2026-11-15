package tech.streamfusion.planner.stream;

import java.util.Collections;
import java.util.Optional;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.planner.codegen.CodeGeneratorContext;
import org.apache.flink.table.planner.codegen.CorrelateCodeGenerator$;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.SingleTransformationTranslator;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.utils.ExecNodeUtil;
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil;
import org.apache.flink.table.runtime.operators.TableStreamOperator;
import org.apache.flink.table.runtime.operators.join.FlinkJoinType;
import org.apache.flink.table.types.logical.RowType;
import tech.streamfusion.operator.ArrowBatch;
import tech.streamfusion.operator.ArrowBatchTypeInformation;
import tech.streamfusion.operator.NativeColumnarTableFunctionOperator;
import tech.streamfusion.planner.NativeConfig;

/** Uses Flink's own correlate generator for conversion, predicates, outer rows and lifecycle. */
public final class NativeGeneratedUnnestExecNode extends ExecNodeBase<ArrowBatch>
    implements StreamExecNode<ArrowBatch>, SingleTransformationTranslator<ArrowBatch> {
  private final RowType inputType;
  private final RowType evaluationInputType;
  private final int[] inputMapping;
  private final RexCall invocation;
  private final RexNode condition;
  private final boolean left;
  private final RowType correlatedType;
  private final int[] projection;

  NativeGeneratedUnnestExecNode(
      ReadableConfig config,
      RowType inputType,
      RowType evaluationInputType,
      int[] inputMapping,
      RowType outputType,
      RowType correlatedType,
      int[] projection,
      RexCall invocation,
      RexNode condition,
      boolean left,
      String description) {
    super(
        ExecNodeContext.newNodeId(),
        new ExecNodeContext("stream-exec-native-generated-unnest_1"),
        config,
        Collections.singletonList(InputProperty.DEFAULT),
        outputType,
        description);
    this.inputType = inputType;
    this.evaluationInputType = evaluationInputType;
    this.inputMapping = inputMapping;
    this.correlatedType = correlatedType;
    this.projection = projection;
    this.invocation = invocation;
    this.condition = condition;
    this.left = left;
  }

  @Override
  @SuppressWarnings("unchecked")
  protected Transformation<ArrowBatch> translateToPlanInternal(
      PlannerBase planner, ExecNodeConfig config) {
    var input = (Transformation<ArrowBatch>) getInputEdges().get(0).translateToPlan(planner);
    var context =
        new CodeGeneratorContext(config, planner.getFlinkContext().getClassLoader())
            .setOperatorBaseClass(TableStreamOperator.class);
    RexNode shifted =
        condition == null ? null : RexUtil.shift(condition, evaluationInputType.getFieldCount());
    var generated =
        CorrelateCodeGenerator$.MODULE$.generateOperator(
            context,
            config,
            evaluationInputType,
            JavaScalaConversionUtil.toScala(Optional.ofNullable(shifted)),
            correlatedType,
            left ? FlinkJoinType.LEFT : FlinkJoinType.INNER,
            invocation,
            "ColumnarGeneratedUnnest",
            true);
    return ExecNodeUtil.createOneInputTransformation(
        input,
        createTransformationMeta("native-generated-unnest", config),
        new NativeColumnarTableFunctionOperator(
            inputType,
            (RowType) getOutputType(),
            generated.getGeneratedClass(),
            projection,
            NativeConfig.transposeBatchRows(config),
            true,
            inputMapping),
        ArrowBatchTypeInformation.INSTANCE,
        input.getParallelism(),
        false);
  }
}
