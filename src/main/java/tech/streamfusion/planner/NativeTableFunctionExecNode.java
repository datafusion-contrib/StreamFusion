package tech.streamfusion.planner;

import java.util.Collections;
import java.util.Optional;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.ReadableConfig;
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

/** Uses Flink's own correlate generator for conversion, predicates, outer rows and lifecycle. */
public final class NativeTableFunctionExecNode extends ExecNodeBase<ArrowBatch>
    implements StreamExecNode<ArrowBatch>, SingleTransformationTranslator<ArrowBatch> {
  private final RowType inputType;
  private final RexCall invocation;
  private final RexNode condition;
  private final boolean left;
  private final boolean preserveCollections;
  private final RowType correlatedType;
  private final int[] projection;
  private final boolean hasProjection;

  NativeTableFunctionExecNode(
      ReadableConfig config,
      RowType inputType,
      RowType outputType,
      RowType correlatedType,
      int[] projection,
      boolean hasProjection,
      RexCall invocation,
      RexNode condition,
      boolean left,
      boolean preserveCollections,
      String description) {
    super(
        ExecNodeContext.newNodeId(),
        new ExecNodeContext("stream-exec-native-table-function_1"),
        config,
        Collections.singletonList(InputProperty.DEFAULT),
        outputType,
        description);
    this.inputType = inputType;
    this.correlatedType = correlatedType;
    this.projection = projection;
    this.hasProjection = hasProjection;
    this.invocation = invocation;
    this.condition = condition;
    this.left = left;
    this.preserveCollections = preserveCollections;
  }

  @Override
  @SuppressWarnings("unchecked")
  protected Transformation<ArrowBatch> translateToPlanInternal(
      PlannerBase planner, ExecNodeConfig config) {
    var input = (Transformation<ArrowBatch>) getInputEdges().get(0).translateToPlan(planner);
    var context =
        new JvmFunctionCodeGeneratorContext(
                config,
                planner.getFlinkContext().getClassLoader(),
                "getRuntimeContext().getUserCodeClassLoader()")
            .setOperatorBaseClass(TableStreamOperator.class);
    RexNode shifted =
        condition == null ? null : RexUtil.shift(condition, inputType.getFieldCount());
    var generated =
        CorrelateCodeGenerator$.MODULE$.generateOperator(
            context,
            config,
            inputType,
            JavaScalaConversionUtil.toScala(Optional.ofNullable(shifted)),
            correlatedType,
            left ? FlinkJoinType.LEFT : FlinkJoinType.INNER,
            invocation,
            "ColumnarJvmTableFunction",
            true);
    RowType outputType = (RowType) getOutputType();
    var resultProjection =
        hasProjection
                && outputType.getChildren().stream()
                    .anyMatch(type -> !type.isNullable() && type.getChildren().isEmpty())
            ? tech.streamfusion.compat.RuntimeCompat.tableFunctionProjection(
                config,
                planner.getFlinkContext().getClassLoader(),
                correlatedType,
                outputType,
                projection)
            : null;
    return ExecNodeUtil.createOneInputTransformation(
        input,
        createTransformationMeta("native-table-function", config),
        new NativeColumnarTableFunctionOperator(
            inputType,
            (RowType) getOutputType(),
            generated.getGeneratedClass(),
            projection,
            NativeConfig.transposeBatchRows(config),
            preserveCollections,
            null,
            resultProjection),
        ArrowBatchTypeInformation.INSTANCE,
        input.getParallelism(),
        false);
  }
}
