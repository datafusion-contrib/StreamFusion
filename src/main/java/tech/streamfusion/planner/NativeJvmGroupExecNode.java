package tech.streamfusion.planner;

import java.util.Arrays;
import java.util.Collections;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.planner.codegen.CodeGeneratorContext;
import org.apache.flink.table.planner.codegen.EqualiserCodeGenerator;
import org.apache.flink.table.planner.codegen.agg.AggsHandlerCodeGenerator;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.SingleTransformationTranslator;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.utils.ExecNodeUtil;
import org.apache.flink.table.planner.plan.utils.AggregateUtil;
import org.apache.flink.table.planner.plan.utils.KeySelectorUtil;
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil;
import org.apache.flink.table.runtime.operators.aggregate.GroupAggFunction;
import org.apache.flink.table.runtime.types.LogicalTypeDataTypeConverter;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import scala.collection.Seq;
import tech.streamfusion.operator.ArrowBatch;
import tech.streamfusion.operator.ArrowBatchRowKeySelector;
import tech.streamfusion.operator.ArrowBatchTypeInformation;
import tech.streamfusion.operator.NativeColumnarJvmGroupOperator;

public final class NativeJvmGroupExecNode extends ExecNodeBase<ArrowBatch>
    implements StreamExecNode<ArrowBatch>, SingleTransformationTranslator<ArrowBatch> {
  private final RowType inputType;
  private final int[] grouping;
  private final Seq<AggregateCall> calls;
  private final boolean[] retracts;
  private final boolean retract;
  private final boolean updateBefore;

  NativeJvmGroupExecNode(
      ReadableConfig config,
      RowType inputType,
      RowType outputType,
      int[] grouping,
      Seq<AggregateCall> calls,
      boolean[] retracts,
      boolean retract,
      boolean updateBefore,
      String description) {
    super(
        ExecNodeContext.newNodeId(),
        new ExecNodeContext("stream-exec-native-jvm-group_1"),
        config,
        Collections.singletonList(InputProperty.DEFAULT),
        outputType,
        description);
    this.inputType = inputType;
    this.grouping = grouping;
    this.calls = calls;
    this.retracts = retracts;
    this.retract = retract;
    this.updateBefore = updateBefore;
  }

  @Override
  @SuppressWarnings("unchecked")
  protected Transformation<ArrowBatch> translateToPlanInternal(
      PlannerBase planner, ExecNodeConfig config) {
    var input = (Transformation<ArrowBatch>) getInputEdges().get(0).translateToPlan(planner);
    var generator =
        new AggsHandlerCodeGenerator(
                new CodeGeneratorContext(config, planner.getFlinkContext().getClassLoader()),
                planner.createRelBuilder(),
                JavaScalaConversionUtil.toScala(inputType.getChildren()),
                true)
            .needAccumulate();
    if (retract) generator.needRetract();
    var info =
        AggregateUtil.transformToStreamAggregateInfoList(
            planner.getTypeFactory(), inputType, calls, retracts, retract, true, true);
    var handler = generator.generateAggsHandler("ColumnarJvmAggsHandler", info);
    LogicalType[] accTypes =
        Arrays.stream(info.getAccTypes())
            .map(LogicalTypeDataTypeConverter::fromDataTypeToLogicalType)
            .toArray(LogicalType[]::new);
    LogicalType[] values =
        Arrays.stream(info.getActualValueTypes())
            .map(LogicalTypeDataTypeConverter::fromDataTypeToLogicalType)
            .toArray(LogicalType[]::new);
    var equaliser =
        new EqualiserCodeGenerator(values, planner.getFlinkContext().getClassLoader())
            .generateRecordEqualiser("ColumnarJvmAggEqualiser");
    var function =
        new GroupAggFunction(
            handler, equaliser, accTypes, info.getIndexOfCountStar(), updateBefore, 0);
    var selector =
        KeySelectorUtil.getRowDataSelector(
            planner.getFlinkContext().getClassLoader(), grouping, InternalTypeInfo.of(inputType));
    var keyType = grouping.length == 1 ? inputType.getTypeAt(grouping[0]).getTypeRoot() : null;
    var transform =
        ExecNodeUtil.createOneInputTransformation(
            input,
            createTransformationMeta("native-jvm-group", config),
            new NativeColumnarJvmGroupOperator(
                inputType,
                (RowType) getOutputType(),
                function,
                selector,
                NativeConfig.transposeBatchRows(config),
                Integer.getInteger("streamfusion.experimental.jvmAccumulatorCacheEntries", 0),
                keyType,
                grouping.length == 1 ? grouping[0] : -1),
            ArrowBatchTypeInformation.INSTANCE,
            input.getParallelism(),
            false);
    transform.setMaxParallelism(
        FlinkKeyGroupUtils.maxParallelism(planner.getExecEnv(), input.getParallelism()));
    transform.setStateKeySelector(new ArrowBatchRowKeySelector(inputType, selector));
    transform.setStateKeyType(selector.getProducedType());
    return transform;
  }
}
