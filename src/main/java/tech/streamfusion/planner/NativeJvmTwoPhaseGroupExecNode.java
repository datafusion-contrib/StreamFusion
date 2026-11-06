package tech.streamfusion.planner;

import java.util.Arrays;
import java.util.Collections;
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
import org.apache.flink.table.planner.plan.utils.AggregateInfoList;
import org.apache.flink.table.planner.plan.utils.KeySelectorUtil;
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil;
import org.apache.flink.table.runtime.operators.aggregate.MiniBatchGlobalGroupAggFunction;
import org.apache.flink.table.runtime.operators.aggregate.MiniBatchLocalGroupAggFunction;
import org.apache.flink.table.runtime.types.LogicalTypeDataTypeConverter;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import tech.streamfusion.operator.ArrowBatch;
import tech.streamfusion.operator.ArrowBatchRowKeySelector;
import tech.streamfusion.operator.ArrowBatchTypeInformation;
import tech.streamfusion.operator.NativeColumnarJvmGlobalGroupOperator;
import tech.streamfusion.operator.NativeColumnarJvmGroupOperator;
import tech.streamfusion.operator.NativeColumnarJvmLocalGroupOperator;

final class NativeJvmTwoPhaseGroupExecNode extends ExecNodeBase<ArrowBatch>
    implements StreamExecNode<ArrowBatch>, SingleTransformationTranslator<ArrowBatch> {
  private final RowType inputType;
  private final RowType originalInput;
  private final RowType partial;
  private final int[] grouping;
  private final boolean retract;
  private final boolean updateBefore;
  private final Long ttl;
  private final AggregateInfoList localInfo;
  private final AggregateInfoList globalInfo;

  NativeJvmTwoPhaseGroupExecNode(
      ReadableConfig config,
      RowType inputType,
      RowType outputType,
      RowType originalInput,
      RowType partial,
      int[] grouping,
      boolean retract,
      boolean updateBefore,
      Long ttl,
      AggregateInfoList localInfo,
      AggregateInfoList globalInfo,
      String description) {
    super(
        ExecNodeContext.newNodeId(),
        new ExecNodeContext("stream-exec-native-jvm-two-phase-group_1"),
        config,
        Collections.singletonList(InputProperty.DEFAULT),
        outputType,
        description);
    this.inputType = inputType;
    this.originalInput = originalInput;
    this.partial = partial;
    this.grouping = grouping;
    this.retract = retract;
    this.updateBefore = updateBefore;
    this.ttl = ttl;
    this.localInfo = localInfo;
    this.globalInfo = globalInfo;
  }

  private AggsHandlerCodeGenerator generator(PlannerBase planner, ExecNodeConfig config) {
    return new AggsHandlerCodeGenerator(
        new CodeGeneratorContext(config, planner.getFlinkContext().getClassLoader()),
        planner.createRelBuilder(),
        JavaScalaConversionUtil.toScala(originalInput.getChildren()),
        true);
  }

  @Override
  @SuppressWarnings("unchecked")
  protected Transformation<ArrowBatch> translateToPlanInternal(
      PlannerBase planner, ExecNodeConfig config) {
    var input = (Transformation<ArrowBatch>) getInputEdges().get(0).translateToPlan(planner);
    var selector =
        KeySelectorUtil.getRowDataSelector(
            planner.getFlinkContext().getClassLoader(), grouping, InternalTypeInfo.of(inputType));
    long retention = ttl == null ? config.getStateRetentionTime() : ttl;
    boolean global = globalInfo != null;
    var trigger = tech.streamfusion.compat.JvmAggregateCompat.miniBatchTrigger(config);
    NativeColumnarJvmGroupOperator operator;
    if (global) {
      var localHandler =
          generator(planner, config)
              .needMerge(grouping.length, true, localInfo.getAccTypes())
              .generateAggsHandler("ColumnarJvmLocalMergeHandler", localInfo);
      var globalHandler =
          generator(planner, config)
              .needMerge(0, true, localInfo.getAccTypes())
              .generateAggsHandler("ColumnarJvmGlobalMergeHandler", globalInfo);
      LogicalType[] accTypes =
          Arrays.stream(globalInfo.getAccTypes())
              .map(LogicalTypeDataTypeConverter::fromDataTypeToLogicalType)
              .toArray(LogicalType[]::new);
      LogicalType[] values =
          Arrays.stream(globalInfo.getActualValueTypes())
              .map(LogicalTypeDataTypeConverter::fromDataTypeToLogicalType)
              .toArray(LogicalType[]::new);
      var equaliser =
          new EqualiserCodeGenerator(values, planner.getFlinkContext().getClassLoader())
              .generateRecordEqualiser("ColumnarJvmGlobalEqualiser");
      var function =
          new MiniBatchGlobalGroupAggFunction(
              localHandler,
              globalHandler,
              equaliser,
              accTypes,
              globalInfo.getIndexOfCountStar(),
              updateBefore,
              retention);
      operator =
          new NativeColumnarJvmGlobalGroupOperator(
              inputType,
              (RowType) getOutputType(),
              function,
              trigger,
              selector,
              NativeConfig.transposeBatchRows(config),
              partial,
              retention);
    } else {
      var generator = generator(planner, config).needAccumulate().needMerge(0, true, null);
      if (retract) generator.needRetract();
      var function =
          new MiniBatchLocalGroupAggFunction(
              generator.generateAggsHandler("ColumnarJvmLocalHandler", localInfo));
      operator =
          new NativeColumnarJvmLocalGroupOperator(
              inputType,
              (RowType) getOutputType(),
              function,
              trigger,
              selector,
              NativeConfig.transposeBatchRows(config),
              partial);
    }
    String name = global ? "NativeJvmGlobalGroupExecNode" : "NativeJvmLocalGroupExecNode";
    var transform =
        ExecNodeUtil.createOneInputTransformation(
            input,
            createTransformationMeta(
                global ? "native-jvm-global-group" : "native-jvm-local-group",
                name + "(" + getDescription() + ")",
                name,
                config),
            operator,
            ArrowBatchTypeInformation.INSTANCE,
            input.getParallelism(),
            false);
    if (global) {
      transform.setMaxParallelism(
          FlinkKeyGroupUtils.maxParallelism(planner.getExecEnv(), input.getParallelism()));
      transform.setStateKeySelector(new ArrowBatchRowKeySelector(inputType, selector));
      transform.setStateKeyType(selector.getProducedType());
    }
    return transform;
  }
}
