package tech.streamfusion.planner.stream;

import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.types.logical.RowType;
import tech.streamfusion.operator.AdaptiveTextCalcOperator;
import tech.streamfusion.operator.NativeCalcOperator;
import tech.streamfusion.operator.NativeUdf;
import tech.streamfusion.planner.NativeCalcExecNode;

/** Streaming Calc with the generated Flink program for malformed or cache-sensitive rows. */
final class AdaptiveTextCalcExecNode extends NativeCalcExecNode {
  private final int[] textInputs;
  private final int[] stringOutputs;
  private final RowType inputType;
  private final RowType outputType;
  private final NativeUdf.Binding fallback;
  private final long[] fallbackLongs;
  private final boolean generatedOnly;

  AdaptiveTextCalcExecNode(
      ReadableConfig config, String description, RowType inputType, RowType outputType,
      int[] kinds, int[] payload, int[] childCounts, long[] longs, double[] doubles,
      String[] strings, int[] projectionRoots, int conditionRoot, String[] outputNames,
      NativeUdf.Binding udfBinding, int[] textInputs, int[] stringOutputs,
      NativeUdf.Binding fallback, long[] fallbackLongs, boolean generatedOnly) {
    super(config, InputProperty.DEFAULT, outputType, description, kinds, payload, childCounts,
        longs, doubles, strings, projectionRoots, conditionRoot, outputNames, udfBinding);
    this.textInputs = textInputs;
    this.stringOutputs = stringOutputs;
    this.inputType = inputType;
    this.outputType = outputType;
    this.fallback = fallback;
    this.fallbackLongs = fallbackLongs;
    this.generatedOnly = generatedOnly;
  }

  @Override
  protected NativeCalcOperator createOperator(
      int[] kinds, int[] payload, int[] childCounts, long[] longs, double[] doubles,
      String[] strings, int[] projectionRoots, int conditionRoot, String[] outputNames,
      NativeUdf.Binding binding) {
    return new AdaptiveTextCalcOperator(kinds, payload, childCounts, longs, doubles, strings,
        projectionRoots, conditionRoot, outputNames, binding, textInputs, stringOutputs,
        inputType, outputType, fallback, fallbackLongs, generatedOnly);
  }
}
