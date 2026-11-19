package tech.streamfusion.planner.stream;

import java.util.BitSet;
import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory$;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.utils.ShortcutUtils;
import tech.streamfusion.operator.NativeUdf;
import tech.streamfusion.planner.ColumnarInput;
import tech.streamfusion.planner.ColumnarOutput;
import tech.streamfusion.planner.ExpressionPlanning;
import tech.streamfusion.planner.JsonStringIdentity;
import tech.streamfusion.planner.JvmFunctionConversions;
import tech.streamfusion.planner.NativeCalcExecNode;
import tech.streamfusion.planner.common.EncodedExpression;
import tech.streamfusion.planner.common.ExpressionKind;
import tech.streamfusion.planner.common.NativePlanDetails;

/**
 * Physical node standing in for a {@link org.apache.calcite.rel.core.Calc} the native operator runs:
 * an optional filter condition plus the projection expressions, both encoded for the native engine.
 * The general form of the native filter — it covers computed columns and constants as well as column
 * subsets. Keeps the replaced node's output type and traits; stateless, so no watermark.
 */
public class StreamPhysicalNativeCalc extends StreamPhysicalNativeSingleRel
    implements ColumnarInput, ColumnarOutput {

  private final int[] kinds;
  private final int[] payload;
  private final int[] childCounts;
  private final long[] longs;
  private final double[] doubles;
  private final String[] strings;
  private final int[] projectionRoots;
  private final int conditionRoot;
  private final String[] outputNames;
  private final NativeUdf.Binding udfBinding;
  // The original projection coordinates, before optional input pruning. Used only to prove
  // relationships between output expressions; execution uses the encoded, remapped program.
  private final RexProgram sourceProgram;
  private final boolean preserveCollections;
  private final Calc sourceCalc;
  private final int[] originalInputMapping;
  private final AdaptiveText adaptiveText;

  private static final class AdaptiveText {
    final int[] inputs;
    final int[] outputs;
    final NativeUdf.Binding fallback;
    final long[] fallbackLongs;
    final boolean generatedOnly;

    AdaptiveText(int[] inputs, int[] outputs, EncodedExpression fallback, boolean generatedOnly) {
      this.inputs = inputs;
      this.outputs = outputs;
      this.fallback = fallback.udfBinding();
      this.fallbackLongs = fallback.longs();
      this.generatedOnly = generatedOnly;
    }
  }

  public StreamPhysicalNativeCalc(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelNode input,
      RelDataType outputRowType,
      EncodedExpression encoded) {
    this(cluster, traitSet, input, outputRowType, encoded, null);
  }

  public StreamPhysicalNativeCalc(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelNode input,
      RelDataType outputRowType,
      EncodedExpression encoded,
      RexProgram sourceProgram) {
    this(cluster, traitSet, input, outputRowType, encoded, sourceProgram, NativePlanDetails.EMPTY,
        null, null);
  }

  StreamPhysicalNativeCalc(Calc original, RelNode input, EncodedExpression encoded) {
    this(original, input, encoded, null);
  }

  StreamPhysicalNativeCalc(Calc original, RelNode input, EncodedExpression encoded,
      int[] originalInputMapping) {
    this(
        original.getCluster(),
        original.getTraitSet(),
        input,
        original.getRowType(),
        encoded,
        original.getProgram(),
        NativePlanDetails.from(original),
        original,
        originalInputMapping);
  }

  private StreamPhysicalNativeCalc(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelNode input,
      RelDataType outputRowType,
      EncodedExpression encoded,
      RexProgram sourceProgram,
      NativePlanDetails planDetails,
      Calc sourceCalc,
      int[] originalInputMapping) {
    this(
        cluster,
        traitSet,
        input,
        outputRowType,
        encoded.kinds(),
        encoded.payload(),
        encoded.childCounts(),
        encoded.longs(),
        encoded.doubles(),
        encoded.strings(),
        encoded.projectionRoots(),
        encoded.conditionRoot(),
        encoded.outputNames(),
        encoded.udfBinding(),
        sourceProgram,
        encoded.requiresCollectionRepresentation(),
        planDetails.withFunctions(encoded),
        sourceCalc,
        originalInputMapping,
        null);
  }

  private StreamPhysicalNativeCalc(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelNode input,
      RelDataType outputRowType,
      int[] kinds,
      int[] payload,
      int[] childCounts,
      long[] longs,
      double[] doubles,
      String[] strings,
      int[] projectionRoots,
      int conditionRoot,
      String[] outputNames,
      NativeUdf.Binding udfBinding,
      RexProgram sourceProgram,
      boolean preserveCollections,
      NativePlanDetails planDetails,
      Calc sourceCalc,
      int[] originalInputMapping,
      AdaptiveText adaptiveText) {
    super(cluster, traitSet, input, outputRowType, planDetails);
    this.kinds = kinds;
    this.payload = payload;
    this.childCounts = childCounts;
    this.longs = longs;
    this.doubles = doubles;
    this.strings = strings;
    this.projectionRoots = projectionRoots;
    this.conditionRoot = conditionRoot;
    this.outputNames = outputNames;
    this.udfBinding = udfBinding;
    this.sourceProgram = sourceProgram;
    this.preserveCollections = preserveCollections;
    this.sourceCalc = sourceCalc;
    this.originalInputMapping = originalInputMapping;
    this.adaptiveText = adaptiveText;
  }

  RexProgram sourceProgram() {
    return sourceProgram;
  }

  boolean hasUserFunctions() {
    return udfBinding != null && udfBinding.hasUserFunctions();
  }

  BitSet byteStringOutputs(BitSet input, BitSet used) {
    if (preservesCollections() || udfBinding != null && !udfBinding.isEmpty()) return null;
    return ByteStringInputs.outputs(
        kinds, payload, childCounts, projectionRoots, conditionRoot, input, getRowType(), used);
  }

  StreamPhysicalNativeCalc withAdaptiveText(RelNode input, BitSet byteStrings, BitSet outputs) {
    if (sourceCalc == null || hasUserFunctions()) return null;
    var needed = ByteStringInputs.utf8Inputs(
        kinds, payload, childCounts, projectionRoots, conditionRoot, byteStrings);
    if (needed.isEmpty()) return null;
    var fallback = ExpressionPlanning.encodeByteBackedHostCalc(sourceCalc);
    if (fallback == null || fallback.udfBinding().hasUserFunctions()) return null;
    for (int i = 0; i < outputRowType.getFieldCount(); i++) {
      if (fallback.isVariableBinaryProjection(i)) return null;
    }
    if (originalInputMapping != null) {
      int[] fallbackKinds = fallback.kinds();
      int[] fallbackPayload = fallback.payload();
      for (int i = 0; i < fallbackKinds.length; i++) {
        if (fallbackKinds[i] == ExpressionKind.INPUT_REF.code()
            && originalInputMapping[fallbackPayload[i]] < 0) return null;
      }
      fallback = fallback.remapInputs(originalInputMapping);
    }
    for (int i = 0; i < outputRowType.getFieldCount(); i++) {
      var type = outputRowType.getFieldList().get(i).getType();
      if (SqlTypeFamily.CHARACTER.contains(type)) {
        outputs.set(i);
      } else if (JsonStringIdentity.containsCharacter(type)) {
        // The adaptive result writer only retags top-level StringData as Binary.
        return null;
      }
    }
    // A volatile call site such as RAND(seed) must retain one evaluator across every batch.
    boolean generatedOnly = ExpressionPlanning.isRowCalc(kinds)
        || sourceProgram.getExprList().stream()
            .anyMatch(expression -> !RexUtil.isDeterministic(expression));
    return new StreamPhysicalNativeCalc(
        getCluster(), getTraitSet(), input, outputRowType, kinds, payload, childCounts,
        longs, doubles, strings, projectionRoots, conditionRoot, outputNames, udfBinding,
        sourceProgram, preserveCollections, planDetails, sourceCalc, originalInputMapping,
        new AdaptiveText(needed.stream().toArray(), outputs.stream().toArray(), fallback,
            generatedOnly));
  }

  boolean hasStringPassthrough() {
    for (int i = 0; i < projectionRoots.length; i++) {
      if (kinds[projectionRoots[i]] == 0
          && SqlTypeFamily.CHARACTER.contains(getRowType().getFieldList().get(i).getType()))
        return true;
    }
    return false;
  }

  boolean preservesCollections() {
    return preserveCollections
        || (udfBinding == null || udfBinding.isEmpty())
            && CollectionValueIdentity.passesCollectionReferences(getInput(), sourceProgram);
  }

  boolean usesJavaJsonInput() {
    for (int i = 0; i < kinds.length; i++)
      if (kinds[i] == 6 && payload[i] == 202) return true;
    return false;
  }

  boolean requiresOriginalStringInputs() {
    for (int kind : kinds) if (kind == 44) return true;
    return false;
  }

  @Override
  public boolean carriesStringValues() {
    return preservesCollections();
  }

  @Override
  public boolean aliasesFunctionResults() {
    return JvmFunctionConversions.scalarResultsAlias(sourceProgram);
  }

  @Override
  public RelWriter explainTerms(RelWriter writer) {
    return super.explainTerms(writer)
        .itemIf("collectionRepresentation", true, preserveCollections)
        .itemIf("onDemandUtf8", true, adaptiveText != null && !adaptiveText.generatedOnly)
        .itemIf("byteBackedFlinkEval", true, adaptiveText != null && adaptiveText.generatedOnly)
        .itemIf(
            "jsonEvaluation",
            "JVM",
            sourceProgram != null
                && ExpressionPlanning.containsSqlJson(sourceProgram)
                && ExpressionPlanning.isRowCalc(kinds));
  }

  @Override
  public boolean requireWatermark() {
    return false;
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new StreamPhysicalNativeCalc(
        getCluster(),
        traitSet,
        inputs.get(0),
        outputRowType,
        kinds,
        payload,
        childCounts,
        longs,
        doubles,
        strings,
        projectionRoots,
        conditionRoot,
        outputNames,
        udfBinding,
        sourceProgram,
        preserveCollections,
        planDetails,
        sourceCalc,
        originalInputMapping,
        adaptiveText);
  }

  @Override
  public ExecNode<?> translateToExecNode() {
    if (adaptiveText != null) {
      return new AdaptiveTextCalcExecNode(
          ShortcutUtils.unwrapTableConfig(this), getRelDetailedDescription(),
          FlinkTypeFactory$.MODULE$.toLogicalRowType(getInput().getRowType()),
          FlinkTypeFactory$.MODULE$.toLogicalRowType(getRowType()),
          kinds, payload, childCounts, longs, doubles, strings, projectionRoots, conditionRoot,
          outputNames, udfBinding, adaptiveText.inputs, adaptiveText.outputs, adaptiveText.fallback,
          adaptiveText.fallbackLongs, adaptiveText.generatedOnly);
    }
    return new NativeCalcExecNode(
        ShortcutUtils.unwrapTableConfig(this),
        InputProperty.DEFAULT,
        FlinkTypeFactory$.MODULE$.toLogicalRowType(getRowType()),
        getRelDetailedDescription(),
        kinds,
        payload,
        childCounts,
        longs,
        doubles,
        strings,
        projectionRoots,
        conditionRoot,
        outputNames,
        udfBinding);
  }
}

