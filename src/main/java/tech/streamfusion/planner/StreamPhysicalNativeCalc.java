package tech.streamfusion.planner;

import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexProgram;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory$;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.utils.ShortcutUtils;
import tech.streamfusion.operator.NativeUdf;

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

  public StreamPhysicalNativeCalc(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelNode input,
      RelDataType outputRowType,
      RexExpression encoded) {
    this(cluster, traitSet, input, outputRowType, encoded, null);
  }

  public StreamPhysicalNativeCalc(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelNode input,
      RelDataType outputRowType,
      RexExpression encoded,
      RexProgram sourceProgram) {
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
        sourceProgram);
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
      RexProgram sourceProgram) {
    super(cluster, traitSet, input, outputRowType);
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
  }

  RexProgram sourceProgram() {
    return sourceProgram;
  }

  @Override
  public RelWriter explainTerms(RelWriter writer) {
    return super.explainTerms(writer)
        .itemIf(
            "jsonEvaluation",
            "JVM",
            sourceProgram != null
                && RexExpression.containsSqlJson(sourceProgram)
                && RexExpression.isRowCalc(kinds));
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
        sourceProgram);
  }

  @Override
  public ExecNode<?> translateToExecNode() {
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

