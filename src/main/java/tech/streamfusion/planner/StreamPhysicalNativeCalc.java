package tech.streamfusion.planner;

import tech.streamfusion.operator.NativeUdf;
import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory$;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.utils.ShortcutUtils;

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

  public StreamPhysicalNativeCalc(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelNode input,
      RelDataType outputRowType,
      RexExpression encoded) {
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
        encoded.udfBinding());
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
      NativeUdf.Binding udfBinding) {
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
        udfBinding);
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

