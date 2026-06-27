package io.github.jordepic.streamfusion.planner;

import java.util.List;
import java.util.stream.Collectors;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.SetOp;
import org.apache.calcite.rel.core.Union;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory$;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalRel;
import org.apache.flink.table.planner.utils.ShortcutUtils;

/**
 * Physical node standing in for a {@link
 * org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalUnion} (UNION ALL). It is a
 * pure stream merge: every input record flows to the output unchanged, so there is no operator and no
 * native code — the exec node lowers to a {@link
 * org.apache.flink.streaming.api.transformations.UnionTransformation} over the inputs' Arrow streams.
 * Columnar in and out ({@link ColumnarInput} and {@link ColumnarOutput}) with N inputs; because it
 * never touches a record it carries {@code $row_kind$} through untouched and is changelog-transparent.
 */
public class StreamPhysicalNativeUnion extends Union
    implements StreamPhysicalRel, ColumnarInput, ColumnarOutput {

  private final RelDataType outputRowType;

  public StreamPhysicalNativeUnion(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      List<RelNode> inputs,
      RelDataType outputRowType) {
    super(cluster, traitSet, inputs, true);
    this.outputRowType = outputRowType;
  }

  @Override
  public boolean requireWatermark() {
    return false;
  }

  @Override
  protected RelDataType deriveRowType() {
    return outputRowType;
  }

  @Override
  public SetOp copy(RelTraitSet traitSet, List<RelNode> inputs, boolean all) {
    return new StreamPhysicalNativeUnion(getCluster(), traitSet, inputs, outputRowType);
  }

  @Override
  public ExecNode<?> translateToExecNode() {
    List<InputProperty> inputProperties =
        getInputs().stream().map(in -> InputProperty.DEFAULT).collect(Collectors.toList());
    return new NativeUnionExecNode(
        ShortcutUtils.unwrapTableConfig(this),
        inputProperties,
        FlinkTypeFactory$.MODULE$.toLogicalRowType(getRowType()),
        getRelDetailedDescription());
  }
}
