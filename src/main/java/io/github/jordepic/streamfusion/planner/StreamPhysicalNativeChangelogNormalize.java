package io.github.jordepic.streamfusion.planner;

import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory$;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalRel;
import org.apache.flink.table.planner.utils.ShortcutUtils;

/**
 * Physical node standing in for a {@link
 * org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalChangelogNormalize} the
 * native operator runs: keep-last-per-unique-key changelog normalization. Columnar in and out ({@link
 * ColumnarInput} and {@link ColumnarOutput}) — it both consumes and emits a changelog (the row kind
 * rides the batch's {@code $row_kind$} column), so a native changelog chain pays no per-operator
 * transpose; the keyed shuffle stays columnar where the input is a columnar producer.
 */
public class StreamPhysicalNativeChangelogNormalize extends SingleRel
    implements StreamPhysicalRel, ColumnarInput, ColumnarOutput {

  private final RelDataType outputRowType;
  private final int[] keyColumns;
  private final boolean generateUpdateBefore;

  public StreamPhysicalNativeChangelogNormalize(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelNode input,
      RelDataType outputRowType,
      int[] keyColumns,
      boolean generateUpdateBefore) {
    super(cluster, traitSet, input);
    this.outputRowType = outputRowType;
    this.keyColumns = keyColumns;
    this.generateUpdateBefore = generateUpdateBefore;
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
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new StreamPhysicalNativeChangelogNormalize(
        getCluster(), traitSet, inputs.get(0), outputRowType, keyColumns, generateUpdateBefore);
  }

  @Override
  public ExecNode<?> translateToExecNode() {
    return new NativeChangelogNormalizeExecNode(
        ShortcutUtils.unwrapTableConfig(this),
        InputProperty.DEFAULT,
        FlinkTypeFactory$.MODULE$.toLogicalRowType(getRowType()),
        getRelDetailedDescription(),
        keyColumns,
        generateUpdateBefore);
  }
}
