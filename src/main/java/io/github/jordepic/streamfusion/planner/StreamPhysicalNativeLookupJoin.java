package io.github.jordepic.streamfusion.planner;

import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
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
 * Physical node standing in for a processing-time lookup join the native operator runs. Columnar on
 * its single (probe) input and on its output ({@link ColumnarInput} / {@link ColumnarOutput}); the
 * dimension table is not an input but a {@code LookupFunction} the operator holds and calls per row.
 * Keeping this operator inside the island lets the probe-side Calc/source stay native rather than the
 * whole query falling back to a rowwise plan around the lookup.
 */
public class StreamPhysicalNativeLookupJoin extends SingleRel
    implements StreamPhysicalRel, ColumnarInput, ColumnarOutput {

  private final RelDataType outputRowType;
  private final RelOptTable temporalTable;
  private final int[] orderedDimKeys;
  private final int[] probeKeyIndices;
  private final int joinType;

  public StreamPhysicalNativeLookupJoin(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelNode input,
      RelDataType outputRowType,
      RelOptTable temporalTable,
      int[] orderedDimKeys,
      int[] probeKeyIndices,
      int joinType) {
    super(cluster, traitSet, input);
    this.outputRowType = outputRowType;
    this.temporalTable = temporalTable;
    this.orderedDimKeys = orderedDimKeys;
    this.probeKeyIndices = probeKeyIndices;
    this.joinType = joinType;
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
    return new StreamPhysicalNativeLookupJoin(
        getCluster(),
        traitSet,
        inputs.get(0),
        outputRowType,
        temporalTable,
        orderedDimKeys,
        probeKeyIndices,
        joinType);
  }

  @Override
  public ExecNode<?> translateToExecNode() {
    return new NativeLookupJoinExecNode(
        ShortcutUtils.unwrapTableConfig(this),
        InputProperty.DEFAULT,
        FlinkTypeFactory$.MODULE$.toLogicalRowType(getRowType()),
        getRelDetailedDescription(),
        temporalTable,
        FlinkTypeFactory$.MODULE$.toLogicalRowType(getInput().getRowType()),
        FlinkTypeFactory$.MODULE$.toLogicalRowType(temporalTable.getRowType()),
        orderedDimKeys,
        probeKeyIndices,
        joinType);
  }
}
