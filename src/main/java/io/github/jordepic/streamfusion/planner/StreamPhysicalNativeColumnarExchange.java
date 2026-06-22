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
 * A columnar keyed exchange: it stands in for a {@link
 * org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalExchange} whose downstream
 * is a native columnar operator, carrying Arrow batches across the shuffle instead of transposing to
 * rows. It splits each batch by key into per-channel sub-batches and routes them, co-locating every
 * row of a key on one channel — all the downstream native operator needs, since it re-groups by key
 * itself (operator state), so the shuffle hash need not match Flink's key-group hash. Schema- and
 * watermark-transparent ({@link ColumnarInput} and {@link ColumnarOutput}); the watermark flows
 * through the partition transformation as usual.
 */
public class StreamPhysicalNativeColumnarExchange extends SingleRel
    implements StreamPhysicalRel, ColumnarInput, ColumnarOutput {

  private final RelDataType outputRowType;
  private final int[] keyColumns;

  public StreamPhysicalNativeColumnarExchange(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelNode input,
      RelDataType outputRowType,
      int[] keyColumns) {
    super(cluster, traitSet, input);
    this.outputRowType = outputRowType;
    this.keyColumns = keyColumns;
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
    return new StreamPhysicalNativeColumnarExchange(
        getCluster(), traitSet, inputs.get(0), outputRowType, keyColumns);
  }

  @Override
  public ExecNode<?> translateToExecNode() {
    return new NativeColumnarExchangeExecNode(
        ShortcutUtils.unwrapTableConfig(this),
        InputProperty.DEFAULT,
        FlinkTypeFactory$.MODULE$.toLogicalRowType(getRowType()),
        getRelDetailedDescription(),
        keyColumns);
  }
}
