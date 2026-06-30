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
 * Transpose entering a columnar region: a rowwise input becomes Arrow batches. Inserted by the
 * transition pass where a columnar operator consumes from a rowwise one. The logical row type is
 * unchanged — only the physical carrier becomes columnar. When the edge carries a changelog,
 * {@code carryRowKind} keeps each row's {@code RowKind} as a hidden column so the native consumer
 * sees it (an insert-only edge omits it — every row is an INSERT).
 *
 * <p>When {@code prunedType} is set the transpose also prunes (nested projection pushdown): it is the
 * input type narrowed to the columns and struct sub-fields a downstream native calc reads, so only
 * those become Arrow columns — the unread fields of a wide source row never get materialized. The
 * consumer's top-level column references are remapped to the compacted positions by the planner.
 */
public class StreamPhysicalRowDataToArrow extends SingleRel
    implements StreamPhysicalRel, ColumnarOutput {

  private final boolean carryRowKind;
  private final RelDataType prunedType;

  public StreamPhysicalRowDataToArrow(
      RelOptCluster cluster, RelTraitSet traitSet, RelNode input, boolean carryRowKind) {
    this(cluster, traitSet, input, carryRowKind, null);
  }

  public StreamPhysicalRowDataToArrow(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelNode input,
      boolean carryRowKind,
      RelDataType prunedType) {
    super(cluster, traitSet, input);
    this.carryRowKind = carryRowKind;
    this.prunedType = prunedType;
  }

  @Override
  public boolean requireWatermark() {
    return false;
  }

  @Override
  protected RelDataType deriveRowType() {
    return prunedType != null ? prunedType : getInput().getRowType();
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new StreamPhysicalRowDataToArrow(
        getCluster(), traitSet, inputs.get(0), carryRowKind, prunedType);
  }

  @Override
  public ExecNode<?> translateToExecNode() {
    return new RowDataToArrowExecNode(
        ShortcutUtils.unwrapTableConfig(this),
        InputProperty.DEFAULT,
        FlinkTypeFactory$.MODULE$.toLogicalRowType(getRowType()),
        getRelDetailedDescription(),
        carryRowKind,
        prunedType == null
            ? null
            : FlinkTypeFactory$.MODULE$.toLogicalRowType(getInput().getRowType()));
  }
}
