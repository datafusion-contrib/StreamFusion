package tech.streamfusion.planner;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.BiRel;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalRel;

/**
 * Two-input counterpart of {@link StreamPhysicalNativeSingleRel} for the native joins: the fixed
 * substitution-time row type and the per-instance digest reuse barrier live here (see {@link
 * NativeRelDigests}). Every native join consumes and produces Arrow batches, so the columnar
 * markers sit on the base.
 */
public abstract class StreamPhysicalNativeBiRel extends BiRel
    implements StreamPhysicalRel, ColumnarInput, ColumnarOutput {

  protected final RelDataType outputRowType;

  /** Digest-only reuse barrier — see {@link NativeRelDigests}. */
  private final long reuseBarrier = NativeRelDigests.nextId();

  protected StreamPhysicalNativeBiRel(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelNode left,
      RelNode right,
      RelDataType outputRowType) {
    super(cluster, traitSet, left, right);
    this.outputRowType = outputRowType;
  }

  @Override
  protected RelDataType deriveRowType() {
    return outputRowType;
  }

  @Override
  public RelWriter explainTerms(RelWriter pw) {
    return NativeRelDigests.withBarrier(super.explainTerms(pw), reuseBarrier);
  }
}
