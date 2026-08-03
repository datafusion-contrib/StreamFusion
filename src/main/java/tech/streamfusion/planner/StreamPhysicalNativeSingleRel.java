package tech.streamfusion.planner;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalRel;

/**
 * Base of the single-input physical rels of the native island. Centralizes the two contracts every
 * one of them must honor: the row type is the one fixed at substitution time (never re-derived from
 * the input), and the digest carries a per-instance reuse barrier so Flink's sub-plan reuse can
 * never merge two native nodes — see {@link NativeRelDigests}. A subclass that adds its own digest
 * terms overrides {@link #explainTerms} and calls super, keeping the barrier. The deliberate
 * exception is the share node, whose whole point is to merge by digest; it stays off this base.
 *
 * <p>A subclass whose output type simply follows its input (the transposes) uses the constructor
 * without a row type.
 */
public abstract class StreamPhysicalNativeSingleRel extends SingleRel implements StreamPhysicalRel {

  protected final RelDataType outputRowType;

  /** Digest-only reuse barrier — see {@link NativeRelDigests}. */
  private final long reuseBarrier = NativeRelDigests.nextId();

  protected StreamPhysicalNativeSingleRel(
      RelOptCluster cluster, RelTraitSet traitSet, RelNode input, RelDataType outputRowType) {
    super(cluster, traitSet, input);
    this.outputRowType = outputRowType;
  }

  protected StreamPhysicalNativeSingleRel(
      RelOptCluster cluster, RelTraitSet traitSet, RelNode input) {
    this(cluster, traitSet, input, null);
  }

  @Override
  protected RelDataType deriveRowType() {
    return outputRowType != null ? outputRowType : getInput().getRowType();
  }

  @Override
  public RelWriter explainTerms(RelWriter pw) {
    return NativeRelDigests.withBarrier(super.explainTerms(pw), reuseBarrier);
  }
}
