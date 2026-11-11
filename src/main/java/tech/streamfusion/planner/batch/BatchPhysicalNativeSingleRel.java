package tech.streamfusion.planner.batch;

import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchPhysicalRel;
import org.apache.flink.table.planner.plan.trait.FlinkRelDistributionTraitDef;
import tech.streamfusion.planner.common.NativePlanDetails;
import tech.streamfusion.planner.common.NativeRelDigests;

/** Batch nodes retain logical types and single-consumer Arrow ownership through subplan reuse. */
abstract class BatchPhysicalNativeSingleRel extends SingleRel implements BatchPhysicalRel {
  private final RelDataType outputType;
  private final long reuseBarrier;
  private boolean shared;
  protected final NativePlanDetails details;

  protected final InputProperty inputProperty(InputProperty.DamBehavior dam) {
    var distribution = getInput().getTraitSet().getTrait(FlinkRelDistributionTraitDef.INSTANCE());
    var builder = InputProperty.builder().damBehavior(dam);
    if (distribution != null && distribution.getType() == RelDistribution.Type.SINGLETON) {
      builder.requiredDistribution(InputProperty.SINGLETON_DISTRIBUTION);
    }
    return builder.build();
  }

  BatchPhysicalNativeSingleRel(RelNode original, RelNode input, NativePlanDetails details) {
    super(original.getCluster(), original.getTraitSet(), input);
    outputType = original.getRowType();
    this.details = details;
    shared =
        original instanceof BatchPhysicalNativeSingleRel
            && ((BatchPhysicalNativeSingleRel) original).shared;
    reuseBarrier =
        shared ? ((BatchPhysicalNativeSingleRel) original).reuseBarrier : NativeRelDigests.nextId();
  }

  final boolean allowSharedCopies() {
    if (getInput() instanceof BatchPhysicalNativeSingleRel) {
      if (!((BatchPhysicalNativeSingleRel) getInput()).allowSharedCopies()) return false;
    } else if (!getInput().getInputs().isEmpty()) return false;
    shared = true;
    return true;
  }

  @Override
  protected RelDataType deriveRowType() {
    return outputType;
  }

  @Override
  public RelWriter explainTerms(RelWriter writer) {
    return NativeRelDigests.withBarrier(details.explain(super.explainTerms(writer)), reuseBarrier);
  }
}
