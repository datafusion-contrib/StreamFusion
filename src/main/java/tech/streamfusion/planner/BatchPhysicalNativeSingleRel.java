package tech.streamfusion.planner;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchPhysicalRel;

/** Batch nodes retain logical types and single-consumer Arrow ownership through subplan reuse. */
abstract class BatchPhysicalNativeSingleRel extends SingleRel implements BatchPhysicalRel {
  private final RelDataType outputType;
  private final long reuseBarrier = NativeRelDigests.nextId();
  protected final NativePlanDetails details;

  BatchPhysicalNativeSingleRel(RelNode original, RelNode input, NativePlanDetails details) {
    super(original.getCluster(), original.getTraitSet(), input);
    outputType = original.getRowType();
    this.details = details;
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
