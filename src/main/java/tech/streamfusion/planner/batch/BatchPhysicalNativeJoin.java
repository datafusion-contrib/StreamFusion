package tech.streamfusion.planner.batch;

import java.util.List;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.BiRel;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory$;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchPhysicalRel;
import org.apache.flink.table.planner.utils.ShortcutUtils;
import tech.streamfusion.planner.ColumnarInput;
import tech.streamfusion.planner.ColumnarOutput;
import tech.streamfusion.planner.common.NativePlanDetails;
import tech.streamfusion.planner.common.NativeRelDigests;

final class BatchPhysicalNativeJoin extends BiRel
    implements BatchPhysicalRel, ColumnarInput, ColumnarOutput {
  private final RelDataType outputType;
  private final BatchJoinSpec spec;
  private final NativePlanDetails details;
  private final long reuseBarrier = NativeRelDigests.nextId();

  BatchPhysicalNativeJoin(RelNode original, RelNode left, RelNode right, BatchJoinSpec spec) {
    this(original, left, right, spec, NativePlanDetails.from(original));
  }

  private BatchPhysicalNativeJoin(
      RelNode original,
      RelNode left,
      RelNode right,
      BatchJoinSpec spec,
      NativePlanDetails details) {
    super(original.getCluster(), original.getTraitSet(), left, right);
    outputType = original.getRowType();
    this.spec = spec;
    this.details = details;
  }

  @Override
  protected RelDataType deriveRowType() {
    return outputType;
  }

  @Override
  public RelNode copy(RelTraitSet traits, List<RelNode> inputs) {
    var copy = new BatchPhysicalNativeJoin(this, inputs.get(0), inputs.get(1), spec, details);
    copy.traitSet = traits;
    return copy;
  }

  @Override
  public RelWriter explainTerms(RelWriter writer) {
    return NativeRelDigests.withBarrier(details.explain(super.explainTerms(writer)), reuseBarrier);
  }

  @Override
  public ExecNode<?> translateToExecNode() {
    return new BatchJoinExecNode(
        ShortcutUtils.unwrapTableConfig(this),
        FlinkTypeFactory$.MODULE$.toLogicalRowType(getLeft().getRowType()),
        FlinkTypeFactory$.MODULE$.toLogicalRowType(getRight().getRowType()),
        FlinkTypeFactory$.MODULE$.toLogicalRowType(getRowType()),
        getRelDetailedDescription(),
        spec);
  }
}
