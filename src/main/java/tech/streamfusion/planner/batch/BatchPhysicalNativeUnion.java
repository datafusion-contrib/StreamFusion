package tech.streamfusion.planner.batch;

import java.util.List;
import java.util.stream.Collectors;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.SetOp;
import org.apache.calcite.rel.core.Union;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory$;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchPhysicalRel;
import org.apache.flink.table.planner.utils.ShortcutUtils;
import tech.streamfusion.planner.ColumnarInput;
import tech.streamfusion.planner.ColumnarOutput;
import tech.streamfusion.planner.common.CommonUnionExecNode;
import tech.streamfusion.planner.common.NativeRelDigests;

final class BatchPhysicalNativeUnion extends Union
    implements BatchPhysicalRel, ColumnarInput, ColumnarOutput {
  private final long reuseBarrier = NativeRelDigests.nextId();

  BatchPhysicalNativeUnion(Union original, List<RelNode> inputs) {
    super(original.getCluster(), original.getTraitSet(), inputs, true);
  }

  @Override
  public SetOp copy(RelTraitSet traits, List<RelNode> inputs, boolean all) {
    if (!all) throw new IllegalArgumentException("Native batch union requires UNION ALL");
    var copy = new BatchPhysicalNativeUnion(this, inputs);
    copy.traitSet = traits;
    return copy;
  }

  @Override
  public RelWriter explainTerms(RelWriter writer) {
    return NativeRelDigests.withBarrier(super.explainTerms(writer), reuseBarrier);
  }

  @Override
  public ExecNode<?> translateToExecNode() {
    return new BatchNativeExecNode<>(
        ShortcutUtils.unwrapTableConfig(this),
        new CommonUnionExecNode(
            ShortcutUtils.unwrapTableConfig(this),
            getInputs().stream().map(input -> InputProperty.DEFAULT).collect(Collectors.toList()),
            FlinkTypeFactory$.MODULE$.toLogicalRowType(getRowType()),
            getRelDetailedDescription(),
            true));
  }
}
