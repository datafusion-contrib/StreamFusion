package tech.streamfusion.planner;

import java.util.List;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory$;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchPhysicalRel;
import org.apache.flink.table.planner.utils.ShortcutUtils;

/** Batch counterpart of the existing retained-view native share point. */
final class BatchPhysicalNativeShare extends SingleRel
    implements BatchPhysicalRel, ColumnarInput, ColumnarOutput {
  private final int consumers;
  private final long token;

  BatchPhysicalNativeShare(RelNode input, int consumers, long token) {
    super(input.getCluster(), input.getTraitSet(), input);
    this.consumers = consumers;
    this.token = token;
  }

  @Override
  public RelNode copy(RelTraitSet traits, List<RelNode> inputs) {
    var copy = new BatchPhysicalNativeShare(inputs.get(0), consumers, token);
    copy.traitSet = traits;
    return copy;
  }

  @Override
  public ExecNode<?> translateToExecNode() {
    var config = ShortcutUtils.unwrapTableConfig(this);
    return new BatchNativeExecNode<>(
        config,
        new NativeShareExecNode(
            config,
            InputProperty.DEFAULT,
            FlinkTypeFactory$.MODULE$.toLogicalRowType(getRowType()),
            getRelDetailedDescription(),
            consumers));
  }

  @Override
  public RelWriter explainTerms(RelWriter writer) {
    return super.explainTerms(writer)
        .item("consumers", consumers)
        .itemIf("shareToken", token, writer.getDetailLevel() == SqlExplainLevel.DIGEST_ATTRIBUTES);
  }
}
