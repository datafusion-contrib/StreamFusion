package tech.streamfusion.planner;

import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory$;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalRel;
import org.apache.flink.table.planner.utils.ShortcutUtils;

/**
 * Explicit share point over a deduplicated native sub-plan: one instance appears as the input of
 * every branch that used to carry its own copy of the sub-plan, and it carries the branch count so
 * the runtime operator can declare it on each batch before the chained broadcast fans the record
 * out. Follows RisingWave's {@code StreamShare} (a share node with a plan-time consumer count) and
 * Arroyo's named-node source dedup; Flink's {@code SubplanReuser} is the host-side precedent.
 *
 * <p>Unlike other native rels this node must NOT carry a per-instance digest barrier: Flink's
 * {@code SameRelObjectShuttle} clones a multi-parent rel into one instance per parent and relies on
 * the clones re-merging by digest in {@code SubplanReuseUtil}. The share (and the sub-plan under
 * it, see the source's share token) therefore digests by a token minted once per dedup group — the
 * clones match each other and nothing else, so exactly the branches this pass grouped share one
 * runtime instance.
 */
public class StreamPhysicalNativeShare extends SingleRel
    implements StreamPhysicalRel, ColumnarInput, ColumnarOutput {

  private final int consumers;
  private final long shareToken;

  public StreamPhysicalNativeShare(
      RelOptCluster cluster, RelTraitSet traitSet, RelNode input, int consumers, long shareToken) {
    super(cluster, traitSet, input);
    this.consumers = consumers;
    this.shareToken = shareToken;
  }

  @Override
  public boolean requireWatermark() {
    return false;
  }

  @Override
  protected RelDataType deriveRowType() {
    return getInput().getRowType();
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new StreamPhysicalNativeShare(
        getCluster(), traitSet, inputs.get(0), consumers, shareToken);
  }

  @Override
  public ExecNode<?> translateToExecNode() {
    return new NativeShareExecNode(
        ShortcutUtils.unwrapTableConfig(this),
        InputProperty.DEFAULT,
        FlinkTypeFactory$.MODULE$.toLogicalRowType(getRowType()),
        getRelDetailedDescription(),
        consumers);
  }

  @Override
  public RelWriter explainTerms(RelWriter pw) {
    return super.explainTerms(pw)
        .item("consumers", consumers)
        .itemIf("shareToken", shareToken, pw.getDetailLevel() == SqlExplainLevel.DIGEST_ATTRIBUTES);
  }
}
