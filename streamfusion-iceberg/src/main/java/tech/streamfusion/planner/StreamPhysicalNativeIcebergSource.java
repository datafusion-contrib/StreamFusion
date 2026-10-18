package tech.streamfusion.planner;

import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalRel;
import org.apache.flink.table.planner.utils.ShortcutUtils;
import tech.streamfusion.iceberg.IcebergSourcePlan;

/** Retains released snapshot enumeration and split state with Arrow payloads. */
public final class StreamPhysicalNativeIcebergSource extends AbstractRelNode
    implements StreamPhysicalRel, ColumnarOutput, ShareableScan {
  private final RelDataType output;
  private final IcebergSourcePlan plan;
  private final long shareToken;
  private final long reuseBarrier = NativeRelDigests.nextId();

  StreamPhysicalNativeIcebergSource(
      RelOptCluster cluster, RelTraitSet traits, RelDataType output, IcebergSourcePlan plan) {
    this(cluster, traits, output, plan, 0);
  }

  private StreamPhysicalNativeIcebergSource(
      RelOptCluster cluster,
      RelTraitSet traits,
      RelDataType output,
      IcebergSourcePlan plan,
      long shareToken) {
    super(cluster, traits);
    this.shareToken = shareToken;
    this.output = output;
    this.plan = plan;
  }

  @Override
  protected RelDataType deriveRowType() {
    return output;
  }

  @Override
  public boolean requireWatermark() {
    return false;
  }

  @Override
  public RelNode copy(RelTraitSet traits, List<RelNode> inputs) {
    return new StreamPhysicalNativeIcebergSource(getCluster(), traits, output, plan, shareToken);
  }

  @Override
  public RelWriter explainTerms(RelWriter writer) {
    var explained =
        super.explainTerms(writer)
            .item("table", plan.table.name())
            .item("fileDecode", "native Parquet / explicit stock task fallback");
    return shareToken == 0
        ? NativeRelDigests.withBarrier(explained, reuseBarrier)
        : explained.itemIf(
            "shareToken",
            shareToken,
            writer.getDetailLevel() == org.apache.calcite.sql.SqlExplainLevel.DIGEST_ATTRIBUTES);
  }

  @Override
  public String sharingKey() {
    return plan.table.name()
        + "|"
        + output.getFullTypeString()
        + "|"
        + org.apache.iceberg.SchemaParser.toJson(plan.table.schema())
        + "|"
        + new java.util.TreeMap<>(plan.options)
        + "|"
        + plan.filters;
  }

  @Override
  public RelNode withShareToken(long token) {
    return new StreamPhysicalNativeIcebergSource(getCluster(), getTraitSet(), output, plan, token);
  }

  @Override
  public ExecNode<?> translateToExecNode() {
    return new NativeIcebergSourceExecNode(
        ShortcutUtils.unwrapTableConfig(this),
        FlinkTypeFactory.toLogicalRowType(output),
        getRelDetailedDescription(),
        plan);
  }
}
