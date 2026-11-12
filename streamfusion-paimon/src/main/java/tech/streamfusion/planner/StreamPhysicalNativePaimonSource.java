package tech.streamfusion.planner;

import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory$;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalRel;
import org.apache.flink.table.planner.utils.ShortcutUtils;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.predicate.Predicate;

/** Streaming Paimon scan with native file reads and admitted native snapshot merging. */
public final class StreamPhysicalNativePaimonSource extends AbstractRelNode
    implements StreamPhysicalRel, ColumnarOutput, ShareableScan {
  private final RelDataType output;
  private final FileStoreTable table;
  private final NativePlanDetails planDetails;
  private final ScanWatermarkSpec watermark;
  private final Predicate partitionPredicate;
  private final String scanIdentity;
  private final long shareToken;
  private final long reuseBarrier = NativeRelDigests.nextId();

  StreamPhysicalNativePaimonSource(
      RelOptCluster cluster,
      RelTraitSet traits,
      RelDataType output,
      FileStoreTable table,
      ScanWatermarkSpec watermark,
      String scanIdentity) {
    this(cluster, traits, output, table, watermark, null, scanIdentity);
  }

  StreamPhysicalNativePaimonSource(
      RelOptCluster cluster,
      RelTraitSet traits,
      RelDataType output,
      FileStoreTable table,
      ScanWatermarkSpec watermark,
      Predicate partitionPredicate,
      String scanIdentity) {
    this(
        cluster,
        traits,
        output,
        table,
        watermark,
        partitionPredicate,
        scanIdentity,
        NativePlanDetails.EMPTY);
  }

  StreamPhysicalNativePaimonSource(
      RelOptCluster cluster,
      RelTraitSet traits,
      RelDataType output,
      FileStoreTable table,
      ScanWatermarkSpec watermark,
      Predicate partitionPredicate,
      String scanIdentity,
      NativePlanDetails planDetails) {
    this(
        cluster,
        traits,
        output,
        table,
        watermark,
        partitionPredicate,
        scanIdentity,
        0,
        planDetails);
  }

  private StreamPhysicalNativePaimonSource(
      RelOptCluster cluster,
      RelTraitSet traits,
      RelDataType output,
      FileStoreTable table,
      ScanWatermarkSpec watermark,
      Predicate partitionPredicate,
      String scanIdentity,
      long shareToken,
      NativePlanDetails planDetails) {
    super(cluster, traits);
    this.output = output;
    this.table = table;
    this.planDetails = planDetails;
    this.watermark = watermark;
    this.partitionPredicate = partitionPredicate;
    this.scanIdentity = scanIdentity;
    this.shareToken = shareToken;
  }

  @Override
  public String sharingKey() {
    return scanIdentity;
  }

  @Override
  public RelNode withShareToken(long token) {
    return new StreamPhysicalNativePaimonSource(
        getCluster(),
        getTraitSet(),
        output,
        table,
        watermark,
        partitionPredicate,
        scanIdentity,
        token,
        planDetails);
  }

  @Override
  public boolean requireWatermark() {
    return false;
  }

  @Override
  protected RelDataType deriveRowType() {
    return output;
  }

  @Override
  public RelNode copy(RelTraitSet traits, List<RelNode> inputs) {
    return new StreamPhysicalNativePaimonSource(
        getCluster(),
        traits,
        output,
        table,
        watermark,
        partitionPredicate,
        scanIdentity,
        shareToken,
        planDetails);
  }

  @Override
  public RelWriter explainTerms(RelWriter writer) {
    RelWriter explained =
        planDetails
            .explain(super.explainTerms(writer))
            .itemIf("table", table.name(), planDetails == NativePlanDetails.EMPTY)
            .itemIf("partitionPredicate", partitionPredicate, partitionPredicate != null)
            .item("snapshotMerge", "native deduplicate / Java fallback")
            .item("fileDecode", "native " + table.coreOptions().fileFormatString());
    return shareToken == 0
        ? NativeRelDigests.withBarrier(explained, reuseBarrier)
        : explained.itemIf(
            "shareToken", shareToken, writer.getDetailLevel() == SqlExplainLevel.DIGEST_ATTRIBUTES);
  }

  @Override
  public ExecNode<?> translateToExecNode() {
    return new NativePaimonSourceExecNode(
        ShortcutUtils.unwrapTableConfig(this),
        FlinkTypeFactory$.MODULE$.toLogicalRowType(output),
        getRelDetailedDescription(),
        table,
        watermark,
        partitionPredicate);
  }
}
