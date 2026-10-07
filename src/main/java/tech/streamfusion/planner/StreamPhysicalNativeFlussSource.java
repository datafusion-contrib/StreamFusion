package tech.streamfusion.planner;

import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalRel;
import org.apache.flink.table.planner.utils.ShortcutUtils;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.flink.source.FlinkSource;
import org.apache.fluss.metadata.TablePath;

/** Fluss log scan whose task boundary emits Arrow and aligned CDC kinds. */
public final class StreamPhysicalNativeFlussSource extends AbstractRelNode
    implements StreamPhysicalRel, ColumnarOutput, ShareableScan, ProjectableNativeSource {
  private final RelDataType outputType;
  private final FlinkSource<?> delegate;
  private final Configuration config;
  private final TablePath path;
  private final ScanWatermarkSpec watermark;
  private final String sourceKey;
  private final boolean preserveSchemaForSharing;
  private final long shareToken;
  private final long reuseBarrier = NativeRelDigests.nextId();

  StreamPhysicalNativeFlussSource(
      RelOptCluster cluster,
      RelTraitSet traits,
      RelDataType outputType,
      FlinkSource<?> delegate,
      Configuration config,
      TablePath path,
      ScanWatermarkSpec watermark,
      String sourceKey,
      boolean preserveSchemaForSharing,
      long shareToken) {
    super(cluster, traits);
    this.outputType = outputType;
    this.delegate = delegate;
    this.config = config;
    this.path = path;
    this.watermark = watermark;
    this.sourceKey = sourceKey;
    this.preserveSchemaForSharing = preserveSchemaForSharing;
    this.shareToken = shareToken;
  }

  @Override
  public boolean requireWatermark() {
    return false;
  }

  @Override
  protected RelDataType deriveRowType() {
    return outputType;
  }

  @Override
  public RelNode copy(RelTraitSet traits, List<RelNode> inputs) {
    return new StreamPhysicalNativeFlussSource(
        getCluster(),
        traits,
        outputType,
        delegate,
        config,
        path,
        watermark,
        sourceKey,
        preserveSchemaForSharing,
        shareToken);
  }

  @Override
  public RelWriter explainTerms(RelWriter writer) {
    RelWriter explained = super.explainTerms(writer).item("table", path);
    return shareToken == 0
        ? NativeRelDigests.withBarrier(explained, reuseBarrier)
        : explained.itemIf(
            "shareToken",
            shareToken,
            writer.getDetailLevel() == org.apache.calcite.sql.SqlExplainLevel.DIGEST_ATTRIBUTES);
  }

  @Override
  public String sharingKey() {
    return sourceKey + "|" + outputType.getFullTypeString();
  }

  @Override
  public RelNode withShareToken(long token) {
    return new StreamPhysicalNativeFlussSource(
        getCluster(),
        getTraitSet(),
        outputType,
        delegate,
        config,
        path,
        watermark,
        sourceKey,
        preserveSchemaForSharing,
        token);
  }

  @Override
  public boolean supportsProjection(RelDataType projected) {
    if (preserveSchemaForSharing || projected.getFieldCount() == 0) return false;
    if (watermark != null && !projected.getFieldNames().contains(watermark.rowtimeFieldName))
      return false;
    for (var field : projected.getFieldList()) {
      var original = outputType.getField(field.getName(), true, false);
      if (original == null || !original.getType().equals(field.getType())) return false;
    }
    return true;
  }

  @Override
  public RelNode withProjection(RelDataType projected) {
    if (!supportsProjection(projected))
      throw new IllegalArgumentException("Unsupported Fluss projection");
    ScanWatermarkSpec next =
        watermark == null
            ? null
            : watermark.withRowtimeIndex(
                projected.getFieldNames().indexOf(watermark.rowtimeFieldName));
    return new StreamPhysicalNativeFlussSource(
        getCluster(),
        getTraitSet(),
        projected,
        delegate,
        config,
        path,
        next,
        sourceKey,
        preserveSchemaForSharing,
        shareToken);
  }

  @Override
  public ExecNode<?> translateToExecNode() {
    return new NativeFlussSourceExecNode(
        ShortcutUtils.unwrapTableConfig(this),
        outputType,
        getRelDetailedDescription(),
        delegate,
        config,
        path,
        watermark);
  }
}
