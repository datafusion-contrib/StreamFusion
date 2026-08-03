package tech.streamfusion.planner;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;
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

/**
 * Leaf physical node standing in for a native rdkafka reader. The selected format provider's decoder
 * rides into the source, so it emits typed Arrow batches and data enters the rest of the plan columnar
 * without becoming rows; a pushed WATERMARK is regenerated per split from the decoded rowtimes. Carries
 * the raw table options and two row types: the full writer schema and the projected output schema the
 * format decoder parses into.
 */
public class StreamPhysicalNativeKafkaSource extends AbstractRelNode
    implements StreamPhysicalRel, ColumnarOutput, ShareableScan {

  private final RelDataType writerRowType;
  private final RelDataType outputRowType;
  private final Map<String, String> options;
  private final ScanWatermarkSpec watermark;
  // 0 = single-consumer source with a per-instance digest barrier; non-zero = the dedup group's
  // token, shared with the StreamPhysicalNativeShare above it so SameRelObjectShuttle clones of
  // the shared subtree re-merge by digest (and match nothing else).
  private final long shareToken;

  public StreamPhysicalNativeKafkaSource(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelDataType outputRowType,
      Map<String, String> options,
      ScanWatermarkSpec watermark) {
    this(cluster, traitSet, outputRowType, outputRowType, options, watermark, 0);
  }

  private StreamPhysicalNativeKafkaSource(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelDataType writerRowType,
      RelDataType outputRowType,
      Map<String, String> options,
      ScanWatermarkSpec watermark,
      long shareToken) {
    super(cluster, traitSet);
    this.writerRowType = writerRowType;
    this.outputRowType = outputRowType;
    this.options = options;
    this.watermark = watermark;
    this.shareToken = shareToken;
  }

  @Override
  public StreamPhysicalNativeKafkaSource withShareToken(long token) {
    return new StreamPhysicalNativeKafkaSource(
        getCluster(), getTraitSet(), writerRowType, outputRowType, options, watermark, token);
  }

  /** The table options, for the planner's projection-honoring check. */
  public Map<String, String> options() {
    return options;
  }

  /**
   * Everything that determines this source's output stream, byte for byte: two sources with equal
   * keys read and decode identically, so the plan can keep one and fan its batches out to every
   * branch (the digest reuse barrier deliberately hides this equivalence from Flink's sub-plan
   * reuse, so the share pass compares semantics directly).
   */
  @Override
  public String sharingKey() {
    return new TreeMap<>(options)
        + "|"
        + writerRowType.getFullTypeString()
        + '|'
        + outputRowType.getFullTypeString()
        + '|'
        + (watermark == null
            ? "none"
            : watermark.rowtimeIndex
                + ":"
                + watermark.rowtimeFieldName
                + ":"
                + watermark.delayMillis
                + ":"
                + watermark.idleTimeoutMillis);
  }

  /**
   * Whether this projection keeps the watermark's rowtime column decoded — the per-split watermark
   * reads it, so a projection that would drop it must not be pushed into the decoder.
   */
  boolean projectionKeepsRowtime(RelDataType projected) {
    return watermark == null || projected.getFieldNames().contains(watermark.rowtimeFieldName);
  }

  /** A copy that emits only {@code projected} (a subset of the full schema), keeping the full schema as
   * the writer type the decoder parses against. */
  public StreamPhysicalNativeKafkaSource withProjection(RelDataType projected) {
    ScanWatermarkSpec remapped =
        watermark == null
            ? null
            : watermark.withRowtimeIndex(
                projected.getFieldNames().indexOf(watermark.rowtimeFieldName));
    return new StreamPhysicalNativeKafkaSource(
        getCluster(), getTraitSet(), writerRowType, projected, options, remapped, shareToken);
  }

  @Override
  public boolean requireWatermark() {
    return false;
  }

  @Override
  protected RelDataType deriveRowType() {
    return outputRowType;
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new StreamPhysicalNativeKafkaSource(
        getCluster(), traitSet, writerRowType, outputRowType, options, watermark, shareToken);
  }


  /** Digest-only reuse barrier — see {@link NativeRelDigests}. */
  private final long reuseBarrier = NativeRelDigests.nextId();
  @Override
  public RelWriter explainTerms(RelWriter writer) {
    RelWriter w =
        super.explainTerms(writer)
            .item("topic", options.getOrDefault("topic", options.get("topic-pattern")));
    if (writerRowType != outputRowType) {
      w = w.item("project", outputRowType.getFieldNames());
    }
    if (watermark != null) {
      w = w.item("watermark", watermark.rowtimeFieldName + " - " + watermark.delayMillis + "ms");
    }
    // A share-tokened source digests by its dedup group, so SameRelObjectShuttle clones re-merge
    // under the one share above them; every other source keeps the per-instance barrier.
    if (shareToken != 0) {
      return w.itemIf(
          "shareToken", shareToken, writer.getDetailLevel() == SqlExplainLevel.DIGEST_ATTRIBUTES);
    }
    return NativeRelDigests.withBarrier(w, reuseBarrier);
  }

  @Override
  public ExecNode<?> translateToExecNode() {
    return new NativeKafkaSourceExecNode(
        ShortcutUtils.unwrapTableConfig(this),
        FlinkTypeFactory$.MODULE$.toLogicalRowType(writerRowType),
        FlinkTypeFactory$.MODULE$.toLogicalRowType(outputRowType),
        getRelDetailedDescription(),
        options,
        watermark);
  }
}
