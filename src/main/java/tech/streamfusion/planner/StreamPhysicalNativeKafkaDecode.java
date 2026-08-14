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
 * Leaf physical node for the native-decode Kafka path: Flink owns enumeration, split assignment,
 * offsets, checkpointing, authentication, and the Kafka consumer, while the split reader batches
 * each partition's bytes and decodes them straight to Arrow. The partition identity is retained
 * through collection, so Flink's normal per-split watermark machinery remains authoritative.
 */
public class StreamPhysicalNativeKafkaDecode extends AbstractRelNode
    implements StreamPhysicalRel, ColumnarOutput, ShareableScan, ProjectableNativeSource {

  private final RelDataType outputRowType;
  // The full record schema as written, kept when the output is pruned: JSON ignores it, but Avro needs
  // it as the writer schema (its datums are schema-less) and resolves the pruned output as the reader.
  private final RelDataType writerRowType;
  private final Map<String, String> options;
  private final ScanWatermarkSpec watermark;
  private final long shareToken;
  private final boolean preserveFullSchemaForSharing;

  public StreamPhysicalNativeKafkaDecode(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelDataType outputRowType,
      Map<String, String> options,
      ScanWatermarkSpec watermark,
      boolean preserveFullSchemaForSharing) {
    this(
        cluster,
        traitSet,
        outputRowType,
        outputRowType,
        options,
        watermark,
        0,
        preserveFullSchemaForSharing);
  }

  private StreamPhysicalNativeKafkaDecode(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelDataType outputRowType,
      RelDataType writerRowType,
      Map<String, String> options,
      ScanWatermarkSpec watermark,
      long shareToken,
      boolean preserveFullSchemaForSharing) {
    super(cluster, traitSet);
    this.outputRowType = outputRowType;
    this.writerRowType = writerRowType;
    this.options = options;
    this.watermark = watermark;
    this.shareToken = shareToken;
    this.preserveFullSchemaForSharing = preserveFullSchemaForSharing;
  }

  @Override
  public StreamPhysicalNativeKafkaDecode withShareToken(long token) {
    return new StreamPhysicalNativeKafkaDecode(
        getCluster(),
        getTraitSet(),
        outputRowType,
        writerRowType,
        options,
        watermark,
        token,
        preserveFullSchemaForSharing);
  }

  @Override
  public String sharingKey() {
    return sharingKey(options, writerRowType, outputRowType, watermark);
  }

  static String sharingKey(
      Map<String, String> options,
      RelDataType writerRowType,
      RelDataType outputRowType,
      ScanWatermarkSpec watermark) {
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
   * A copy that decodes only {@code projected}'s columns/fields (the planner's projection pushdown),
   * while remembering this decode's current type as the full writer schema (Avro resolution reads the
   * full record but builds only the projected fields; JSON just decodes the narrowed schema).
   */
  @Override
  public StreamPhysicalNativeKafkaDecode withProjection(RelDataType projected) {
    ScanWatermarkSpec projectedWatermark = watermark;
    if (watermark != null) {
      int rowtimeIndex = projected.getFieldNames().indexOf(watermark.rowtimeFieldName);
      if (rowtimeIndex < 0) {
        throw new IllegalArgumentException("projection dropped the Kafka watermark column");
      }
      projectedWatermark = watermark.withRowtimeIndex(rowtimeIndex);
    }
    return new StreamPhysicalNativeKafkaDecode(
        getCluster(),
        getTraitSet(),
        projected,
        outputRowType,
        options,
        projectedWatermark,
        shareToken,
        preserveFullSchemaForSharing);
  }

  /** Whether decode projection preserves the physical column used by per-split watermarks. */
  @Override
  public boolean supportsProjection(RelDataType projected) {
    return !preserveFullSchemaForSharing
        && KafkaTables.decodeHonorsProjection(options)
        && (watermark == null || projected.getFieldNames().contains(watermark.rowtimeFieldName));
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
    return new StreamPhysicalNativeKafkaDecode(
        getCluster(),
        traitSet,
        outputRowType,
        writerRowType,
        options,
        watermark,
        shareToken,
        preserveFullSchemaForSharing);
  }


  /** Digest-only reuse barrier — see {@link NativeRelDigests}. */
  private final long reuseBarrier = NativeRelDigests.nextId();
  @Override
  public RelWriter explainTerms(RelWriter writer) {
    // The keyed items must be part of the digest: two tables differing only in their key format
    // or key projection would otherwise digest identically and be wrongly share-reused.
    RelWriter explained =
        super.explainTerms(writer)
            .item("topic", options.get("topic"))
            .item("format", options.getOrDefault("value.format", options.get("format")))
            .itemIf("watermarkColumn", watermark == null ? null : watermark.rowtimeFieldName, watermark != null)
            .itemIf("watermarkDelay", watermark == null ? null : watermark.delayMillis, watermark != null)
            .itemIf("keyFormat", options.get("key.format"), options.containsKey("key.format"))
            .itemIf("keyFields", options.get("key.fields"), options.containsKey("key.fields"))
            .itemIf(
                "keyPrefix",
                options.get("key.fields-prefix"),
                options.containsKey("key.fields-prefix"))
            .itemIf(
                "valueFieldsInclude",
                options.get("value.fields-include"),
                options.containsKey("value.fields-include"));
    if (shareToken != 0) {
      return explained.itemIf(
          "shareToken", shareToken, writer.getDetailLevel() == SqlExplainLevel.DIGEST_ATTRIBUTES);
    }
    return NativeRelDigests.withBarrier(explained, reuseBarrier);
  }

  @Override
  public ExecNode<?> translateToExecNode() {
    return new NativeKafkaDecodeExecNode(
        ShortcutUtils.unwrapTableConfig(this),
        FlinkTypeFactory$.MODULE$.toLogicalRowType(getRowType()),
        FlinkTypeFactory$.MODULE$.toLogicalRowType(writerRowType),
        getRelDetailedDescription(),
        options,
        watermark);
  }
}
