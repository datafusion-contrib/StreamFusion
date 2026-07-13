package io.github.jordepic.streamfusion.planner;

import java.util.List;
import java.util.Map;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.type.RelDataType;
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
    implements StreamPhysicalRel, ColumnarOutput {

  private final RelDataType writerRowType;
  private final RelDataType outputRowType;
  private final Map<String, String> options;
  private final ScanWatermarkSpec watermark;

  public StreamPhysicalNativeKafkaSource(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelDataType outputRowType,
      Map<String, String> options,
      ScanWatermarkSpec watermark) {
    this(cluster, traitSet, outputRowType, outputRowType, options, watermark);
  }

  private StreamPhysicalNativeKafkaSource(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelDataType writerRowType,
      RelDataType outputRowType,
      Map<String, String> options,
      ScanWatermarkSpec watermark) {
    super(cluster, traitSet);
    this.writerRowType = writerRowType;
    this.outputRowType = outputRowType;
    this.options = options;
    this.watermark = watermark;
  }

  /** The table options, for the planner's projection-honoring check. */
  public Map<String, String> options() {
    return options;
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
        getCluster(), getTraitSet(), writerRowType, projected, options, remapped);
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
        getCluster(), traitSet, writerRowType, outputRowType, options, watermark);
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
