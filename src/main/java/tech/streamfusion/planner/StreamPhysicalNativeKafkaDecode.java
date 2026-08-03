package tech.streamfusion.planner;

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
 * Leaf physical node for the shallow native-decode Kafka path: Flink's own {@code KafkaSource} consumes
 * the topic's raw value bytes (offsets/checkpointing/auth all Flink's), and a native operator decodes
 * those bytes straight to Arrow — skipping Flink's per-record {@code RowData} materialization. The data
 * starts columnar at the source edge. Carries the scan's row type and raw table options (the exec node
 * builds the byte source + picks the decode format from them).
 */
public class StreamPhysicalNativeKafkaDecode extends AbstractRelNode
    implements StreamPhysicalRel, ColumnarOutput {

  private final RelDataType outputRowType;
  // The full record schema as written, kept when the output is pruned: JSON ignores it, but Avro needs
  // it as the writer schema (its datums are schema-less) and resolves the pruned output as the reader.
  private final RelDataType writerRowType;
  private final Map<String, String> options;

  public StreamPhysicalNativeKafkaDecode(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelDataType outputRowType,
      Map<String, String> options) {
    this(cluster, traitSet, outputRowType, outputRowType, options);
  }

  private StreamPhysicalNativeKafkaDecode(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelDataType outputRowType,
      RelDataType writerRowType,
      Map<String, String> options) {
    super(cluster, traitSet);
    this.outputRowType = outputRowType;
    this.writerRowType = writerRowType;
    this.options = options;
  }

  Map<String, String> options() {
    return options;
  }

  /**
   * A copy that decodes only {@code projected}'s columns/fields (the planner's projection pushdown),
   * while remembering this decode's current type as the full writer schema (Avro resolution reads the
   * full record but builds only the projected fields; JSON just decodes the narrowed schema).
   */
  StreamPhysicalNativeKafkaDecode withProjection(RelDataType projected) {
    return new StreamPhysicalNativeKafkaDecode(
        getCluster(), getTraitSet(), projected, outputRowType, options);
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
        getCluster(), traitSet, outputRowType, writerRowType, options);
  }


  /** Digest-only reuse barrier — see {@link NativeRelDigests}. */
  private final long reuseBarrier = NativeRelDigests.nextId();
  @Override
  public RelWriter explainTerms(RelWriter writer) {
    // The keyed items must be part of the digest: two tables differing only in their key format
    // or key projection would otherwise digest identically and be wrongly share-reused.
    return NativeRelDigests.withBarrier(
        super.explainTerms(writer)
            .item("topic", options.get("topic"))
            .item("format", options.getOrDefault("value.format", options.get("format")))
            .itemIf("keyFormat", options.get("key.format"), options.containsKey("key.format"))
            .itemIf("keyFields", options.get("key.fields"), options.containsKey("key.fields"))
            .itemIf(
                "keyPrefix",
                options.get("key.fields-prefix"),
                options.containsKey("key.fields-prefix"))
            .itemIf(
                "valueFieldsInclude",
                options.get("value.fields-include"),
                options.containsKey("value.fields-include")),
        reuseBarrier);
  }

  @Override
  public ExecNode<?> translateToExecNode() {
    return new NativeKafkaDecodeExecNode(
        ShortcutUtils.unwrapTableConfig(this),
        FlinkTypeFactory$.MODULE$.toLogicalRowType(getRowType()),
        FlinkTypeFactory$.MODULE$.toLogicalRowType(writerRowType),
        getRelDetailedDescription(),
        options);
  }
}
