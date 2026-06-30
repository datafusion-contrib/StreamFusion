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
 * Leaf physical node standing in for a Kafka source the native rdkafka reader runs. It emits the
 * topic's records as Arrow batches decoded in Rust, so the data starts columnar and never becomes rows
 * — the read side of a fully columnar pipeline. Carries the raw table options (the exec node builds the
 * FLIP-27 source from them) and two row types: the {@code writerRowType} the decoder parses against (the
 * full table schema) and the {@code outputRowType} it emits. They differ when a downstream Calc's
 * projection is pushed into the source ({@link #withProjection}): the decode then builds only the read
 * columns/sub-fields straight from the bytes (a narrowed JSON output schema, a bare-Avro reader schema,
 * or a pruned protobuf descriptor), the source's columnar analog of the entry-transpose pruning.
 */
public class StreamPhysicalNativeKafkaSource extends AbstractRelNode
    implements StreamPhysicalRel, ColumnarOutput {

  private final RelDataType writerRowType;
  private final RelDataType outputRowType;
  private final Map<String, String> options;

  public StreamPhysicalNativeKafkaSource(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelDataType outputRowType,
      Map<String, String> options) {
    this(cluster, traitSet, outputRowType, outputRowType, options);
  }

  private StreamPhysicalNativeKafkaSource(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelDataType writerRowType,
      RelDataType outputRowType,
      Map<String, String> options) {
    super(cluster, traitSet);
    this.writerRowType = writerRowType;
    this.outputRowType = outputRowType;
    this.options = options;
  }

  /** The table options, for the planner's projection-honoring check. */
  public Map<String, String> options() {
    return options;
  }

  /** A copy that emits only {@code projected} (a subset of the full schema), keeping the full schema as
   * the writer type the decoder parses against. */
  public StreamPhysicalNativeKafkaSource withProjection(RelDataType projected) {
    return new StreamPhysicalNativeKafkaSource(
        getCluster(), getTraitSet(), writerRowType, projected, options);
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
        getCluster(), traitSet, writerRowType, outputRowType, options);
  }

  @Override
  public RelWriter explainTerms(RelWriter writer) {
    RelWriter w = super.explainTerms(writer).item("topic", options.get("topic"));
    if (writerRowType != outputRowType) {
      w = w.item("project", outputRowType.getFieldNames());
    }
    return w;
  }

  @Override
  public ExecNode<?> translateToExecNode() {
    return new NativeKafkaSourceExecNode(
        ShortcutUtils.unwrapTableConfig(this),
        FlinkTypeFactory$.MODULE$.toLogicalRowType(writerRowType),
        FlinkTypeFactory$.MODULE$.toLogicalRowType(outputRowType),
        getRelDetailedDescription(),
        options);
  }
}
