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
 * — the read side of a fully columnar pipeline. Carries the scan's row type and the raw table options
 * (the exec node builds the FLIP-27 source from them).
 */
public class StreamPhysicalNativeKafkaSource extends AbstractRelNode
    implements StreamPhysicalRel, ColumnarOutput {

  private final RelDataType outputRowType;
  private final Map<String, String> options;

  public StreamPhysicalNativeKafkaSource(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelDataType outputRowType,
      Map<String, String> options) {
    super(cluster, traitSet);
    this.outputRowType = outputRowType;
    this.options = options;
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
    return new StreamPhysicalNativeKafkaSource(getCluster(), traitSet, outputRowType, options);
  }

  @Override
  public RelWriter explainTerms(RelWriter writer) {
    return super.explainTerms(writer).item("topic", options.get("topic"));
  }

  @Override
  public ExecNode<?> translateToExecNode() {
    return new NativeKafkaSourceExecNode(
        ShortcutUtils.unwrapTableConfig(this),
        FlinkTypeFactory$.MODULE$.toLogicalRowType(getRowType()),
        getRelDetailedDescription(),
        options);
  }
}
