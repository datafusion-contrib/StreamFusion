package tech.streamfusion.planner;

import java.util.Arrays;
import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.utils.ShortcutUtils;

/** A StarRocks Stream Load sink that consumes Arrow batches and encodes rows natively. */
public final class StreamPhysicalNativeStarRocksSink extends StreamPhysicalNativeSingleRel
    implements ColumnarInput {
  private final StarRocksSinkRuntime runtime;

  StreamPhysicalNativeStarRocksSink(
      RelOptCluster cluster,
      RelTraitSet traits,
      RelNode input,
      RelDataType output,
      StarRocksSinkRuntime runtime) {
    super(cluster, traits, input, output);
    this.runtime = runtime;
  }

  @Override
  public boolean requireWatermark() {
    return false;
  }

  @Override
  public RelWriter explainTerms(RelWriter writer) {
    return super.explainTerms(writer)
        .item("table", runtime.table)
        .item("format", runtime.encoding.isJson() ? "json" : "csv")
        .item("writer", runtime.unifiedSink != null ? "unified" : "sink-function")
        .itemIf("upsertMaterialize", true, runtime.materialize)
        .itemIf(
            "inputUpsertKey",
            Arrays.toString(runtime.inputUpsertKeys),
            runtime.materialize && runtime.inputUpsertKeys != null);
  }

  @Override
  public RelNode copy(RelTraitSet traits, List<RelNode> inputs) {
    return new StreamPhysicalNativeStarRocksSink(
        getCluster(), traits, inputs.get(0), outputRowType, runtime);
  }

  @Override
  public ExecNode<?> translateToExecNode() {
    return new NativeStarRocksSinkExecNode(
        ShortcutUtils.unwrapTableConfig(this), getRelDetailedDescription(), runtime);
  }
}
