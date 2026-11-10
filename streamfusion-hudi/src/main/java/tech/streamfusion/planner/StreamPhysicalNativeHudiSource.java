package tech.streamfusion.planner;

import java.util.List;
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
import tech.streamfusion.hudi.NativeHudiSource;

public final class StreamPhysicalNativeHudiSource extends AbstractRelNode
    implements StreamPhysicalRel, ColumnarOutput {
  private final RelDataType output;
  private final NativeHudiSource.Plan source;
  private final NativePlanDetails details;
  private final long reuseBarrier = NativeRelDigests.nextId();

  StreamPhysicalNativeHudiSource(
      RelOptCluster cluster,
      RelTraitSet traits,
      RelDataType output,
      NativeHudiSource.Plan source,
      NativePlanDetails details) {
    super(cluster, traits);
    this.output = output;
    this.source = source;
    this.details = details;
  }

  @Override
  protected RelDataType deriveRowType() {
    return output;
  }

  @Override
  public boolean requiresColumnarConsumer() {
    return true;
  }

  @Override
  public boolean requireWatermark() {
    return false;
  }

  @Override
  public RelNode copy(RelTraitSet traits, List<RelNode> inputs) {
    return new StreamPhysicalNativeHudiSource(getCluster(), traits, output, source, details);
  }

  @Override
  public RelWriter explainTerms(RelWriter writer) {
    return NativeRelDigests.withBarrier(
        details.explain(super.explainTerms(writer)).item("fileDecode", "native Parquet"),
        reuseBarrier);
  }

  @Override
  public ExecNode<?> translateToExecNode() {
    return new NativeHudiSourceExecNode(
        ShortcutUtils.unwrapTableConfig(this),
        FlinkTypeFactory$.MODULE$.toLogicalRowType(output),
        getRelDetailedDescription(),
        source);
  }
}
