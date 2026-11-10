package tech.streamfusion.planner;

import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory$;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.utils.ShortcutUtils;
import tech.streamfusion.hudi.NativeHudiAppendSink;

public final class StreamPhysicalNativeHudiSink extends StreamPhysicalNativeSingleRel
    implements ColumnarInput {
  private final NativeHudiAppendSink.Plan plan;

  StreamPhysicalNativeHudiSink(
      RelOptCluster cluster,
      RelTraitSet traits,
      RelNode input,
      RelDataType output,
      NativeHudiAppendSink.Plan plan,
      NativePlanDetails details) {
    super(cluster, traits, input, output, details);
    this.plan = plan;
  }

  @Override
  public boolean requireWatermark() {
    return false;
  }

  @Override
  public RelNode copy(RelTraitSet traits, List<RelNode> inputs) {
    return new StreamPhysicalNativeHudiSink(
        getCluster(), traits, inputs.get(0), outputRowType, plan, planDetails);
  }

  @Override
  public ExecNode<?> translateToExecNode() {
    return new NativeHudiSinkExecNode(
        ShortcutUtils.unwrapTableConfig(this),
        InputProperty.DEFAULT,
        FlinkTypeFactory$.MODULE$.toLogicalRowType(getRowType()),
        getRelDetailedDescription(),
        plan);
  }
}
