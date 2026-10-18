package tech.streamfusion.planner;

import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.utils.ShortcutUtils;
import tech.streamfusion.iceberg.IcebergSinkPlan;

public final class StreamPhysicalNativeIcebergSink extends StreamPhysicalNativeSingleRel
    implements ColumnarInput {
  private final IcebergSinkPlan plan;

  StreamPhysicalNativeIcebergSink(
      RelOptCluster cluster,
      RelTraitSet traits,
      RelNode input,
      RelDataType outputType,
      IcebergSinkPlan plan) {
    super(cluster, traits, input, outputType);
    this.plan = plan;
  }

  @Override
  public boolean requireWatermark() {
    return false;
  }

  @Override
  public RelNode copy(RelTraitSet traits, List<RelNode> inputs) {
    return new StreamPhysicalNativeIcebergSink(
        getCluster(), traits, inputs.get(0), outputRowType, plan);
  }

  @Override
  public ExecNode<?> translateToExecNode() {
    return new NativeIcebergSinkExecNode(
        ShortcutUtils.unwrapTableConfig(this),
        InputProperty.DEFAULT,
        FlinkTypeFactory.toLogicalRowType(getRowType()),
        getRelDetailedDescription(),
        plan);
  }
}
