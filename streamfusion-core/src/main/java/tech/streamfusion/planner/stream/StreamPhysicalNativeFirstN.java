package tech.streamfusion.planner.stream;

import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory$;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.utils.ShortcutUtils;
import tech.streamfusion.planner.ColumnarInput;
import tech.streamfusion.planner.ColumnarOutput;
import tech.streamfusion.planner.FlinkKeyGroupUtils;

public class StreamPhysicalNativeFirstN extends StreamPhysicalNativeSingleRel
    implements ColumnarInput, ColumnarOutput {
  private final int[] partitions;
  private final int limit;
  private final boolean outputRank;

  public StreamPhysicalNativeFirstN(
      RelOptCluster cluster,
      RelTraitSet traits,
      RelNode input,
      RelDataType outputType,
      int[] partitions,
      int limit,
      boolean outputRank) {
    super(cluster, traits, input, outputType);
    this.partitions = partitions;
    this.limit = limit;
    this.outputRank = outputRank;
  }

  @Override
  public boolean requireWatermark() {
    return false;
  }

  @Override
  public RelNode copy(RelTraitSet traits, List<RelNode> inputs) {
    return new StreamPhysicalNativeFirstN(
        getCluster(), traits, inputs.get(0), outputRowType, partitions, limit, outputRank);
  }

  @Override
  public ExecNode<?> translateToExecNode() {
    return new NativeFirstNExecNode(
        ShortcutUtils.unwrapTableConfig(this),
        InputProperty.DEFAULT,
        FlinkTypeFactory$.MODULE$.toLogicalRowType(getRowType()),
        getRelDetailedDescription(),
        partitions,
        FlinkKeyGroupUtils.timestampPrecisions(getInput().getRowType(), partitions),
        limit,
        outputRank);
  }
}
