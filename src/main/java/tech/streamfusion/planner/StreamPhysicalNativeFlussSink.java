package tech.streamfusion.planner;

import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.utils.ShortcutUtils;
import org.apache.flink.table.types.logical.RowType;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.TablePath;

public final class StreamPhysicalNativeFlussSink extends StreamPhysicalNativeSingleRel
    implements ColumnarInput {
  private final List<String> partitionKeys;
  private final RowType type;
  private final Configuration config;
  private final TablePath path;

  StreamPhysicalNativeFlussSink(
      RelOptCluster cluster,
      RelTraitSet traits,
      RelNode input,
      RelDataType outputType,
      RowType type,
      Configuration config,
      TablePath path,
      List<String> partitionKeys) {
    super(cluster, traits, input, outputType);
    this.type = type;
    this.config = config;
    this.path = path;
    this.partitionKeys = List.copyOf(partitionKeys);
  }

  @Override
  public boolean requireWatermark() {
    return false;
  }

  @Override
  public RelNode copy(RelTraitSet traits, List<RelNode> inputs) {
    return new StreamPhysicalNativeFlussSink(
        getCluster(), traits, inputs.get(0), outputRowType, type, config, path, partitionKeys);
  }

  @Override
  public ExecNode<?> translateToExecNode() {
    return new NativeFlussSinkExecNode(
        ShortcutUtils.unwrapTableConfig(this),
        type,
        getRelDetailedDescription(),
        config,
        path,
        partitionKeys);
  }
}
