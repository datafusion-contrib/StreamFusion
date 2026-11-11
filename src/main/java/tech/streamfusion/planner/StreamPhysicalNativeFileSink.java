package tech.streamfusion.planner;

import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.utils.ShortcutUtils;
import tech.streamfusion.planner.common.NativePlanDetails;

/**
 * Physical node standing in for a filesystem columnar sink the native writer runs. It keeps the
 * replaced sink's row type and traits and carries the matcher's plan — path, partition keys, table
 * options, and the translated encoder settings — for the operator chain the exec node builds.
 * Stateless with respect to event time, so it needs no watermark.
 */
public class StreamPhysicalNativeFileSink extends StreamPhysicalNativeSingleRel
    implements ColumnarInput {

  private final FileSinkMatcher.Planned planned;

  StreamPhysicalNativeFileSink(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelNode input,
      RelDataType outputRowType,
      FileSinkMatcher.Planned planned,
      NativePlanDetails planDetails) {
    super(cluster, traitSet, input, outputRowType, planDetails);
    this.planned = planned;
  }

  @Override
  public String getRelTypeName() {
    return "StreamPhysicalNative"
        + (Character.toUpperCase(planned.format.charAt(0)) + planned.format.substring(1))
        + "Sink";
  }

  @Override
  public boolean requireWatermark() {
    // Partition-time commit triggers consume watermarks inside the reused Flink writer, but they
    // arrive on the stream regardless; the sink itself forces no watermark generation.
    return false;
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new StreamPhysicalNativeFileSink(
        getCluster(), traitSet, inputs.get(0), outputRowType, planned, planDetails);
  }

  @Override
  public ExecNode<?> translateToExecNode() {
    return new NativeFileSinkExecNode(
        ShortcutUtils.unwrapTableConfig(this),
        InputProperty.DEFAULT,
        planned.rowType,
        getRelDetailedDescription(),
        planned);
  }
}
