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

/**
 * Physical node standing in for a {@link
 * org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalTemporalSort} the native
 * sorter runs: an event-time sort (`ORDER BY rowtime`). Columnar in and out ({@link ColumnarInput}
 * and {@link ColumnarOutput}): it buffers Arrow batches and, on a watermark, emits the rows it has
 * completed in ascending rowtime order, forwarding the batch's columns unchanged. It requires an
 * upstream watermark — the watermark is what releases buffered rows in order.
 */
public class StreamPhysicalNativeTemporalSort extends StreamPhysicalNativeSingleRel
    implements ColumnarInput, ColumnarOutput {

  private final int rowtimeColumn;

  public StreamPhysicalNativeTemporalSort(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelNode input,
      RelDataType outputRowType,
      int rowtimeColumn) {
    super(cluster, traitSet, input, outputRowType);
    this.rowtimeColumn = rowtimeColumn;
  }

  @Override
  public boolean requireWatermark() {
    return true;
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new StreamPhysicalNativeTemporalSort(
        getCluster(), traitSet, inputs.get(0), outputRowType, rowtimeColumn);
  }

  @Override
  public ExecNode<?> translateToExecNode() {
    return new NativeTemporalSortExecNode(
        ShortcutUtils.unwrapTableConfig(this),
        InputProperty.DEFAULT,
        FlinkTypeFactory$.MODULE$.toLogicalRowType(getRowType()),
        getRelDetailedDescription(),
        rowtimeColumn);
  }
}

