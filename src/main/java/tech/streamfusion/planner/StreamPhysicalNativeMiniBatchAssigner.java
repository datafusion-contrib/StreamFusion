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
 * Columnar form of Flink's {@code MiniBatchAssigner}: forwards Arrow batches ({@link ColumnarInput}
 * and {@link ColumnarOutput}) and emits the mini-batch marker watermarks that the downstream native
 * local aggregate flushes on — generated from processing time (proc-time mode) or filtered from the
 * upstream event-time watermarks (row-time mode). Insert-only (it adds no rows and touches no
 * {@code $row_kind$}), so it requires no changelog handling.
 */
public class StreamPhysicalNativeMiniBatchAssigner extends StreamPhysicalNativeSingleRel
    implements ColumnarInput, ColumnarOutput {

  private final long intervalMs;
  private final boolean rowTime;

  public StreamPhysicalNativeMiniBatchAssigner(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelNode input,
      RelDataType outputRowType,
      long intervalMs,
      boolean rowTime) {
    super(cluster, traitSet, input, outputRowType);
    this.intervalMs = intervalMs;
    this.rowTime = rowTime;
  }

  @Override
  public boolean requireWatermark() {
    return false;
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new StreamPhysicalNativeMiniBatchAssigner(
        getCluster(), traitSet, inputs.get(0), outputRowType, intervalMs, rowTime);
  }

  /**
   * A copy over a different (narrower) input — the assigner forwards batches untouched, so a calc
   * above it can push its projection pruning through to the entry transpose below (the assigner's
   * row type just follows the pruned input).
   */
  StreamPhysicalNativeMiniBatchAssigner withInput(RelNode input, RelDataType prunedRowType) {
    return new StreamPhysicalNativeMiniBatchAssigner(
        getCluster(), getTraitSet(), input, prunedRowType, intervalMs, rowTime);
  }

  @Override
  public ExecNode<?> translateToExecNode() {
    return new NativeMiniBatchAssignerExecNode(
        ShortcutUtils.unwrapTableConfig(this),
        InputProperty.DEFAULT,
        FlinkTypeFactory$.MODULE$.toLogicalRowType(getRowType()),
        getRelDetailedDescription(),
        intervalMs,
        rowTime);
  }
}

