package io.github.jordepic.streamfusion.planner;

import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory$;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalRel;
import org.apache.flink.table.planner.utils.ShortcutUtils;

/**
 * Physical node standing in for a {@link
 * org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalWindowTableFunction} the
 * native operator runs: an event-time TUMBLE/HOP/CUMULATE windowing TVF over a local-time-zone
 * rowtime. Columnar on its input and output ({@link ColumnarInput} and {@link ColumnarOutput}): it
 * assigns each Arrow row to its window(s) and emits the input columns (fanned out one copy per window
 * for hopping/cumulative) with window_start/window_end/window_time appended. It does no buffering, so
 * watermarks pass straight through; it requires an upstream watermark because its rowtime windowing
 * is what the downstream window join/aggregate triggers on.
 */
public class StreamPhysicalNativeWindowTableFunction extends SingleRel
    implements StreamPhysicalRel, ColumnarInput, ColumnarOutput {

  private final RelDataType outputRowType;
  private final int timeColumn;
  private final long windowMillis;
  private final long slideMillis;
  private final boolean cumulative;
  private final boolean proctime;

  public StreamPhysicalNativeWindowTableFunction(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelNode input,
      RelDataType outputRowType,
      int timeColumn,
      long windowMillis,
      long slideMillis,
      boolean cumulative,
      boolean proctime) {
    super(cluster, traitSet, input);
    this.outputRowType = outputRowType;
    this.timeColumn = timeColumn;
    this.windowMillis = windowMillis;
    this.slideMillis = slideMillis;
    this.cumulative = cumulative;
    this.proctime = proctime;
  }

  @Override
  public boolean requireWatermark() {
    return !proctime;
  }

  @Override
  protected RelDataType deriveRowType() {
    return outputRowType;
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new StreamPhysicalNativeWindowTableFunction(
        getCluster(),
        traitSet,
        inputs.get(0),
        outputRowType,
        timeColumn,
        windowMillis,
        slideMillis,
        cumulative,
        proctime);
  }

  @Override
  public ExecNode<?> translateToExecNode() {
    return new NativeWindowTableFunctionExecNode(
        ShortcutUtils.unwrapTableConfig(this),
        InputProperty.DEFAULT,
        FlinkTypeFactory$.MODULE$.toLogicalRowType(getRowType()),
        getRelDetailedDescription(),
        timeColumn,
        windowMillis,
        slideMillis,
        cumulative,
        proctime);
  }
}
