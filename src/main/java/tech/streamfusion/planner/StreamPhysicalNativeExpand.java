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
 * org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalExpand} the native operator
 * runs: a GROUPING SETS / CUBE / ROLLUP expansion. Columnar in and out ({@link ColumnarInput} and
 * {@link ColumnarOutput}): it fans each input row out to one row per grouping set — copying the
 * grouped-in columns, nulling the grouped-out ones, and stamping the per-set expand id — carrying the
 * {@code $row_kind$} tag through (changelog-transparent), so it sits in the columnar island between
 * its (native) input and the downstream native GROUP BY over the keys plus the expand-id column.
 */
public class StreamPhysicalNativeExpand extends StreamPhysicalNativeSingleRel
    implements ColumnarInput, ColumnarOutput {

  private final int numExpandRows;
  private final int numOutputColumns;
  private final int expandIdIndex;
  private final boolean expandIdIsLong;
  private final int[] copyIndices;
  private final long[] expandIdValues;

  public StreamPhysicalNativeExpand(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelNode input,
      RelDataType outputRowType,
      int numExpandRows,
      int numOutputColumns,
      int expandIdIndex,
      boolean expandIdIsLong,
      int[] copyIndices,
      long[] expandIdValues) {
    super(cluster, traitSet, input, outputRowType);
    this.numExpandRows = numExpandRows;
    this.numOutputColumns = numOutputColumns;
    this.expandIdIndex = expandIdIndex;
    this.expandIdIsLong = expandIdIsLong;
    this.copyIndices = copyIndices;
    this.expandIdValues = expandIdValues;
  }

  @Override
  public boolean requireWatermark() {
    return false;
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new StreamPhysicalNativeExpand(
        getCluster(),
        traitSet,
        inputs.get(0),
        outputRowType,
        numExpandRows,
        numOutputColumns,
        expandIdIndex,
        expandIdIsLong,
        copyIndices,
        expandIdValues);
  }

  @Override
  public ExecNode<?> translateToExecNode() {
    return new NativeExpandExecNode(
        ShortcutUtils.unwrapTableConfig(this),
        InputProperty.DEFAULT,
        FlinkTypeFactory$.MODULE$.toLogicalRowType(getRowType()),
        getRelDetailedDescription(),
        numExpandRows,
        numOutputColumns,
        expandIdIndex,
        expandIdIsLong,
        copyIndices,
        expandIdValues);
  }
}

