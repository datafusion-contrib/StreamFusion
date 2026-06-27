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
 * Physical node standing in for a keep-first deduplication the native operator runs: a {@code
 * ROW_NUMBER() OVER (PARTITION BY … ORDER BY rowtime ASC) = 1} the host plans as a row-time
 * deduplicate. Columnar in and out ({@link ColumnarInput} and {@link ColumnarOutput}): the input is
 * shuffled by the partition key (a columnar exchange) and, on a watermark, the operator emits each
 * key's minimum-rowtime row (insert-only) and keeps the rest. Requires an upstream watermark — the
 * watermark is what releases a key's first row.
 */
public class StreamPhysicalNativeDeduplicate extends SingleRel
    implements StreamPhysicalRel, ColumnarInput, ColumnarOutput {

  private final RelDataType outputRowType;
  private final int[] partitionColumns;
  private final int rowtimeColumn;

  public StreamPhysicalNativeDeduplicate(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelNode input,
      RelDataType outputRowType,
      int[] partitionColumns,
      int rowtimeColumn) {
    super(cluster, traitSet, input);
    this.outputRowType = outputRowType;
    this.partitionColumns = partitionColumns;
    this.rowtimeColumn = rowtimeColumn;
  }

  @Override
  public boolean requireWatermark() {
    return true;
  }

  @Override
  protected RelDataType deriveRowType() {
    return outputRowType;
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new StreamPhysicalNativeDeduplicate(
        getCluster(), traitSet, inputs.get(0), outputRowType, partitionColumns, rowtimeColumn);
  }

  @Override
  public ExecNode<?> translateToExecNode() {
    return new NativeDeduplicateExecNode(
        ShortcutUtils.unwrapTableConfig(this),
        InputProperty.DEFAULT,
        FlinkTypeFactory$.MODULE$.toLogicalRowType(getRowType()),
        getRelDetailedDescription(),
        partitionColumns,
        rowtimeColumn);
  }
}
