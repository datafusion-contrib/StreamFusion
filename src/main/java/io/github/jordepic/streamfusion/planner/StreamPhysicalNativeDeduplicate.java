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
 * Physical node standing in for a deduplication the native operator runs, the host's {@code
 * ROW_NUMBER() OVER (PARTITION BY … ORDER BY time …) = 1}. Columnar in and out ({@link ColumnarInput}
 * and {@link ColumnarOutput}): the input is shuffled by the partition key (a columnar exchange).
 * Modes: rowtime keep-first (ASC) buffers and, on a watermark, emits each key's minimum-rowtime row
 * insert-only — so it requires an upstream watermark; every other mode (rowtime keep-last, and
 * proctime keep-first/keep-last) emits eagerly per row with no watermark. Keep-last emits a retract
 * changelog; keep-first is insert-only.
 */
public class StreamPhysicalNativeDeduplicate extends SingleRel
    implements StreamPhysicalRel, ColumnarInput, ColumnarOutput {

  private final RelDataType outputRowType;
  private final int[] partitionColumns;
  private final int rowtimeColumn;
  private final boolean keepLast;
  private final boolean generateUpdateBefore;
  private final boolean proctime;

  public StreamPhysicalNativeDeduplicate(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelNode input,
      RelDataType outputRowType,
      int[] partitionColumns,
      int rowtimeColumn,
      boolean keepLast,
      boolean generateUpdateBefore,
      boolean proctime) {
    super(cluster, traitSet, input);
    this.outputRowType = outputRowType;
    this.partitionColumns = partitionColumns;
    this.rowtimeColumn = rowtimeColumn;
    this.keepLast = keepLast;
    this.generateUpdateBefore = generateUpdateBefore;
    this.proctime = proctime;
  }

  @Override
  public boolean requireWatermark() {
    // Only rowtime keep-first releases on a watermark; proctime modes and keep-last emit eagerly.
    return !keepLast && !proctime;
  }

  @Override
  protected RelDataType deriveRowType() {
    return outputRowType;
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new StreamPhysicalNativeDeduplicate(
        getCluster(),
        traitSet,
        inputs.get(0),
        outputRowType,
        partitionColumns,
        rowtimeColumn,
        keepLast,
        generateUpdateBefore,
        proctime);
  }

  @Override
  public ExecNode<?> translateToExecNode() {
    return new NativeDeduplicateExecNode(
        ShortcutUtils.unwrapTableConfig(this),
        InputProperty.DEFAULT,
        FlinkTypeFactory$.MODULE$.toLogicalRowType(getRowType()),
        getRelDetailedDescription(),
        partitionColumns,
        rowtimeColumn,
        keepLast,
        generateUpdateBefore,
        proctime);
  }
}
