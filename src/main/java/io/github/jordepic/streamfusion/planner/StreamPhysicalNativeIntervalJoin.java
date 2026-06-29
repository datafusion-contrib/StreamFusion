package io.github.jordepic.streamfusion.planner;

import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.BiRel;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory$;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalRel;
import org.apache.flink.table.planner.utils.ShortcutUtils;

/**
 * Physical node standing in for an event-time INNER interval join the native operator runs.
 * Columnar on both inputs and on its output ({@link ColumnarInput} and {@link ColumnarOutput}): each
 * input is shuffled by its equi-join key (a columnar exchange) and the join emits Arrow batches of
 * the matched pairs (left columns then right columns), so it rides the keyed shuffle with no
 * transpose. Requires a watermark — the combined input watermark drives state eviction.
 */
public class StreamPhysicalNativeIntervalJoin extends BiRel
    implements StreamPhysicalRel, ColumnarInput, ColumnarOutput {

  private final RelDataType outputRowType;
  private final int[] leftKeys;
  private final int[] rightKeys;
  private final int leftTime;
  private final int rightTime;
  private final long lowerMillis;
  private final long upperMillis;
  private final int joinType;
  private final RexExpression predicate;
  private final boolean proctime;

  public StreamPhysicalNativeIntervalJoin(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelNode left,
      RelNode right,
      RelDataType outputRowType,
      int[] leftKeys,
      int[] rightKeys,
      int leftTime,
      int rightTime,
      long lowerMillis,
      long upperMillis,
      int joinType,
      RexExpression predicate,
      boolean proctime) {
    super(cluster, traitSet, left, right);
    this.outputRowType = outputRowType;
    this.leftKeys = leftKeys;
    this.rightKeys = rightKeys;
    this.leftTime = leftTime;
    this.rightTime = rightTime;
    this.lowerMillis = lowerMillis;
    this.upperMillis = upperMillis;
    this.joinType = joinType;
    this.predicate = predicate;
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
    return new StreamPhysicalNativeIntervalJoin(
        getCluster(),
        traitSet,
        inputs.get(0),
        inputs.get(1),
        outputRowType,
        leftKeys,
        rightKeys,
        leftTime,
        rightTime,
        lowerMillis,
        upperMillis,
        joinType,
        predicate,
        proctime);
  }

  @Override
  public ExecNode<?> translateToExecNode() {
    return new NativeIntervalJoinExecNode(
        ShortcutUtils.unwrapTableConfig(this),
        InputProperty.DEFAULT,
        InputProperty.DEFAULT,
        FlinkTypeFactory$.MODULE$.toLogicalRowType(getRowType()),
        getRelDetailedDescription(),
        leftKeys,
        rightKeys,
        leftTime,
        rightTime,
        lowerMillis,
        upperMillis,
        joinType,
        FlinkTypeFactory$.MODULE$.toLogicalRowType(getLeft().getRowType()),
        FlinkTypeFactory$.MODULE$.toLogicalRowType(getRight().getRowType()),
        predicate,
        proctime);
  }
}
