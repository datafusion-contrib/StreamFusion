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
 * Physical node standing in for an event-time INNER window join the native operator runs. Columnar
 * on both inputs and on its output ({@link ColumnarInput} and {@link ColumnarOutput}): each input is
 * shuffled by its equi-join key (a columnar exchange) and the join emits Arrow batches of the matched
 * pairs (left columns then right columns). Requires a watermark — the combined input watermark closes
 * windows and drives state eviction.
 */
public class StreamPhysicalNativeWindowJoin extends BiRel
    implements StreamPhysicalRel, ColumnarInput, ColumnarOutput {

  private final RelDataType outputRowType;
  private final int[] leftKeys;
  private final int[] rightKeys;
  private final int leftWindowStart;
  private final int leftWindowEnd;
  private final int rightWindowStart;
  private final int rightWindowEnd;
  private final RexExpression predicate;

  public StreamPhysicalNativeWindowJoin(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelNode left,
      RelNode right,
      RelDataType outputRowType,
      int[] leftKeys,
      int[] rightKeys,
      int leftWindowStart,
      int leftWindowEnd,
      int rightWindowStart,
      int rightWindowEnd,
      RexExpression predicate) {
    super(cluster, traitSet, left, right);
    this.outputRowType = outputRowType;
    this.leftKeys = leftKeys;
    this.rightKeys = rightKeys;
    this.leftWindowStart = leftWindowStart;
    this.leftWindowEnd = leftWindowEnd;
    this.rightWindowStart = rightWindowStart;
    this.rightWindowEnd = rightWindowEnd;
    this.predicate = predicate;
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
    return new StreamPhysicalNativeWindowJoin(
        getCluster(),
        traitSet,
        inputs.get(0),
        inputs.get(1),
        outputRowType,
        leftKeys,
        rightKeys,
        leftWindowStart,
        leftWindowEnd,
        rightWindowStart,
        rightWindowEnd,
        predicate);
  }

  @Override
  public ExecNode<?> translateToExecNode() {
    return new NativeWindowJoinExecNode(
        ShortcutUtils.unwrapTableConfig(this),
        InputProperty.DEFAULT,
        InputProperty.DEFAULT,
        FlinkTypeFactory$.MODULE$.toLogicalRowType(getRowType()),
        getRelDetailedDescription(),
        leftKeys,
        rightKeys,
        leftWindowStart,
        leftWindowEnd,
        rightWindowStart,
        rightWindowEnd,
        predicate);
  }
}
