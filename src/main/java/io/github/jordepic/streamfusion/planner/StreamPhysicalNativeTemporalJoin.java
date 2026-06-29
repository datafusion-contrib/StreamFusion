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
 * Physical node standing in for an event-time temporal table join the native operator runs. Columnar
 * on both inputs and on its output ({@link ColumnarInput} and {@link ColumnarOutput}): each input is
 * shuffled by its equi-join key (a columnar exchange) and the join emits Arrow batches of the matched
 * rows (probe columns then build columns). Requires a watermark — the combined input watermark
 * resolves buffered probe rows against the build version valid at their time and drives state cleanup.
 */
public class StreamPhysicalNativeTemporalJoin extends BiRel
    implements StreamPhysicalRel, ColumnarInput, ColumnarOutput {

  private final RelDataType outputRowType;
  private final int[] leftKeys;
  private final int[] rightKeys;
  private final int leftTime;
  private final int rightTime;
  private final int joinType;
  private final RexExpression predicate;

  public StreamPhysicalNativeTemporalJoin(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelNode left,
      RelNode right,
      RelDataType outputRowType,
      int[] leftKeys,
      int[] rightKeys,
      int leftTime,
      int rightTime,
      int joinType,
      RexExpression predicate) {
    super(cluster, traitSet, left, right);
    this.outputRowType = outputRowType;
    this.leftKeys = leftKeys;
    this.rightKeys = rightKeys;
    this.leftTime = leftTime;
    this.rightTime = rightTime;
    this.joinType = joinType;
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
    return new StreamPhysicalNativeTemporalJoin(
        getCluster(),
        traitSet,
        inputs.get(0),
        inputs.get(1),
        outputRowType,
        leftKeys,
        rightKeys,
        leftTime,
        rightTime,
        joinType,
        predicate);
  }

  @Override
  public ExecNode<?> translateToExecNode() {
    return new NativeTemporalJoinExecNode(
        ShortcutUtils.unwrapTableConfig(this),
        InputProperty.DEFAULT,
        InputProperty.DEFAULT,
        FlinkTypeFactory$.MODULE$.toLogicalRowType(getRowType()),
        getRelDetailedDescription(),
        leftKeys,
        rightKeys,
        leftTime,
        rightTime,
        joinType,
        FlinkTypeFactory$.MODULE$.toLogicalRowType(getLeft().getRowType()),
        FlinkTypeFactory$.MODULE$.toLogicalRowType(getRight().getRowType()),
        predicate);
  }
}
