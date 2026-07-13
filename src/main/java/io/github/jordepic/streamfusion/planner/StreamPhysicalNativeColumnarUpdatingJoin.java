package io.github.jordepic.streamfusion.planner;

import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.BiRel;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory$;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalRel;
import org.apache.flink.table.planner.utils.ShortcutUtils;

/**
 * Physical node for the regular (non-windowed) INNER updating join, run by the native joiner. Arrow
 * batches in on both inputs and out ({@link ColumnarInput} and {@link ColumnarOutput}); each input is
 * shuffled by its equi-join key (a columnar exchange where the side sits on a columnar producer,
 * otherwise a transpose at the boundary). It preserves the replaced node's output type and traits —
 * including its retracting changelog mode — and needs no watermark (unbounded keyed state).
 */
public class StreamPhysicalNativeColumnarUpdatingJoin extends BiRel
    implements StreamPhysicalRel, ColumnarInput, ColumnarOutput {

  private final RelDataType outputRowType;
  private final int[] leftKeys;
  private final int[] rightKeys;
  private final int joinType;
  private final RexExpression predicate;
  private final boolean bothJoinKeysUnique;

  public StreamPhysicalNativeColumnarUpdatingJoin(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelNode left,
      RelNode right,
      RelDataType outputRowType,
      int[] leftKeys,
      int[] rightKeys,
      int joinType,
      RexExpression predicate,
      boolean bothJoinKeysUnique) {
    super(cluster, traitSet, left, right);
    this.outputRowType = outputRowType;
    this.leftKeys = leftKeys;
    this.rightKeys = rightKeys;
    this.joinType = joinType;
    this.predicate = predicate;
    this.bothJoinKeysUnique = bothJoinKeysUnique;
  }

  @Override
  public boolean requireWatermark() {
    return false;
  }

  @Override
  protected RelDataType deriveRowType() {
    return outputRowType;
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new StreamPhysicalNativeColumnarUpdatingJoin(
        getCluster(),
        traitSet,
        inputs.get(0),
        inputs.get(1),
        outputRowType,
        leftKeys,
        rightKeys,
        joinType,
        predicate,
        bothJoinKeysUnique);
  }

  @Override
  public ExecNode<?> translateToExecNode() {
    return new NativeColumnarUpdatingJoinExecNode(
        ShortcutUtils.unwrapTableConfig(this),
        InputProperty.DEFAULT,
        InputProperty.DEFAULT,
        FlinkTypeFactory$.MODULE$.toLogicalRowType(getRowType()),
        getRelDetailedDescription(),
        leftKeys,
        rightKeys,
        joinType,
        FlinkTypeFactory$.MODULE$.toLogicalRowType(getLeft().getRowType()),
        FlinkTypeFactory$.MODULE$.toLogicalRowType(getRight().getRowType()),
        predicate,
        FlinkKeyGroupUtils.timestampPrecisions(getLeft().getRowType(), leftKeys),
        bothJoinKeysUnique);
  }

  /** Digest-only reuse barrier — see {@link NativeRelDigests}. */
  private final long reuseBarrier = NativeRelDigests.nextId();

  @Override
  public RelWriter explainTerms(RelWriter pw) {
    return NativeRelDigests.withBarrier(super.explainTerms(pw), reuseBarrier);
  }
}
