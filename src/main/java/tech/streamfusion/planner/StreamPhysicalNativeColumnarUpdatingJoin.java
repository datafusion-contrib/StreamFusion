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
 * Physical node for the regular (non-windowed) INNER updating join, run by the native joiner. Arrow
 * batches in on both inputs and out ({@link ColumnarInput} and {@link ColumnarOutput}); each input is
 * shuffled by its equi-join key (a columnar exchange where the side sits on a columnar producer,
 * otherwise a transpose at the boundary). It preserves the replaced node's output type and traits —
 * including its retracting changelog mode — and needs no watermark (unbounded keyed state).
 */
public class StreamPhysicalNativeColumnarUpdatingJoin extends StreamPhysicalNativeBiRel {

  private final int[] leftKeys;
  private final int[] rightKeys;
  private final int joinType;
  private final RexExpression predicate;
  private final boolean leftJoinKeyUnique;
  private final boolean rightJoinKeyUnique;
  // Per-side TTLs from a STATE_TTL hint on the host join (-1 = no hint for that side); the exec
  // node resolves each against table.exec.state.ttl at translate time, hint winning.
  private final long leftStateTtlHintMillis;
  private final long rightStateTtlHintMillis;

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
      boolean leftJoinKeyUnique,
      boolean rightJoinKeyUnique,
      long leftStateTtlHintMillis,
      long rightStateTtlHintMillis) {
    super(cluster, traitSet, left, right, outputRowType);
    this.leftKeys = leftKeys;
    this.rightKeys = rightKeys;
    this.joinType = joinType;
    this.predicate = predicate;
    this.leftJoinKeyUnique = leftJoinKeyUnique;
    this.rightJoinKeyUnique = rightJoinKeyUnique;
    this.leftStateTtlHintMillis = leftStateTtlHintMillis;
    this.rightStateTtlHintMillis = rightStateTtlHintMillis;
  }

  @Override
  public boolean requireWatermark() {
    return false;
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
        leftJoinKeyUnique,
        rightJoinKeyUnique,
        leftStateTtlHintMillis,
        rightStateTtlHintMillis);
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
        leftJoinKeyUnique,
        rightJoinKeyUnique,
        leftStateTtlHintMillis,
        rightStateTtlHintMillis);
  }
}
