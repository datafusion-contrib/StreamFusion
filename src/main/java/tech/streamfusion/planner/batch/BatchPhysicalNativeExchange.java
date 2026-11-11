package tech.streamfusion.planner.batch;

import java.util.List;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory$;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.utils.ShortcutUtils;
import tech.streamfusion.planner.ColumnarInput;
import tech.streamfusion.planner.ColumnarOutput;
import tech.streamfusion.planner.common.NativePlanDetails;

/** Blocking Arrow distributions; Flink owns persisted frames and stage replay. */
final class BatchPhysicalNativeExchange extends BatchPhysicalNativeSingleRel
    implements ColumnarInput, ColumnarOutput {
  private final int[] keys;
  private boolean broadcast;

  BatchPhysicalNativeExchange(RelNode original, RelNode input) {
    this(
        original,
        input,
        ((org.apache.flink.table.planner.plan.nodes.physical.batch.BatchPhysicalExchange) original)
            .getDistribution().getKeys().stream().mapToInt(Integer::intValue).toArray());
    broadcast =
        ((org.apache.flink.table.planner.plan.nodes.physical.batch.BatchPhysicalExchange) original)
                .getDistribution()
                .getType()
            == org.apache.calcite.rel.RelDistribution.Type.BROADCAST_DISTRIBUTED;
  }

  BatchPhysicalNativeExchange(RelNode original, RelNode input, int[] keys) {
    this(original, input, keys, NativePlanDetails.from(original));
  }

  boolean partitions(int[] expected) {
    return java.util.Arrays.equals(keys, expected);
  }

  private BatchPhysicalNativeExchange(
      RelNode original, RelNode input, int[] keys, NativePlanDetails details) {
    super(original, input, details);
    this.keys = keys.clone();
  }

  @Override
  public RelNode copy(RelTraitSet traits, List<RelNode> inputs) {
    var copy = new BatchPhysicalNativeExchange(this, inputs.get(0), keys, details);
    copy.traitSet = traits;
    copy.broadcast = broadcast;
    return copy;
  }

  @Override
  public ExecNode<?> translateToExecNode() {
    if (broadcast)
      return new BatchBroadcastExchangeExecNode(
          ShortcutUtils.unwrapTableConfig(this),
          FlinkTypeFactory$.MODULE$.toLogicalRowType(getRowType()),
          getRelDetailedDescription());
    if (keys.length != 0)
      return new BatchHashExchangeExecNode(
          ShortcutUtils.unwrapTableConfig(this),
          FlinkTypeFactory$.MODULE$.toLogicalRowType(getRowType()),
          getRelDetailedDescription(),
          keys);
    return new BatchSingletonExchangeExecNode(
        ShortcutUtils.unwrapTableConfig(this),
        FlinkTypeFactory$.MODULE$.toLogicalRowType(getRowType()),
        getRelDetailedDescription());
  }
}
