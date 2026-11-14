package tech.streamfusion.planner.stream;

import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory$;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.utils.ChangelogPlanUtils;
import org.apache.flink.table.planner.utils.ShortcutUtils;
import tech.streamfusion.planner.ColumnarInput;
import tech.streamfusion.planner.ColumnarOutput;

/** Removes UPDATE_BEFORE without leaving the columnar island. */
public class StreamPhysicalNativeDropUpdateBefore extends StreamPhysicalNativeSingleRel
    implements ColumnarInput, ColumnarOutput {
  public StreamPhysicalNativeDropUpdateBefore(
      RelOptCluster cluster, RelTraitSet traits, RelNode input) {
    super(cluster, traits, input, input.getRowType());
  }

  @Override
  public boolean requireWatermark() {
    return false;
  }

  @Override
  public RelNode copy(RelTraitSet traits, List<RelNode> inputs) {
    return new StreamPhysicalNativeDropUpdateBefore(getCluster(), traits, inputs.get(0));
  }

  @Override
  public ExecNode<?> translateToExecNode() {
    if (ChangelogPlanUtils.generateUpdateBefore(this)) {
      throw new IllegalStateException(
          "DropUpdateBefore is required to emit UPDATE_BEFORE messages");
    }
    return new NativeDropUpdateBeforeExecNode(
        ShortcutUtils.unwrapTableConfig(this),
        InputProperty.DEFAULT,
        FlinkTypeFactory$.MODULE$.toLogicalRowType(getRowType()),
        getRelDetailedDescription());
  }
}
