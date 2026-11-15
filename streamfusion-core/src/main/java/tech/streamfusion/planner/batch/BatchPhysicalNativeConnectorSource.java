package tech.streamfusion.planner.batch;

import java.util.List;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory$;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.utils.ShortcutUtils;
import tech.streamfusion.planner.ColumnarOutput;
import tech.streamfusion.planner.NativePlannerExtension;
import tech.streamfusion.planner.common.CommonRowDataToArrowExecNode;
import tech.streamfusion.planner.common.NativePlanDetails;

/** Retains the host scan in the graph while replacing its translated reader boundary. */
final class BatchPhysicalNativeConnectorSource extends BatchPhysicalNativeSingleRel
    implements ColumnarOutput {
  private final NativePlannerExtension.BatchSource source;

  BatchPhysicalNativeConnectorSource(RelNode original, NativePlannerExtension.BatchSource source) {
    this(original, original, source, NativePlanDetails.from(original));
  }

  private BatchPhysicalNativeConnectorSource(
      RelNode original,
      RelNode input,
      NativePlannerExtension.BatchSource source,
      NativePlanDetails details) {
    super(original, input, details);
    this.source = source;
  }

  @Override
  public RelNode copy(RelTraitSet traits, List<RelNode> inputs) {
    var result = new BatchPhysicalNativeConnectorSource(this, inputs.get(0), source, details);
    result.traitSet = traits;
    return result;
  }

  @Override
  public ExecNode<?> translateToExecNode() {
    var config = ShortcutUtils.unwrapTableConfig(this);
    var boundary =
        new CommonRowDataToArrowExecNode(
            config,
            InputProperty.DEFAULT,
            FlinkTypeFactory$.MODULE$.toLogicalRowType(getRowType()),
            getRelDetailedDescription(),
            false,
            null,
            false,
            4096);
    boundary.allowNativeFileSource(source);
    return new BatchNativeExecNode<>(config, boundary);
  }
}
