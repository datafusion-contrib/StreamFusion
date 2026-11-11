package tech.streamfusion.planner;

import java.util.List;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory$;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.utils.ShortcutUtils;

final class BatchPhysicalNativeAggregate extends BatchPhysicalNativeSingleRel
    implements ColumnarInput, ColumnarOutput {
  private final BatchAggregateSpec specification;

  BatchPhysicalNativeAggregate(RelNode original, RelNode input, BatchAggregateSpec specification) {
    this(original, input, specification, NativePlanDetails.from(original));
  }

  private BatchPhysicalNativeAggregate(
      RelNode original,
      RelNode input,
      BatchAggregateSpec specification,
      NativePlanDetails details) {
    super(original, input, details);
    this.specification = specification;
  }

  @Override
  public RelNode copy(RelTraitSet traits, List<RelNode> inputs) {
    var copy = new BatchPhysicalNativeAggregate(this, inputs.get(0), specification, details);
    copy.traitSet = traits;
    return copy;
  }

  @Override
  public ExecNode<?> translateToExecNode() {
    // HASH here would make Flink's graph processor insert a RowData exchange. Keyed routing is
    // already enforced by the explicit Arrow exchange; retain its physical relational trait.
    return new BatchAggregateExecNode(
        ShortcutUtils.unwrapTableConfig(this),
        FlinkTypeFactory$.MODULE$.toLogicalRowType(getInput().getRowType()),
        FlinkTypeFactory$.MODULE$.toLogicalRowType(getRowType()),
        getRelDetailedDescription(),
        specification,
        specification.finalOutput
            ? InputProperty.builder()
                .requiredDistribution(
                    specification.keys.length == 0
                        ? InputProperty.SINGLETON_DISTRIBUTION
                        : InputProperty.ANY_DISTRIBUTION)
                .damBehavior(InputProperty.DamBehavior.END_INPUT)
                .build()
            : inputProperty(InputProperty.DamBehavior.END_INPUT));
  }
}
