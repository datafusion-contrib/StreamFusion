package tech.streamfusion.planner.stream;

import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory$;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.utils.ShortcutUtils;
import tech.streamfusion.hudi.HudiSinkPlan;
import tech.streamfusion.planner.ColumnarInput;
import tech.streamfusion.planner.common.NativePlanDetails;

public final class StreamPhysicalNativeHudiSink extends StreamPhysicalNativeSingleRel
    implements ColumnarInput {
  private final HudiSinkPlan plan;
  private final int[] materializationKeys;
  private final int[] inputUpsertKeys;

  StreamPhysicalNativeHudiSink(
      RelOptCluster cluster,
      RelTraitSet traits,
      RelNode input,
      RelDataType output,
      HudiSinkPlan plan,
      int[] materializationKeys,
      int[] inputUpsertKeys,
      NativePlanDetails details) {
    super(cluster, traits, input, output, details);
    this.plan = plan;
    this.materializationKeys = materializationKeys;
    this.inputUpsertKeys = inputUpsertKeys;
  }

  @Override
  public org.apache.calcite.rel.RelWriter explainTerms(org.apache.calcite.rel.RelWriter writer) {
    return super.explainTerms(writer)
        .item("writeMode", plan.writeMode())
        .itemIf("materializer", "native", materializationKeys != null);
  }

  @Override
  public boolean requireWatermark() {
    return false;
  }

  @Override
  public RelNode copy(RelTraitSet traits, List<RelNode> inputs) {
    return new StreamPhysicalNativeHudiSink(
        getCluster(),
        traits,
        inputs.get(0),
        outputRowType,
        plan,
        materializationKeys,
        inputUpsertKeys,
        planDetails);
  }

  @Override
  public ExecNode<?> translateToExecNode() {
    return new NativeHudiSinkExecNode(
        ShortcutUtils.unwrapTableConfig(this),
        InputProperty.DEFAULT,
        FlinkTypeFactory$.MODULE$.toLogicalRowType(getRowType()),
        getRelDetailedDescription(),
        plan,
        materializationKeys,
        inputUpsertKeys);
  }
}
