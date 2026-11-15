package tech.streamfusion.planner.stream;

import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.utils.AggregateInfoList;
import org.apache.flink.table.planner.utils.ShortcutUtils;
import org.apache.flink.table.types.logical.RowType;
import tech.streamfusion.planner.ColumnarInput;
import tech.streamfusion.planner.ColumnarOutput;

final class StreamPhysicalNativeJvmTwoPhaseGroup extends StreamPhysicalNativeSingleRel
    implements ColumnarInput, ColumnarOutput {
  private final RowType originalInput;
  private final RowType partial;
  private final int[] grouping;
  private final boolean retract;
  private final boolean updateBefore;
  private final Long ttl;
  private final AggregateInfoList localInfo;
  private final AggregateInfoList globalInfo;

  StreamPhysicalNativeJvmTwoPhaseGroup(
      RelOptCluster cluster,
      RelTraitSet traits,
      RelNode input,
      RelDataType output,
      RowType originalInput,
      RowType partial,
      int[] grouping,
      boolean retract,
      boolean updateBefore,
      Long ttl,
      AggregateInfoList localInfo,
      AggregateInfoList globalInfo) {
    super(cluster, traits, input, output);
    this.originalInput = originalInput;
    this.partial = partial;
    this.grouping = grouping;
    this.retract = retract;
    this.updateBefore = updateBefore;
    this.ttl = ttl;
    this.localInfo = localInfo;
    this.globalInfo = globalInfo;
  }

  @Override
  public RelWriter explainTerms(RelWriter writer) {
    return super.explainTerms(writer)
        .item("phase", globalInfo == null ? "local" : "global")
        .item("grouping", java.util.Arrays.toString(grouping));
  }

  @Override
  public boolean requireWatermark() {
    return false;
  }

  @Override
  public RelNode copy(RelTraitSet traits, List<RelNode> inputs) {
    return new StreamPhysicalNativeJvmTwoPhaseGroup(
        getCluster(),
        traits,
        inputs.get(0),
        outputRowType,
        originalInput,
        partial,
        grouping,
        retract,
        updateBefore,
        ttl,
        localInfo,
        globalInfo);
  }

  @Override
  public ExecNode<?> translateToExecNode() {
    return new NativeJvmTwoPhaseGroupExecNode(
        ShortcutUtils.unwrapTableConfig(this),
        FlinkTypeFactory.toLogicalRowType(getInput().getRowType()),
        FlinkTypeFactory.toLogicalRowType(getRowType()),
        originalInput,
        partial,
        grouping,
        retract,
        updateBefore,
        ttl,
        localInfo,
        globalInfo,
        getRelDetailedDescription());
  }
}
