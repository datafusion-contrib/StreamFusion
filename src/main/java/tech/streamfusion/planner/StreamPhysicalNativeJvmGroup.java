package tech.streamfusion.planner;

import java.util.Arrays;
import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.utils.ShortcutUtils;
import scala.collection.Seq;

public final class StreamPhysicalNativeJvmGroup extends StreamPhysicalNativeSingleRel
    implements ColumnarInput, ColumnarOutput {
  private final int[] grouping;
  private final Seq<AggregateCall> calls;
  private final boolean[] retracts;
  private final boolean retract;
  private final boolean updateBefore;

  StreamPhysicalNativeJvmGroup(
      RelOptCluster cluster,
      RelTraitSet traits,
      RelNode input,
      RelDataType output,
      int[] grouping,
      Seq<AggregateCall> calls,
      boolean[] retracts,
      boolean retract,
      boolean updateBefore) {
    super(cluster, traits, input, output);
    this.grouping = grouping;
    this.calls = calls;
    this.retracts = retracts;
    this.retract = retract;
    this.updateBefore = updateBefore;
  }

  @Override
  public RelWriter explainTerms(RelWriter writer) {
    return super.explainTerms(writer).item("grouping", Arrays.toString(grouping));
  }

  @Override
  public boolean requireWatermark() {
    return false;
  }

  @Override
  public RelNode copy(RelTraitSet traits, List<RelNode> inputs) {
    return new StreamPhysicalNativeJvmGroup(
        getCluster(),
        traits,
        inputs.get(0),
        outputRowType,
        grouping,
        calls,
        retracts,
        retract,
        updateBefore);
  }

  @Override
  public ExecNode<?> translateToExecNode() {
    return new NativeJvmGroupExecNode(
        ShortcutUtils.unwrapTableConfig(this),
        FlinkTypeFactory.toLogicalRowType(getInput().getRowType()),
        FlinkTypeFactory.toLogicalRowType(getRowType()),
        grouping,
        calls,
        retracts,
        retract,
        updateBefore,
        getRelDetailedDescription());
  }
}
