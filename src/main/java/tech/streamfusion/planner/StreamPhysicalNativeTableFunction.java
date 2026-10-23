package tech.streamfusion.planner;

import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.utils.ShortcutUtils;

/** Keeps JVM lateral application inside an Arrow island. */
public final class StreamPhysicalNativeTableFunction extends StreamPhysicalNativeSingleRel
    implements ColumnarInput, ColumnarOutput {
  private final RexCall invocation;
  private final RexNode condition;
  private final boolean left;
  private final RelDataType correlatedType;
  private final int[] projection;

  StreamPhysicalNativeTableFunction(
      RelOptCluster cluster,
      RelTraitSet traits,
      RelNode input,
      RelDataType output,
      RexCall invocation,
      RexNode condition,
      boolean left) {
    this(
        cluster,
        traits,
        input,
        output,
        invocation,
        condition,
        left,
        output,
        java.util.stream.IntStream.range(0, output.getFieldCount()).toArray());
  }

  private StreamPhysicalNativeTableFunction(
      RelOptCluster cluster,
      RelTraitSet traits,
      RelNode input,
      RelDataType output,
      RexCall invocation,
      RexNode condition,
      boolean left,
      RelDataType correlatedType,
      int[] projection) {
    super(cluster, traits, input, output);
    this.correlatedType = correlatedType;
    this.projection = projection;
    this.invocation = invocation;
    this.condition = condition;
    this.left = left;
  }

  boolean preservesCollections() {
    return !JvmFunctionTypes.scalarRow(getInput().getRowType())
        || !JvmFunctionTypes.scalarRow(correlatedType)
        || invocation.getOperands().stream()
            .anyMatch(arg -> !JvmFunctionTypes.scalar(arg.getType()));
  }

  @Override
  public boolean requireWatermark() {
    return false;
  }

  @Override
  public RelNode copy(RelTraitSet traits, List<RelNode> inputs) {
    return new StreamPhysicalNativeTableFunction(
        getCluster(),
        traits,
        inputs.get(0),
        outputRowType,
        invocation,
        condition,
        left,
        correlatedType,
        projection);
  }

  StreamPhysicalNativeTableFunction withProjection(
      RelTraitSet traits, RelDataType output, int[] columns) {
    int[] combined = java.util.Arrays.stream(columns).map(index -> projection[index]).toArray();
    return new StreamPhysicalNativeTableFunction(
        getCluster(),
        traits,
        getInput(),
        output,
        invocation,
        condition,
        left,
        correlatedType,
        combined);
  }

  @Override
  public ExecNode<?> translateToExecNode() {
    return new NativeTableFunctionExecNode(
        ShortcutUtils.unwrapTableConfig(this),
        FlinkTypeFactory.toLogicalRowType(getInput().getRowType()),
        FlinkTypeFactory.toLogicalRowType(getRowType()),
        FlinkTypeFactory.toLogicalRowType(correlatedType),
        projection,
        invocation,
        condition,
        left,
        preservesCollections(),
        getRelDetailedDescription());
  }
}
