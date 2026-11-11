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

/** Keeps representation-sensitive builtin UNNEST evaluation inside an Arrow island. */
public final class StreamPhysicalNativeGeneratedUnnest extends StreamPhysicalNativeSingleRel
    implements ColumnarInput, ColumnarOutput {
  private final RexCall invocation;
  private final RexNode condition;
  private final boolean left;
  private final RelDataType correlatedType;
  private final int[] projection;
  private final RelDataType evaluationInputType;
  private final int[] inputMapping;

  StreamPhysicalNativeGeneratedUnnest(
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
        java.util.stream.IntStream.range(0, output.getFieldCount()).toArray(),
        input.getRowType(),
        null);
  }

  private StreamPhysicalNativeGeneratedUnnest(
      RelOptCluster cluster,
      RelTraitSet traits,
      RelNode input,
      RelDataType output,
      RexCall invocation,
      RexNode condition,
      boolean left,
      RelDataType correlatedType,
      int[] projection,
      RelDataType evaluationInputType,
      int[] inputMapping) {
    super(cluster, traits, input, output);
    this.evaluationInputType = evaluationInputType;
    this.inputMapping = inputMapping;
    this.correlatedType = correlatedType;
    this.projection = projection;
    this.invocation = invocation;
    this.condition = condition;
    this.left = left;
  }

  @Override
  public boolean carriesStringValues() {
    return true;
  }

  @Override
  public boolean requireWatermark() {
    return false;
  }

  @Override
  public RelNode copy(RelTraitSet traits, List<RelNode> inputs) {
    return new StreamPhysicalNativeGeneratedUnnest(
        getCluster(),
        traits,
        inputs.get(0),
        outputRowType,
        invocation,
        condition,
        left,
        correlatedType,
        projection,
        evaluationInputType,
        inputMapping);
  }

  StreamPhysicalNativeGeneratedUnnest withProjection(
      RelTraitSet traits, RelDataType output, int[] columns) {
    int[] combined = java.util.Arrays.stream(columns).map(index -> projection[index]).toArray();
    var pruned = inputMapping == null ? pruneInput(combined) : null;
    return new StreamPhysicalNativeGeneratedUnnest(
        getCluster(),
        traits,
        pruned == null ? getInput() : pruned.input,
        output,
        invocation,
        condition,
        left,
        correlatedType,
        combined,
        evaluationInputType,
        pruned == null ? inputMapping : pruned.mapping);
  }

  private static final class Pruned {
    final RelNode input;
    final int[] mapping;

    Pruned(RelNode input, int[] mapping) {
      this.input = input;
      this.mapping = mapping;
    }
  }

  private Pruned pruneInput(int[] selected) {
    var needed = new java.util.TreeSet<Integer>();
    int arity = evaluationInputType.getFieldCount();
    for (int index : selected) if (index < arity) needed.add(index);
    var reads =
        new org.apache.calcite.rex.RexVisitorImpl<Void>(true) {
          @Override
          public Void visitFieldAccess(org.apache.calcite.rex.RexFieldAccess field) {
            if (field.getReferenceExpr() instanceof org.apache.calcite.rex.RexCorrelVariable)
              needed.add(field.getField().getIndex());
            return super.visitFieldAccess(field);
          }
        };
    invocation.accept(reads);
    if (condition != null) condition.accept(reads);
    if (needed.size() == arity) return null;
    var builder = getCluster().getTypeFactory().builder();
    int[] mapping = new int[arity];
    java.util.Arrays.fill(mapping, -1);
    for (int index : needed) {
      var field = evaluationInputType.getFieldList().get(index);
      mapping[index] = builder.getFieldCount();
      builder.add(field.getName(), field.getType());
    }
    RelNode pruned = pruneBoundary(getInput(), builder.build());
    return pruned == null ? null : new Pruned(pruned, mapping);
  }

  private static RelNode pruneBoundary(RelNode input, RelDataType prunedType) {
    if (input instanceof StreamPhysicalNativeDropUpdateBefore) {
      RelNode below = pruneBoundary(input.getInput(0), prunedType);
      return below == null ? null : input.copy(input.getTraitSet(), List.of(below));
    }
    if (input instanceof StreamPhysicalNativeMiniBatchAssigner) {
      RelNode below = pruneBoundary(input.getInput(0), prunedType);
      return below == null
          ? null
          : ((StreamPhysicalNativeMiniBatchAssigner) input).withInput(below, prunedType);
    }
    if (input instanceof ColumnarOutput) return null;
    boolean kinds =
        input instanceof org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalRel
            && !org.apache.flink.table.planner.plan.utils.ChangelogPlanUtils.isInsertOnly(
                (org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalRel)
                    input);
    return new StreamPhysicalRowDataToArrow(
        input.getCluster(), input.getTraitSet(), input, kinds, prunedType, true);
  }

  @Override
  public ExecNode<?> translateToExecNode() {
    return new NativeGeneratedUnnestExecNode(
        ShortcutUtils.unwrapTableConfig(this),
        FlinkTypeFactory.toLogicalRowType(getInput().getRowType()),
        FlinkTypeFactory.toLogicalRowType(evaluationInputType),
        inputMapping,
        FlinkTypeFactory.toLogicalRowType(getRowType()),
        FlinkTypeFactory.toLogicalRowType(correlatedType),
        projection,
        invocation,
        condition,
        left,
        getRelDetailedDescription());
  }
}
