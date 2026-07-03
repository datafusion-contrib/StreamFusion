package io.github.jordepic.streamfusion.planner;

import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory$;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalRel;
import org.apache.flink.table.planner.plan.utils.FlinkRexUtil;
import org.apache.flink.table.planner.plan.utils.FunctionCallUtil;
import org.apache.flink.table.planner.utils.ShortcutUtils;

/**
 * Physical node standing in for a processing-time lookup join the native operator runs. Columnar on
 * its single (probe) input and on its output ({@link ColumnarInput} / {@link ColumnarOutput}); the
 * dimension table is not an input but a lookup the operator performs per row through Flink's own
 * generated join runner (key building, pre-filter, dimension-side calc, residual condition). Keeping
 * this operator inside the island lets the probe-side Calc/source stay native rather than the whole
 * query falling back to a rowwise plan around the lookup.
 */
public class StreamPhysicalNativeLookupJoin extends SingleRel
    implements StreamPhysicalRel, ColumnarInput, ColumnarOutput {

  private final RelDataType outputRowType;
  private final RelOptTable temporalTable;
  private final Map<Integer, FunctionCallUtil.FunctionParam> lookupKeys;
  private final @Nullable RexProgram calcOnTemporalTable;
  private final @Nullable RexNode preFilterCondition;
  private final @Nullable RexNode remainingJoinCondition;
  private final boolean leftOuterJoin;
  private final @Nullable FunctionCallUtil.AsyncOptions asyncOptions;

  public StreamPhysicalNativeLookupJoin(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelNode input,
      RelDataType outputRowType,
      RelOptTable temporalTable,
      Map<Integer, FunctionCallUtil.FunctionParam> lookupKeys,
      @Nullable RexProgram calcOnTemporalTable,
      @Nullable RexNode preFilterCondition,
      @Nullable RexNode remainingJoinCondition,
      boolean leftOuterJoin,
      @Nullable FunctionCallUtil.AsyncOptions asyncOptions) {
    super(cluster, traitSet, input);
    this.outputRowType = outputRowType;
    this.temporalTable = temporalTable;
    this.lookupKeys = lookupKeys;
    this.calcOnTemporalTable = calcOnTemporalTable;
    this.preFilterCondition = preFilterCondition;
    this.remainingJoinCondition = remainingJoinCondition;
    this.leftOuterJoin = leftOuterJoin;
    this.asyncOptions = asyncOptions;
  }

  @Override
  public boolean requireWatermark() {
    return false;
  }

  @Override
  protected RelDataType deriveRowType() {
    return outputRowType;
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new StreamPhysicalNativeLookupJoin(
        getCluster(),
        traitSet,
        inputs.get(0),
        outputRowType,
        temporalTable,
        lookupKeys,
        calcOnTemporalTable,
        preFilterCondition,
        remainingJoinCondition,
        leftOuterJoin,
        asyncOptions);
  }

  @Override
  public ExecNode<?> translateToExecNode() {
    // Split the dimension-side calc into its projection and filter, exactly as Flink's own physical
    // node does when handing off to its exec node.
    List<RexNode> projectionOnTemporalTable = null;
    RexNode filterOnTemporalTable = null;
    if (calcOnTemporalTable != null) {
      scala.Tuple2<List<RexNode>, scala.Option<RexNode>> expanded =
          FlinkRexUtil.expandRexProgram(calcOnTemporalTable);
      projectionOnTemporalTable = expanded._1();
      filterOnTemporalTable = expanded._2().isDefined() ? expanded._2().get() : null;
    }
    return new NativeLookupJoinExecNode(
        ShortcutUtils.unwrapTableConfig(this),
        InputProperty.DEFAULT,
        FlinkTypeFactory$.MODULE$.toLogicalRowType(getRowType()),
        getRelDetailedDescription(),
        temporalTable,
        FlinkTypeFactory$.MODULE$.toLogicalRowType(getInput().getRowType()),
        lookupKeys,
        projectionOnTemporalTable,
        filterOnTemporalTable,
        preFilterCondition,
        remainingJoinCondition,
        leftOuterJoin,
        asyncOptions);
  }
}
