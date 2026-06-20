package io.github.jordepic.streamfusion.planner;

import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory$;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalRel;
import org.apache.flink.table.planner.utils.ShortcutUtils;

/**
 * Physical node standing in for a pure filter the native operator runs. It keeps the replaced node's
 * output type (the input type — a filter does not rewrite columns) and traits, and carries the
 * encoded predicate expression the operator compiles and applies. Stateless, so it needs no
 * watermark.
 */
public class StreamPhysicalNativeFilter extends SingleRel
    implements StreamPhysicalRel, ColumnarInput, ColumnarOutput {

  private final RelDataType outputRowType;
  private final int[] projection;
  private final int[] kinds;
  private final int[] payload;
  private final int[] childCounts;
  private final long[] longs;
  private final double[] doubles;
  private final String[] strings;

  public StreamPhysicalNativeFilter(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelNode input,
      RelDataType outputRowType,
      int[] projection,
      int[] kinds,
      int[] payload,
      int[] childCounts,
      long[] longs,
      double[] doubles,
      String[] strings) {
    super(cluster, traitSet, input);
    this.outputRowType = outputRowType;
    this.projection = projection;
    this.kinds = kinds;
    this.payload = payload;
    this.childCounts = childCounts;
    this.longs = longs;
    this.doubles = doubles;
    this.strings = strings;
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
    return new StreamPhysicalNativeFilter(
        getCluster(),
        traitSet,
        inputs.get(0),
        outputRowType,
        projection,
        kinds,
        payload,
        childCounts,
        longs,
        doubles,
        strings);
  }

  @Override
  public ExecNode<?> translateToExecNode() {
    return new NativeFilterExecNode(
        ShortcutUtils.unwrapTableConfig(this),
        InputProperty.DEFAULT,
        FlinkTypeFactory$.MODULE$.toLogicalRowType(getRowType()),
        getRelDetailedDescription(),
        projection,
        kinds,
        payload,
        childCounts,
        longs,
        doubles,
        strings);
  }
}
