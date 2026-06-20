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
 * Physical node standing in for a filesystem Parquet sink the native writer runs. It keeps the
 * replaced sink's row type and traits and carries the output path; the native operator writes the
 * incoming rows to Parquet directly. Stateless with respect to event time, so it needs no watermark.
 */
public class StreamPhysicalNativeParquetSink extends SingleRel
    implements StreamPhysicalRel, ColumnarInput {

  private final RelDataType outputRowType;
  private final String path;

  public StreamPhysicalNativeParquetSink(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelNode input,
      RelDataType outputRowType,
      String path) {
    super(cluster, traitSet, input);
    this.outputRowType = outputRowType;
    this.path = path;
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
    return new StreamPhysicalNativeParquetSink(
        getCluster(), traitSet, inputs.get(0), outputRowType, path);
  }

  @Override
  public ExecNode<?> translateToExecNode() {
    return new NativeParquetSinkExecNode(
        ShortcutUtils.unwrapTableConfig(this),
        InputProperty.DEFAULT,
        FlinkTypeFactory$.MODULE$.toLogicalRowType(getRowType()),
        getRelDetailedDescription(),
        path);
  }
}
