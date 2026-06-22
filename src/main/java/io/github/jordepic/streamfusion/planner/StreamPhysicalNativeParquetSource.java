package io.github.jordepic.streamfusion.planner;

import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory$;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalRel;
import org.apache.flink.table.planner.utils.ShortcutUtils;

/**
 * Leaf physical node standing in for a filesystem Parquet source the native reader runs. It reads the
 * directory as Arrow batches, so the data starts columnar and never becomes rows — the read side of a
 * fully columnar pipeline. Keeps the scan's row type; stateless, so it needs no watermark.
 */
public class StreamPhysicalNativeParquetSource extends AbstractRelNode
    implements StreamPhysicalRel, ColumnarOutput {

  private final RelDataType outputRowType;
  private final String path;
  private final boolean utcTimestamp;

  public StreamPhysicalNativeParquetSource(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelDataType outputRowType,
      String path,
      boolean utcTimestamp) {
    super(cluster, traitSet);
    this.outputRowType = outputRowType;
    this.path = path;
    this.utcTimestamp = utcTimestamp;
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
    return new StreamPhysicalNativeParquetSource(
        getCluster(), traitSet, outputRowType, path, utcTimestamp);
  }

  @Override
  public RelWriter explainTerms(RelWriter writer) {
    return super.explainTerms(writer).item("path", path);
  }

  @Override
  public ExecNode<?> translateToExecNode() {
    // The output field names, in order, are exactly the columns to read (honoring the projection
    // Flink pushed into the scan); the native reader selects them by name from each file batch.
    String[] projection = getRowType().getFieldNames().toArray(new String[0]);
    return new NativeParquetSourceExecNode(
        ShortcutUtils.unwrapTableConfig(this),
        FlinkTypeFactory$.MODULE$.toLogicalRowType(getRowType()),
        getRelDetailedDescription(),
        path,
        projection,
        utcTimestamp);
  }
}
