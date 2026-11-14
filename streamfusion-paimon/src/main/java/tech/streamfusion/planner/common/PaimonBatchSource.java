package tech.streamfusion.planner.common;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.streaming.api.transformations.SourceTransformation;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.types.logical.RowType;
import org.apache.paimon.flink.source.FileStoreSourceSplit;
import org.apache.paimon.flink.source.PendingSplitsCheckpoint;
import org.apache.paimon.flink.source.StaticFileStoreSource;
import org.apache.paimon.table.FileStoreTable;
import tech.streamfusion.operator.ArrowBatch;
import tech.streamfusion.operator.ArrowBatchTypeInformation;
import tech.streamfusion.paimon.NativePaimonSource;
import tech.streamfusion.planner.NativePlannerExtension;

/** Keeps the exact bounded scan planner, including consumed partition predicates and snapshots. */
public final class PaimonBatchSource implements NativePlannerExtension.BatchSource {
  private final FileStoreTable table;

  public PaimonBatchSource(FileStoreTable table) {
    this.table = table;
  }

  @Override
  @SuppressWarnings("unchecked")
  public Transformation<ArrowBatch> translate(
      PlannerBase planner, Transformation<RowData> input, RowType output, int rows) {
    if (!(input instanceof SourceTransformation)) return null;
    var original = (SourceTransformation<?, ?, ?>) input;
    if (!(original.getSource() instanceof StaticFileStoreSource)
        && !original
            .getSource()
            .getClass()
            .getName()
            .equals("org.apache.paimon.flink.source.PaimonDataStreamSource")) return null;
    if (!(original.getSource().getSplitSerializer()
            instanceof org.apache.paimon.flink.source.FileStoreSourceSplitSerializer)
        || !(original.getSource().getEnumeratorCheckpointSerializer()
            instanceof org.apache.paimon.flink.source.PendingSplitsCheckpointSerializer))
      return null;
    int[] projection =
        output.getFieldNames().stream()
            .mapToInt(table.rowType().getFieldNames()::indexOf)
            .toArray();
    var source =
        NativePaimonSource.bounded(
            table,
            projection,
            rows,
            (Source<RowData, FileStoreSourceSplit, PendingSplitsCheckpoint>) original.getSource());
    var result =
        new SourceTransformation<>(
            "NativePaimonBatchSource",
            source.withLineage(),
            WatermarkStrategy.noWatermarks(),
            ArrowBatchTypeInformation.INSTANCE,
            input.getParallelism());
    if (input.getMaxParallelism() > 0) result.setMaxParallelism(input.getMaxParallelism());
    if (input.getUid() != null) result.setUid(input.getUid());
    result.setDescription(input.getDescription());
    result.setResources(input.getMinResources(), input.getPreferredResources());
    input.getSlotSharingGroup().ifPresent(result::setSlotSharingGroup);
    return result;
  }
}
