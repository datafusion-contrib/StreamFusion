package tech.streamfusion.operator;

import java.io.IOException;
import org.apache.flink.streaming.api.functions.sink.filesystem.PartFileInfo;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.CheckpointRollingPolicy;
import org.apache.flink.util.Preconditions;

/**
 * Flink's {@code FileSystemTableSink.TableRollingPolicy} generified to the sink's batch element
 * (upstream hard-types it to {@code RowData}), with the bulk-format behavior fixed: a Parquet part
 * file always rolls on checkpoint, because an in-progress Parquet file has no footer and cannot be
 * resumed. Size checks see the drained stream position, which advances a flushed row group at a
 * time — the same granularity the host writer's size checks observe.
 */
public final class NativeParquetRollingPolicy
    extends CheckpointRollingPolicy<PartitionedArrowBatch, String> {

  private final long rollingFileSize;
  private final long rollingTimeInterval;
  private final long inactivityInterval;

  public NativeParquetRollingPolicy(
      long rollingFileSize, long rollingTimeInterval, long inactivityInterval) {
    Preconditions.checkArgument(rollingFileSize > 0L);
    Preconditions.checkArgument(rollingTimeInterval > 0L);
    Preconditions.checkArgument(inactivityInterval > 0L);
    this.rollingFileSize = rollingFileSize;
    this.rollingTimeInterval = rollingTimeInterval;
    this.inactivityInterval = inactivityInterval;
  }

  @Override
  public boolean shouldRollOnCheckpoint(PartFileInfo<String> partFileState) {
    return true;
  }

  @Override
  public boolean shouldRollOnEvent(PartFileInfo<String> partFileState, PartitionedArrowBatch element)
      throws IOException {
    return partFileState.getSize() > rollingFileSize;
  }

  @Override
  public boolean shouldRollOnProcessingTime(PartFileInfo<String> partFileState, long currentTime) {
    return currentTime - partFileState.getCreationTime() >= rollingTimeInterval
        || currentTime - partFileState.getLastUpdateTime() >= inactivityInterval;
  }
}
