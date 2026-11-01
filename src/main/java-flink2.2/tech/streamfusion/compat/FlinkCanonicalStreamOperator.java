package tech.streamfusion.compat;

import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.streaming.api.operators.OperatorSnapshotFutures;

/** Flink 2.2 permits replacing the snapshot backend for a stock-schema canonical export. */
public abstract class FlinkCanonicalStreamOperator<OUT> extends FlinkStreamOperator<OUT> {
  private static final long serialVersionUID = 1L;

  protected OperatorSnapshotFutures snapshotCanonicalState(
      long checkpointId, long timestamp, CheckpointOptions options, CheckpointStreamFactory streams)
      throws Exception {
    return null;
  }

  @Override
  public OperatorSnapshotFutures snapshotState(
      long checkpointId, long timestamp, CheckpointOptions options, CheckpointStreamFactory streams)
      throws Exception {
    OperatorSnapshotFutures canonical =
        snapshotCanonicalState(checkpointId, timestamp, options, streams);
    return canonical == null
        ? super.snapshotState(checkpointId, timestamp, options, streams)
        : canonical;
  }
}
