package tech.streamfusion.compat;

import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.streaming.api.operators.OperatorSnapshotFutures;

/** Flink 1.18 retains its final snapshot lifecycle and the existing native canonical format. */
public abstract class FlinkCanonicalStreamOperator<OUT> extends FlinkStreamOperator<OUT> {
  private static final long serialVersionUID = 1L;

  protected OperatorSnapshotFutures snapshotCanonicalState(
      long checkpointId, long timestamp, CheckpointOptions options, CheckpointStreamFactory streams)
      throws Exception {
    return null;
  }
}
