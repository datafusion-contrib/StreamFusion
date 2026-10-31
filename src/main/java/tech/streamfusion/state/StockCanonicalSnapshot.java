package tech.streamfusion.state;

import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.SavepointType;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.CheckpointableKeyedStateBackend;
import org.apache.flink.runtime.state.OperatorStateBackend;
import org.apache.flink.streaming.api.operators.OperatorSnapshotFutures;
import org.apache.flink.streaming.api.operators.StreamOperatorStateHandler;

/** Captures stock keyed and managed operator state at the same synchronous barrier boundary. */
public final class StockCanonicalSnapshot {
  private StockCanonicalSnapshot() {}

  public static OperatorSnapshotFutures capture(
      CheckpointableKeyedStateBackend<?> keyed,
      OperatorStateBackend operator,
      long checkpointId,
      long timestamp,
      CheckpointStreamFactory streams,
      CheckpointOptions options,
      CloseableRegistry cancelables)
      throws Exception {
    if (!options.getCheckpointType().isSavepoint()
        || ((SavepointType) options.getCheckpointType()).getFormatType()
            != SavepointFormatType.CANONICAL)
      throw new IllegalArgumentException("Stock state capture requires a canonical savepoint");
    var result = new OperatorSnapshotFutures();
    try {
      if (operator != null)
        result.setOperatorStateManagedFuture(
            operator.snapshot(checkpointId, timestamp, streams, options));
      var runner = StreamOperatorStateHandler.prepareCanonicalSavepoint(keyed, cancelables);
      result.setKeyedStateManagedFuture(runner.snapshot(checkpointId, timestamp, streams, options));
      return result;
    } catch (Exception | Error failure) {
      try {
        result.cancel();
      } catch (Exception | Error cleanup) {
        failure.addSuppressed(cleanup);
      }
      throw failure;
    }
  }
}
