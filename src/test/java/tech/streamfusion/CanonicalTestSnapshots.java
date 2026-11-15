package tech.streamfusion;

import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.checkpoint.SavepointType;
import org.apache.flink.runtime.state.CheckpointStorageLocationReference;
import org.apache.flink.runtime.state.memory.MemCheckpointStreamFactory;
import org.apache.flink.streaming.util.AbstractStreamOperatorTestHarness;

/** Cancels sibling snapshot futures when a canonical writer fails in an operator harness. */
public final class CanonicalTestSnapshots {
  private CanonicalTestSnapshots() {}

  public static OperatorSubtaskState snapshot(
      AbstractStreamOperatorTestHarness<?> harness, long id, long timestamp) throws Exception {
    harness.prepareSnapshotPreBarrier(id);
    var futures =
        harness
            .getOperator()
            .snapshotState(
                id,
                timestamp,
                CheckpointOptions.forConfig(
                    SavepointType.savepoint(SavepointFormatType.CANONICAL),
                    CheckpointStorageLocationReference.getDefault(),
                    true,
                    false,
                    0),
                new MemCheckpointStreamFactory(16 * 1024 * 1024));
    try {
      return tech.streamfusion.compat.CanonicalTestCompat.finishSnapshot(futures);
    } finally {
      futures.cancel();
    }
  }
}
