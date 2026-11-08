package tech.streamfusion.compat;

public interface CheckpointStorageAccessCompat
    extends org.apache.flink.runtime.state.CheckpointStorageAccess {
  org.apache.flink.runtime.state.CheckpointStorageAccess storageDelegate();

  @Override
  default org.apache.flink.runtime.state.CheckpointStorageWorkerView toFileMergingStorage(
      org.apache.flink.runtime.checkpoint.filemerging.FileMergingSnapshotManager manager,
      org.apache.flink.runtime.execution.Environment environment)
      throws java.io.IOException {
    return storageDelegate().toFileMergingStorage(manager, environment);
  }
}
