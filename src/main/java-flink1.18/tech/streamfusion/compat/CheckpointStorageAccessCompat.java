package tech.streamfusion.compat;

public interface CheckpointStorageAccessCompat
    extends org.apache.flink.runtime.state.CheckpointStorageAccess {
  org.apache.flink.runtime.state.CheckpointStorageAccess storageDelegate();
}
