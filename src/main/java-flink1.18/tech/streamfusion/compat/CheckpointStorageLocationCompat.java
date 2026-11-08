package tech.streamfusion.compat;

public interface CheckpointStorageLocationCompat
    extends org.apache.flink.runtime.state.CheckpointStorageLocation {
  org.apache.flink.runtime.state.CheckpointStorageLocation storageDelegate();
}
