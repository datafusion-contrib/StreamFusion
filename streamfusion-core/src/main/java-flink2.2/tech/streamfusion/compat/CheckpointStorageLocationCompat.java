package tech.streamfusion.compat;

public interface CheckpointStorageLocationCompat
    extends org.apache.flink.runtime.state.CheckpointStorageLocation {
  org.apache.flink.runtime.state.CheckpointStorageLocation storageDelegate();

  @Override
  default void reusePreviousStateHandle(
      java.util.Collection<? extends org.apache.flink.runtime.state.StreamStateHandle> handles) {
    storageDelegate().reusePreviousStateHandle(handles);
  }

  @Override
  default boolean couldReuseStateHandle(org.apache.flink.runtime.state.StreamStateHandle handle) {
    return storageDelegate().couldReuseStateHandle(handle);
  }
}
