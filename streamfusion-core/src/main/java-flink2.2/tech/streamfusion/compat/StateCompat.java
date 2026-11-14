package tech.streamfusion.compat;

import java.util.List;
import java.util.stream.Stream;
import org.apache.flink.runtime.state.*;

/** Preserves the host line's key context and incremental checkpoint ownership APIs. */
public final class StateCompat {
  private StateCompat() {}

  public static boolean normalizerComparesRowKind() {
    return false;
  }

  public static <K> void setCurrentKeyAndGroup(
      CheckpointableKeyedStateBackend<K> backend, K key, int group) {
    backend.setCurrentKeyAndKeyGroup(key, group);
  }

  public static <K, N> Stream<K> keys(
      CheckpointableKeyedStateBackend<K> backend, List<String> states, N namespace) {
    return backend.getKeys(states, namespace);
  }

  public static String backendType(CheckpointableKeyedStateBackend<?> backend) {
    return backend.getBackendTypeIdentifier();
  }

  public static boolean couldReuse(CheckpointStreamFactory factory, StreamStateHandle handle) {
    return factory.couldReuseStateHandle(handle);
  }

  public static StreamStateHandle placeholder(StreamStateHandle handle) {
    return new PlaceholderStreamStateHandle(
        handle.getStreamStateHandleID(), handle.getStateSize(), false);
  }

  public static void reused(CheckpointStreamFactory factory, List<StreamStateHandle> handles)
      throws java.io.IOException {
    factory.reusePreviousStateHandle(handles);
  }

  public static StreamStateHandle metaHandle(IncrementalRemoteKeyedStateHandle handle) {
    return handle.getMetaDataStateHandle();
  }
}
