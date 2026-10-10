package tech.streamfusion.compat;

import java.util.List;
import java.util.stream.Stream;
import org.apache.flink.runtime.state.*;
import tech.streamfusion.state.RocksDBNativeKeyedStateBackend;

/** Preserves the host line's key context and incremental checkpoint ownership APIs. */
public final class StateCompat {
  private StateCompat() {}

  public static <K> void setCurrentKeyAndGroup(
      CheckpointableKeyedStateBackend<K> backend, K key, int group) {
    if (backend instanceof RocksDBNativeKeyedStateBackend<?>) {
      RocksDBNativeKeyedStateBackend<K> nativeBackend =
          ((RocksDBNativeKeyedStateBackend<K>) backend);

      nativeBackend.setCurrentKeyAndKeyGroup(key, group);
    } else if (backend instanceof org.apache.flink.runtime.state.heap.HeapKeyedStateBackend<?>) {
      org.apache.flink.runtime.state.heap.HeapKeyedStateBackend<K> host =
          ((org.apache.flink.runtime.state.heap.HeapKeyedStateBackend<K>) backend);

      // The canonical key identifies a partition; its hash is unrelated to that partition's group.
      host.getKeyContext().setCurrentKey(key);
      host.getKeyContext().setCurrentKeyGroupIndex(group);
    } else if (backend instanceof AbstractKeyedStateBackend<?>
        && KeyGroupRangeAssignment.assignToKeyGroup(
                key, ((AbstractKeyedStateBackend<K>) backend).getNumberOfKeyGroups())
            == group) {
      AbstractKeyedStateBackend<K> host = ((AbstractKeyedStateBackend<K>) backend);

      host.setCurrentKey(key);
    } else {
      throw new IllegalStateException(
          "Backend cannot restore an explicit key group: " + backend.getClass().getName());
    }
  }

  public static <K, N> Stream<K> keys(
      CheckpointableKeyedStateBackend<K> backend, List<String> states, N namespace) {
    return states.stream().flatMap(state -> backend.getKeys(state, namespace)).distinct();
  }

  public static String backendType(CheckpointableKeyedStateBackend<?> backend) {
    return backend.getClass().getName();
  }

  public static boolean couldReuse(CheckpointStreamFactory factory, StreamStateHandle handle) {
    return true;
  }

  public static StreamStateHandle placeholder(StreamStateHandle handle) {
    return new PlaceholderStreamStateHandle(handle.getStreamStateHandleID(), handle.getStateSize());
  }

  public static void reused(CheckpointStreamFactory factory, List<StreamStateHandle> handles)
      throws java.io.IOException {
    // 1.18 registers reused placeholders through the shared-state registry, without a factory
    // callback.
  }

  public static StreamStateHandle metaHandle(IncrementalRemoteKeyedStateHandle handle) {
    return handle.getMetaStateHandle();
  }
}
