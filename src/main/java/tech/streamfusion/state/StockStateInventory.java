package tech.streamfusion.state;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.CheckpointableKeyedStateBackend;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot;
import org.apache.flink.streaming.api.operators.TimerSerializer;

/** Rejects restored state that a native adapter cannot consume before native initialization. */
public final class StockStateInventory {
  private static final Set<String> EMPTY_USER_TIMERS =
      Set.of("_timer_state/event_user-timers", "_timer_state/processing_user-timers");

  private StockStateInventory() {}

  public static <K> void validate(
      CheckpointableKeyedStateBackend<K> backend,
      Map<String, StateDescriptor.Type> expected,
      Set<String> consumedTimerQueues,
      TypeSerializer<K> keySerializer)
      throws Exception {
    var resources = backend.savepoint().getSnapshotResources();
    try (AutoCloseable release = resources::release) {
      var metadata = resources.getMetaInfoSnapshots();
      Map<Integer, String> unusedTimers = new HashMap<>();
      for (int index = 0; index < metadata.size(); index++) {
        var state = metadata.get(index);
        String name = state.getName();
        switch (state.getBackendStateType()) {
          case KEY_VALUE:
            StateDescriptor.Type kind = expected.get(name);
            if (kind == null)
              throw incompatible(name, "descriptor is not owned by the selected native adapter");
            if (!kind.name()
                .equals(state.getOption(StateMetaInfoSnapshot.CommonOptionsKeys.KEYED_STATE_TYPE)))
              throw incompatible(name, "expected " + kind + " state");
            var namespace =
                state.getTypeSerializerSnapshot(
                    StateMetaInfoSnapshot.CommonSerializerKeys.NAMESPACE_SERIALIZER);
            if (namespace == null
                || !VoidNamespaceSerializer.INSTANCE.equals(namespace.restoreSerializer()))
              throw incompatible(name, "expected the stock void namespace serializer");
            break;
          case PRIORITY_QUEUE:
            boolean owned = consumedTimerQueues.contains(name);
            if (!owned && !EMPTY_USER_TIMERS.contains(name))
              throw incompatible(name, "timer queue is not owned by the selected native adapter");
            var timerSnapshot =
                state.getTypeSerializerSnapshot(
                    StateMetaInfoSnapshot.CommonSerializerKeys.VALUE_SERIALIZER);
            var timerSerializer = timerSnapshot == null ? null : timerSnapshot.restoreSerializer();
            if (!(timerSerializer instanceof TimerSerializer)
                || !VoidNamespaceSerializer.INSTANCE.equals(
                    ((TimerSerializer<?, ?>) timerSerializer).getNamespaceSerializer()))
              throw incompatible(name, "expected stock timers with the void namespace serializer");
            if (!keySerializer.equals(((TimerSerializer<?, ?>) timerSerializer).getKeySerializer()))
              throw incompatible(
                  name, "timer key serializer differs from the operator key serializer");
            if (!owned) unusedTimers.put(index, name);
            break;
          default:
            throw incompatible(
                name, "unsupported backend state kind " + state.getBackendStateType());
        }
      }
      if (!unusedTimers.isEmpty()) {
        try (var iterator = resources.createKVStateIterator()) {
          while (iterator.isValid()) {
            String name = unusedTimers.get(iterator.kvStateId());
            if (name != null)
              throw incompatible(name, "native adapter does not consume these pending timers");
            iterator.next();
          }
        }
      }
    }
  }

  private static IllegalStateException incompatible(String name, String reason) {
    return new IllegalStateException("Cannot import canonical state '" + name + "': " + reason);
  }
}
