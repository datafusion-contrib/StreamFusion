package tech.streamfusion.state;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
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

  public static final class State implements java.io.Serializable {
    private static final long serialVersionUID = 1L;
    final StateDescriptor.Type kind;
    final TypeSerializer<?> namespace;

    public State(StateDescriptor.Type kind, TypeSerializer<?> namespace) {
      this.kind = Objects.requireNonNull(kind, "state kind");
      this.namespace = Objects.requireNonNull(namespace, "state namespace").duplicate();
    }
  }

  public static final class Timer implements java.io.Serializable {
    private static final long serialVersionUID = 1L;
    final TypeSerializer<?> namespace;
    final boolean emptyOnly;

    private Timer(TypeSerializer<?> namespace, boolean emptyOnly) {
      this.namespace = Objects.requireNonNull(namespace, "timer namespace").duplicate();
      this.emptyOnly = emptyOnly;
    }

    public static Timer consumed(TypeSerializer<?> namespace) {
      return new Timer(namespace, false);
    }

    public static Timer empty(TypeSerializer<?> namespace) {
      return new Timer(namespace, true);
    }
  }

  public static <K> void validate(
      CheckpointableKeyedStateBackend<K> backend,
      Map<String, StateDescriptor.Type> expected,
      Set<String> consumedTimerQueues,
      TypeSerializer<K> keySerializer)
      throws Exception {
    Map<String, State> states = new HashMap<>();
    expected.forEach(
        (name, kind) -> states.put(name, new State(kind, VoidNamespaceSerializer.INSTANCE)));
    Map<String, Timer> timers = new HashMap<>();
    consumedTimerQueues.forEach(
        name -> timers.put(name, Timer.consumed(VoidNamespaceSerializer.INSTANCE)));
    validate(backend, states, timers, keySerializer);
  }

  public static <K> void validate(
      CheckpointableKeyedStateBackend<K> backend,
      Map<String, State> expected,
      Map<String, Timer> timers,
      TypeSerializer<K> keySerializer)
      throws Exception {
    expected = Map.copyOf(expected);
    timers = Map.copyOf(timers);
    Objects.requireNonNull(keySerializer, "operator key serializer");
    var resources = backend.savepoint().getSnapshotResources();
    try (AutoCloseable release = resources::release) {
      var metadata = resources.getMetaInfoSnapshots();
      Map<Integer, String> unusedTimers = new HashMap<>();
      for (int index = 0; index < metadata.size(); index++) {
        var state = metadata.get(index);
        String name = state.getName();
        switch (state.getBackendStateType()) {
          case KEY_VALUE:
            State declared = expected.get(name);
            if (declared == null)
              throw incompatible(name, "descriptor is not owned by the selected native adapter");
            if (!declared
                .kind
                .name()
                .equals(state.getOption(StateMetaInfoSnapshot.CommonOptionsKeys.KEYED_STATE_TYPE)))
              throw incompatible(name, "expected " + declared.kind + " state");
            var namespace =
                state.getTypeSerializerSnapshot(
                    StateMetaInfoSnapshot.CommonSerializerKeys.NAMESPACE_SERIALIZER);
            if (namespace == null || !declared.namespace.equals(namespace.restoreSerializer()))
              throw incompatible(name, namespaceError(declared.namespace));
            break;
          case PRIORITY_QUEUE:
            Timer timer = timers.get(name);
            if (timer == null && EMPTY_USER_TIMERS.contains(name))
              timer = Timer.empty(VoidNamespaceSerializer.INSTANCE);
            if (timer == null)
              throw incompatible(name, "timer queue is not owned by the selected native adapter");
            var timerSnapshot =
                state.getTypeSerializerSnapshot(
                    StateMetaInfoSnapshot.CommonSerializerKeys.VALUE_SERIALIZER);
            var timerSerializer = timerSnapshot == null ? null : timerSnapshot.restoreSerializer();
            if (!(timerSerializer instanceof TimerSerializer)
                || !timer.namespace.equals(
                    ((TimerSerializer<?, ?>) timerSerializer).getNamespaceSerializer()))
              throw incompatible(name, "timer " + namespaceError(timer.namespace));
            if (!keySerializer.equals(((TimerSerializer<?, ?>) timerSerializer).getKeySerializer()))
              throw incompatible(
                  name, "timer key serializer differs from the operator key serializer");
            if (timer.emptyOnly) unusedTimers.put(index, name);
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

  private static String namespaceError(TypeSerializer<?> expected) {
    return VoidNamespaceSerializer.INSTANCE.equals(expected)
        ? "expected the stock void namespace serializer"
        : "expected stock namespace serializer " + expected.getClass().getName();
  }

  private static IllegalStateException incompatible(String name, String reason) {
    return new IllegalStateException("Cannot import canonical state '" + name + "': " + reason);
  }
}
