package tech.streamfusion.state;

import java.util.Map;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.ListSerializer;
import org.apache.flink.api.common.typeutils.base.MapSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.runtime.state.CheckpointableKeyedStateBackend;
import org.apache.flink.runtime.state.CompositeKeySerializationUtils;
import org.apache.flink.runtime.state.RegisteredKeyValueStateBackendMetaInfo;
import org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot;
import tech.streamfusion.compat.StateCompat;

/** Clears a projection through its immutable snapshot iterator, without collecting all keys. */
final class StockStateCleanup {
  private StockStateCleanup() {}

  static <K> void clear(
      CheckpointableKeyedStateBackend<K> backend, Map<String, StateDescriptor.Type> states)
      throws Exception {
    if (!(backend instanceof AbstractKeyedStateBackend))
      throw new IllegalArgumentException("Stock projection requires a released Flink backend");
    var host = (AbstractKeyedStateBackend<K>) backend;
    K previousKey = host.getCurrentKey();
    int previousGroup = host.getCurrentKeyGroupIndex();
    int prefix =
        CompositeKeySerializationUtils.computeRequiredBytesInKeyGroupPrefix(
            host.getNumberOfKeyGroups());
    var resources = backend.savepoint().getSnapshotResources();
    try (AutoCloseable release = resources::release;
        var iterator = resources.createKVStateIterator()) {
      var metadata = resources.getMetaInfoSnapshots();
      var descriptors = new java.util.HashMap<Integer, StateDescriptor<?, ?>>();
      var namespaces = new java.util.HashMap<Integer, TypeSerializer<Object>>();
      for (int i = 0; i < metadata.size(); i++) {
        var entry = metadata.get(i);
        if (entry.getBackendStateType() == StateMetaInfoSnapshot.BackendStateType.KEY_VALUE
            && states.containsKey(entry.getName())) {
          var info = new RegisteredKeyValueStateBackendMetaInfo<Object, Object>(entry);
          if (states.get(entry.getName()) != info.getStateType())
            throw new IllegalStateException("Stock state kind changed: " + entry.getName());
          descriptors.put(i, descriptor(info));
          namespaces.put(i, info.getNamespaceSerializer());
        }
      }
      var input = new DataInputDeserializer();
      while (iterator.isValid()) {
        var descriptor = descriptors.get(iterator.kvStateId());
        if (descriptor != null) {
          var namespaceSerializer = namespaces.get(iterator.kvStateId());
          boolean ambiguous =
              CompositeKeySerializationUtils.isAmbiguousKeyPossible(
                  backend.getKeySerializer(), namespaceSerializer);
          input.setBuffer(iterator.key());
          int group = CompositeKeySerializationUtils.readKeyGroup(prefix, input);
          K key =
              CompositeKeySerializationUtils.readKey(backend.getKeySerializer(), input, ambiguous);
          Object namespace =
              CompositeKeySerializationUtils.readNamespace(namespaceSerializer, input, ambiguous);
          StateCompat.setCurrentKeyAndGroup(backend, key, group);
          backend.getPartitionedState(namespace, namespaceSerializer, descriptor).clear();
        }
        iterator.next();
      }
    } finally {
      if (previousKey == null) CanonicalNativeState.clearKeyContext(host);
      else StateCompat.setCurrentKeyAndGroup(backend, previousKey, previousGroup);
    }
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  private static StateDescriptor<?, ?> descriptor(
      RegisteredKeyValueStateBackendMetaInfo<Object, Object> info) {
    switch (info.getStateType()) {
      case VALUE:
        return new ValueStateDescriptor<>(info.getName(), info.getStateSerializer());
      case LIST:
        return new ListStateDescriptor<>(
            info.getName(), ((ListSerializer) info.getStateSerializer()).getElementSerializer());
      case MAP:
        var map = (MapSerializer) info.getStateSerializer();
        return new MapStateDescriptor<>(
            info.getName(), map.getKeySerializer(), map.getValueSerializer());
      default:
        throw new IllegalStateException(
            "Unsupported stock projection state: " + info.getStateType());
    }
  }
}
