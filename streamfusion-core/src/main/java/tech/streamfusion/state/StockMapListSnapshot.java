package tech.streamfusion.state;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.NoSuchElementException;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.ListSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.common.typeutils.base.MapSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.runtime.state.CheckpointableKeyedStateBackend;
import org.apache.flink.runtime.state.CompositeKeySerializationUtils;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.CloseableIterator;
import tech.streamfusion.compat.FlinkStateBackendCompat;

/** Visits released map/list bytes, with optional disk staging and entry-by-entry restore. */
final class StockMapListSnapshot {
  private final StockStateSpool spool;
  private final TypeSerializer<RowData> rows;
  private final int values, order, counts;

  StockMapListSnapshot(StockStateSpool spool, TypeSerializer<RowData> rows, int firstSection) {
    this.spool = spool;
    this.rows = rows.duplicate();
    values = firstSection;
    order = firstSection + 1;
    counts = firstSection + 2;
  }

  void capture(
      CheckpointableKeyedStateBackend<RowData> backend, String name, TypeSerializer<RowData> keys)
      throws Exception {
    visit(
        backend,
        name,
        keys,
        VoidNamespaceSerializer.INSTANCE,
        LongSerializer.INSTANCE,
        rows,
        (owner, namespace, timestamp, encoded) -> {
          if (spool.contains(values, owner, timestamp))
            throw new IllegalArgumentException("Duplicate canonical map entry: " + name);
          byte[] counter = spool.get(counts, owner, 0);
          long position = counter == null ? 0 : ByteBuffer.wrap(counter).getLong();
          spool.addOwner(owner);
          spool.put(values, owner, timestamp, encoded);
          spool.put(order, owner, position, ByteBuffer.allocate(8).putLong(timestamp).array());
          spool.put(
              counts,
              owner,
              0,
              ByteBuffer.allocate(8).putLong(Math.incrementExact(position)).array());
        });
  }

  @FunctionalInterface
  interface BucketReceiver<N, K> {
    void accept(RowData owner, N namespace, K mapKey, byte[] encoded) throws Exception;
  }

  static <N, K, V> void visit(
      CheckpointableKeyedStateBackend<RowData> backend,
      String name,
      TypeSerializer<RowData> keys,
      TypeSerializer<N> namespace,
      TypeSerializer<K> mapKeys,
      TypeSerializer<V> rows,
      BucketReceiver<N, K> receiver)
      throws Exception {
    visitRaw(backend, name, keys, namespace, mapKeys, new ListSerializer<>(rows), receiver);
  }

  static <N, K, V> void visitRaw(
      CheckpointableKeyedStateBackend<RowData> backend,
      String name,
      TypeSerializer<RowData> keys,
      TypeSerializer<N> namespace,
      TypeSerializer<K> mapKeys,
      TypeSerializer<V> values,
      BucketReceiver<N, K> receiver)
      throws Exception {
    int prefix = FlinkStateBackendCompat.keyGroupPrefixBytes(backend);
    var resources = backend.savepoint().getSnapshotResources();
    try (AutoCloseable release = resources::release) {
      if (!keys.equals(resources.getKeySerializer()))
        throw new IllegalArgumentException("Canonical map owner serializer changed");
      var metadata = resources.getMetaInfoSnapshots();
      int selected = -1;
      TypeSerializer<?> expected = mapKeys == null ? values : new MapSerializer<>(mapKeys, values);
      for (int i = 0; i < metadata.size(); i++) {
        var state = metadata.get(i);
        if (!state.getName().equals(name)) continue;
        var namespaceSnapshot =
            state.getTypeSerializerSnapshot(
                StateMetaInfoSnapshot.CommonSerializerKeys.NAMESPACE_SERIALIZER);
        var valueSnapshot =
            state.getTypeSerializerSnapshot(
                StateMetaInfoSnapshot.CommonSerializerKeys.VALUE_SERIALIZER);
        if (state.getBackendStateType() != StateMetaInfoSnapshot.BackendStateType.KEY_VALUE
            || !(mapKeys == null ? "VALUE" : "MAP")
                .equals(state.getOption(StateMetaInfoSnapshot.CommonOptionsKeys.KEYED_STATE_TYPE))
            || namespaceSnapshot == null
            || !sameSerializer(namespace, namespaceSnapshot.restoreSerializer())
            || valueSnapshot == null
            || !sameSerializer(expected, valueSnapshot.restoreSerializer()))
          throw new IllegalArgumentException("Canonical map/list serializer changed: " + name);
        selected = i;
      }
      if (selected < 0) return;
      boolean ambiguous = CompositeKeySerializationUtils.isAmbiguousKeyPossible(keys, namespace);
      try (var entries = resources.createKVStateIterator()) {
        while (entries.isValid()) {
          if (entries.kvStateId() == selected) {
            var input = new DataInputDeserializer(entries.key());
            int group = CompositeKeySerializationUtils.readKeyGroup(prefix, input);
            var owner = CompositeKeySerializationUtils.readKey(keys, input, ambiguous);
            N ns = CompositeKeySerializationUtils.readNamespace(namespace, input, ambiguous);
            K mapKey = mapKeys == null ? null : mapKeys.deserialize(input);
            if (group != entries.keyGroup()
                || !backend.getKeyGroupRange().contains(group)
                || input.available() != 0)
              throw new IllegalArgumentException("Canonical map key/namespace changed: " + name);
            receiver.accept(owner, ns, mapKey, entries.value());
          }
          entries.next();
        }
      }
    }
  }

  private static boolean sameSerializer(TypeSerializer<?> expected, TypeSerializer<?> actual) {
    try {
      return expected.getClass() == actual.getClass() && expected.equals(actual);
    } catch (ClassCastException mismatch) {
      // Released composite serializers may cast nested serializers before checking their types.
      return false;
    }
  }

  CloseableIterator<Long> timestamps(RowData owner) throws Exception {
    return spool.values(order, owner, LongSerializer.INSTANCE);
  }

  CloseableIterator<Long> sortedTimestamps(RowData owner) throws Exception {
    return spool.times(values, owner);
  }

  boolean contains(RowData owner, long timestamp) throws Exception {
    return spool.contains(values, owner, timestamp);
  }

  int size(RowData owner, long timestamp) throws Exception {
    return header(new DataInputDeserializer(encoded(owner, timestamp)));
  }

  Iterable<RowData> rows(RowData owner, long timestamp) throws Exception {
    return decode(encoded(owner, timestamp), rows);
  }

  private byte[] encoded(RowData owner, long timestamp) throws Exception {
    byte[] encoded = spool.get(values, owner, timestamp);
    if (encoded == null) throw new IllegalArgumentException("Missing canonical map/list bucket");
    return encoded;
  }

  static int size(byte[] encoded) throws IOException {
    return header(new DataInputDeserializer(encoded));
  }

  private static int header(DataInputDeserializer input) throws IOException {
    if (input.readBoolean()) {
      if (input.available() != 0) throw new IOException("Trailing null canonical map value bytes");
      return -1;
    }
    int size = input.readInt();
    if (size < 0) throw new IOException("Negative canonical list size");
    return size;
  }

  static <V> Iterable<V> decode(byte[] encoded, TypeSerializer<V> rows) {
    return () -> {
      var input = new DataInputDeserializer(encoded);
      final int size;
      try {
        size = header(input);
      } catch (IOException failure) {
        throw new UncheckedIOException(failure);
      }
      return new Iterator<>() {
        private int remaining = Math.max(0, size);

        @Override
        public boolean hasNext() {
          if (remaining == 0 && input.available() != 0)
            throw new IllegalArgumentException("Trailing canonical list bytes");
          return remaining > 0;
        }

        @Override
        public V next() {
          if (!hasNext()) throw new NoSuchElementException();
          try {
            var row = rows.deserialize(input);
            remaining--;
            if (remaining == 0) hasNext();
            return row;
          } catch (IOException failure) {
            throw new UncheckedIOException(failure);
          }
        }
      };
    };
  }
}
