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
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.CloseableIterator;
import tech.streamfusion.compat.FlinkStateBackendCompat;

/** Stages released canonical map/list bytes and decodes one row at a time during restore. */
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
    int prefix = FlinkStateBackendCompat.keyGroupPrefixBytes(backend);
    var resources = backend.savepoint().getSnapshotResources();
    try (AutoCloseable release = resources::release) {
      if (!keys.equals(resources.getKeySerializer()))
        throw new IllegalArgumentException("Canonical map owner serializer changed");
      var metadata = resources.getMetaInfoSnapshots();
      int selected = -1;
      var namespace = VoidNamespaceSerializer.INSTANCE;
      var expected = new MapSerializer<>(LongSerializer.INSTANCE, new ListSerializer<>(rows));
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
            || !"MAP"
                .equals(state.getOption(StateMetaInfoSnapshot.CommonOptionsKeys.KEYED_STATE_TYPE))
            || namespaceSnapshot == null
            || !namespace.equals(namespaceSnapshot.restoreSerializer())
            || valueSnapshot == null
            || !expected.equals(valueSnapshot.restoreSerializer()))
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
            var ns = CompositeKeySerializationUtils.readNamespace(namespace, input, ambiguous);
            long timestamp = LongSerializer.INSTANCE.deserialize(input);
            if (group != entries.keyGroup()
                || !backend.getKeyGroupRange().contains(group)
                || ns != VoidNamespace.INSTANCE
                || input.available() != 0)
              throw new IllegalArgumentException("Canonical map key/namespace changed: " + name);
            if (spool.contains(values, owner, timestamp))
              throw new IllegalArgumentException("Duplicate canonical map entry: " + name);
            byte[] counter = spool.get(counts, owner, 0);
            long position = counter == null ? 0 : ByteBuffer.wrap(counter).getLong();
            spool.addOwner(owner);
            spool.put(values, owner, timestamp, entries.value());
            spool.put(order, owner, position, ByteBuffer.allocate(8).putLong(timestamp).array());
            spool.put(
                counts,
                owner,
                0,
                ByteBuffer.allocate(8).putLong(Math.incrementExact(position)).array());
          }
          entries.next();
        }
      }
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

  private static int header(DataInputDeserializer input) throws IOException {
    if (input.readBoolean()) {
      if (input.available() != 0) throw new IOException("Trailing null canonical map value bytes");
      return -1;
    }
    int size = input.readInt();
    if (size < 0) throw new IOException("Negative canonical list size");
    return size;
  }

  static Iterable<RowData> decode(byte[] encoded, TypeSerializer<RowData> rows) {
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
        public RowData next() {
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
