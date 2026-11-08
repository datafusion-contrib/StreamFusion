package tech.streamfusion.state;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.NoSuchElementException;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.runtime.state.CheckpointableKeyedStateBackend;
import org.apache.flink.runtime.state.CompositeKeySerializationUtils;
import org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot;
import org.apache.flink.streaming.api.operators.TimerHeapInternalTimer;
import org.apache.flink.util.CloseableIterator;

/** Reads canonical timer bytes without depending on a live priority queue's iteration cache. */
final class StockTimerSnapshot {
  private StockTimerSnapshot() {}

  @SuppressWarnings("unchecked")
  static <K, N> CloseableIterator<TimerHeapInternalTimer<K, N>> iterator(
      CheckpointableKeyedStateBackend<K> backend, String name) throws Exception {
    var resources = backend.savepoint().getSnapshotResources();
    try {
      var metadata = resources.getMetaInfoSnapshots();
      int selected = -1;
      TypeSerializer<TimerHeapInternalTimer<K, N>> serializer = null;
      for (int index = 0; index < metadata.size(); index++) {
        var state = metadata.get(index);
        if (state.getName().equals(name)
            && state.getBackendStateType()
                == StateMetaInfoSnapshot.BackendStateType.PRIORITY_QUEUE) {
          selected = index;
          serializer =
              (TypeSerializer<TimerHeapInternalTimer<K, N>>)
                  (TypeSerializer<?>)
                      state
                          .getTypeSerializerSnapshot(
                              StateMetaInfoSnapshot.CommonSerializerKeys.VALUE_SERIALIZER)
                          .restoreSerializer();
        }
      }
      if (selected < 0) {
        resources.release();
        return CloseableIterator.empty();
      }
      final int stateId = selected;
      final var timers = serializer;
      var entries = resources.createKVStateIterator();
      int prefix =
          CompositeKeySerializationUtils.computeRequiredBytesInKeyGroupPrefix(
              ((org.apache.flink.runtime.state.AbstractKeyedStateBackend<K>) backend)
                  .getNumberOfKeyGroups());
      return new CloseableIterator<>() {
        private boolean closed;

        @Override
        public boolean hasNext() {
          try {
            while (!closed && entries.isValid() && entries.kvStateId() != stateId) entries.next();
            return !closed && entries.isValid();
          } catch (IOException failure) {
            throw new UncheckedIOException(failure);
          }
        }

        @Override
        public TimerHeapInternalTimer<K, N> next() {
          if (!hasNext()) throw new NoSuchElementException();
          try {
            byte[] key = entries.key();
            var result =
                timers.deserialize(new DataInputDeserializer(key, prefix, key.length - prefix));
            entries.next();
            return result;
          } catch (IOException failure) {
            throw new UncheckedIOException(failure);
          }
        }

        @Override
        public void close() throws Exception {
          if (closed) return;
          closed = true;
          try {
            entries.close();
          } finally {
            resources.release();
          }
        }
      };
    } catch (Exception | Error failure) {
      try {
        resources.release();
      } catch (Throwable cleanup) {
        failure.addSuppressed(cleanup);
      }
      throw failure;
    }
  }
}
