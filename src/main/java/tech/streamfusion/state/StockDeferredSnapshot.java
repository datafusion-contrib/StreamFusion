package tech.streamfusion.state;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.AbstractList;
import java.util.List;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.*;
import org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.function.SupplierWithException;

/** Runs existing stock-state adapters against an independently owned native read view. */
public final class StockDeferredSnapshot implements FullSnapshotResources<RowData> {
  /** Handle ownership stays under the snapshot monitor, including failed asynchronous creation. */
  public static final class NativeReader implements AutoCloseable {
    private long handle;
    private final SupplierWithException<Long, Exception> factory;
    private final java.util.function.LongConsumer dispose;
    private boolean closed;

    private NativeReader(
        long handle,
        SupplierWithException<Long, Exception> factory,
        java.util.function.LongConsumer dispose) {
      this.handle = handle;
      this.factory = factory;
      this.dispose = dispose;
    }

    public static NativeReader captured(long handle, java.util.function.LongConsumer dispose) {
      if (handle == 0) throw new IllegalArgumentException("captured native handle is missing");
      return new NativeReader(handle, null, dispose);
    }

    public static NativeReader deferred(
        SupplierWithException<Long, Exception> factory, java.util.function.LongConsumer dispose) {
      return new NativeReader(0, factory, dispose);
    }

    private long handle() throws Exception {
      if (closed) throw new IllegalStateException("native canonical reader is closed");
      if (handle == 0) {
        handle = factory.get();
        if (handle == 0)
          throw new IllegalStateException("native canonical reader returned no handle");
      }
      return handle;
    }

    @Override
    public void close() {
      if (closed) return;
      closed = true;
      long owned = handle;
      handle = 0;
      if (owned != 0) dispose.accept(owned);
    }
  }

  private final TypeSerializer<RowData> keys;
  private final KeyGroupRange groups;
  private final StockStateContract contract;
  private final NativeReader reader;
  private final long snapshot;
  private final java.util.concurrent.atomic.AtomicBoolean released =
      new java.util.concurrent.atomic.AtomicBoolean();
  private final boolean persistent;
  private final SupplierWithException<CanonicalProjection, Exception> factory;
  private final AutoCloseable owner;
  private CanonicalProjection projection;
  private FullSnapshotResources<RowData> converted;
  private boolean closed;
  private final List<StateMetaInfoSnapshot> metadata =
      new AbstractList<>() {
        // Flink checks emptiness before scheduling the async writer. A captured view may be empty;
        // conservatively schedule it, then expose its real metadata on the writer thread.
        @Override
        public boolean isEmpty() {
          return false;
        }

        @Override
        public int size() {
          return resources().getMetaInfoSnapshots().size();
        }

        @Override
        public StateMetaInfoSnapshot get(int index) {
          return resources().getMetaInfoSnapshots().get(index);
        }
      };

  public StockDeferredSnapshot(
      TypeSerializer<RowData> keys,
      KeyGroupRange groups,
      StockStateContract contract,
      NativeReader reader,
      long snapshot,
      boolean persistent,
      SupplierWithException<CanonicalProjection, Exception> factory,
      AutoCloseable owner) {
    this.keys = keys.duplicate();
    this.groups = groups;
    this.contract = contract;
    this.reader = reader;
    this.snapshot = snapshot;
    this.persistent = persistent;
    this.factory = factory;
    this.owner = owner;
  }

  private synchronized FullSnapshotResources<RowData> resources() {
    if (closed) throw new IllegalStateException("canonical read view is closed");
    if (converted == null) {
      try {
        projection = factory.get();
        var backend = projection.backend();
        tech.streamfusion.Native.withCanonicalDatabases(
            snapshot,
            () -> {
              try {
                contract.exportState(backend, reader.handle(), persistent);
              } catch (Exception failure) {
                throw new org.apache.flink.util.FlinkRuntimeException(failure);
              }
              return 0;
            });
        converted = backend.savepoint().getSnapshotResources();
      } catch (Exception failure) {
        throw new UncheckedIOException(
            new IOException("Cannot convert captured native state", failure));
      }
    }
    return converted;
  }

  @Override
  public List<StateMetaInfoSnapshot> getMetaInfoSnapshots() {
    return metadata;
  }

  @Override
  public KeyGroupRange getKeyGroupRange() {
    return groups;
  }

  @Override
  public TypeSerializer<RowData> getKeySerializer() {
    return keys;
  }

  @Override
  public StreamCompressionDecorator getStreamCompressionDecorator() {
    return resources().getStreamCompressionDecorator();
  }

  @Override
  public KeyValueStateIterator createKVStateIterator() throws IOException {
    return resources().createKVStateIterator();
  }

  @Override
  public void release() {
    if (!released.compareAndSet(false, true)) return;
    tech.streamfusion.Native.cancelCanonicalDatabases(snapshot);
    synchronized (this) {
      closed = true;
      Throwable failure = null;
      for (AutoCloseable resource :
          new AutoCloseable[] {
            converted == null ? null : converted::release, projection, reader, owner
          }) {
        try {
          if (resource != null) resource.close();
        } catch (Throwable cleanup) {
          if (failure == null) failure = cleanup;
          else failure.addSuppressed(cleanup);
        }
      }
      if (failure != null) org.apache.flink.util.ExceptionUtils.rethrow(failure);
    }
  }
}
