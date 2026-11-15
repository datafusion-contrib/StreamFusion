package tech.streamfusion.state;

import org.apache.flink.runtime.state.CheckpointableKeyedStateBackend;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.function.SupplierWithException;

/** Owns the stock-schema view separately from the native batch-key backend. */
public final class CanonicalProjection implements java.io.Closeable {
  private final SupplierWithException<CheckpointableKeyedStateBackend<RowData>, Exception> factory;
  private final SupplierWithException<CheckpointableKeyedStateBackend<RowData>, Exception>
      emptyFactory;
  private final boolean restored;
  private CheckpointableKeyedStateBackend<RowData> backend;
  private boolean closed;

  CanonicalProjection(
      SupplierWithException<CheckpointableKeyedStateBackend<RowData>, Exception> factory,
      boolean restored)
      throws Exception {
    this(factory, factory, restored);
  }

  CanonicalProjection(
      SupplierWithException<CheckpointableKeyedStateBackend<RowData>, Exception> factory,
      SupplierWithException<CheckpointableKeyedStateBackend<RowData>, Exception> emptyFactory,
      boolean restored)
      throws Exception {
    this.emptyFactory = emptyFactory;
    this.factory = factory;
    this.restored = restored;
    if (restored) backend();
  }

  public CanonicalProjection fresh() throws Exception {
    return new CanonicalProjection(emptyFactory, emptyFactory, false);
  }

  public boolean isRestored() {
    return restored;
  }

  public synchronized CheckpointableKeyedStateBackend<RowData> backend() throws Exception {
    if (closed) throw new IllegalStateException("canonical projection is closed");
    if (backend == null) backend = factory.get();
    return backend;
  }

  @Override
  public synchronized void close() throws java.io.IOException {
    if (closed) return;
    closed = true;
    if (backend != null) {
      Exception failure = null;
      try {
        backend.close();
      } catch (Exception error) {
        failure = error;
      }
      try {
        backend.dispose();
      } catch (Exception error) {
        if (failure == null) failure = error;
        else failure.addSuppressed(error);
      }
      backend = null;
      if (failure != null)
        throw new java.io.IOException("Cannot close canonical state view", failure);
    }
  }
}
