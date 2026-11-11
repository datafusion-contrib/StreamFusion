package tech.streamfusion.state;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.RunnableFuture;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.OperatorStateBackend;
import org.apache.flink.runtime.state.OperatorStateHandle;
import org.apache.flink.runtime.state.SnapshotResult;
import org.apache.flink.util.ResourceGuard;

/** Retains managed operator state through either released Flink snapshot entry point. */
final class CanonicalOperatorStateBackend
    extends tech.streamfusion.compat.FlinkOperatorStateBackendCompat {
  private CloseableRegistry cancelables;
  private org.apache.flink.util.function.SupplierWithException<AutoCloseable, Exception> retain;
  private final ResourceGuard snapshots = new ResourceGuard();

  CanonicalOperatorStateBackend(OperatorStateBackend delegate) {
    super(delegate);
  }

  void configureSnapshots(
      CloseableRegistry cancelables,
      org.apache.flink.util.function.SupplierWithException<AutoCloseable, Exception> retain) {
    this.cancelables = java.util.Objects.requireNonNull(cancelables);
    this.retain = java.util.Objects.requireNonNull(retain);
  }

  @Override
  public RunnableFuture<SnapshotResult<OperatorStateHandle>> snapshot(
      long id, long timestamp, CheckpointStreamFactory streams, CheckpointOptions options)
      throws Exception {
    if (!options.getCheckpointType().isSavepoint()
        || ((org.apache.flink.runtime.checkpoint.SavepointType) options.getCheckpointType())
                .getFormatType()
            != org.apache.flink.core.execution.SavepointFormatType.CANONICAL)
      return delegate.snapshot(id, timestamp, streams, options);
    var lease = snapshots.acquireResource();
    RunnableFuture<SnapshotResult<OperatorStateHandle>> future = null;
    AutoCloseable nativeLease = null;
    try {
      nativeLease = retain.get();
      future = delegate.snapshot(id, timestamp, streams, options);
      var retained = nativeLease;
      nativeLease = null;
      return StockCanonicalSnapshot.retainOperatorSnapshot(
          future,
          () ->
              () -> {
                try {
                  retained.close();
                } finally {
                  lease.close();
                }
              },
          cancelables);
    } catch (Exception | Error failure) {
      try {
        if (future != null) future.cancel(true);
      } catch (Throwable cleanup) {
        failure.addSuppressed(cleanup);
      } finally {
        org.apache.flink.util.IOUtils.closeQuietly(nativeLease);
        lease.close();
      }
      throw failure;
    }
  }

  @Override
  public void close() throws IOException {
    snapshots.close();
    delegate.close();
  }

  @Override
  public void dispose() {
    snapshots.close();
    delegate.dispose();
  }

  @Override
  public <K, V> BroadcastState<K, V> getBroadcastState(MapStateDescriptor<K, V> descriptor)
      throws Exception {
    return delegate.getBroadcastState(descriptor);
  }

  @Override
  public <S> ListState<S> getListState(ListStateDescriptor<S> descriptor) throws Exception {
    return delegate.getListState(descriptor);
  }

  @Override
  public <S> ListState<S> getUnionListState(ListStateDescriptor<S> descriptor) throws Exception {
    return delegate.getUnionListState(descriptor);
  }

  @Override
  public Set<String> getRegisteredStateNames() {
    return delegate.getRegisteredStateNames();
  }

  @Override
  public Set<String> getRegisteredBroadcastStateNames() {
    return delegate.getRegisteredBroadcastStateNames();
  }
}
