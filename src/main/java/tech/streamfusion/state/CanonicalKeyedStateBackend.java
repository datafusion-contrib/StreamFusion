package tech.streamfusion.state;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.RunnableFuture;
import java.util.stream.Stream;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.state.*;
import org.apache.flink.runtime.state.StateSnapshotTransformer.StateSnapshotTransformFactory;
import org.apache.flink.runtime.state.heap.HeapPriorityQueueElement;

/** Ties the canonical view to the backend's task lifetime, including failed initialization. */
public final class CanonicalKeyedStateBackend<K>
    implements CheckpointableKeyedStateBackend<K>, CheckpointListener {
  private final CheckpointableKeyedStateBackend<K> delegate;
  private final CanonicalProjection projection;
  private org.apache.flink.util.function.SupplierWithException<SavepointResources<?>, Exception>
      canonicalSnapshot;
  private org.apache.flink.util.function.ThrowingRunnable<Exception> nativeSnapshot;

  CanonicalKeyedStateBackend(
      CheckpointableKeyedStateBackend<K> delegate, CanonicalProjection projection) {
    this.delegate = delegate;
    this.projection = projection;
  }

  public CheckpointableKeyedStateBackend<K> delegate() {
    return delegate;
  }

  public CanonicalProjection projection() {
    return projection;
  }

  public void configureSnapshots(
      org.apache.flink.util.function.SupplierWithException<SavepointResources<?>, Exception>
          canonicalSnapshot,
      org.apache.flink.util.function.ThrowingRunnable<Exception> nativeSnapshot) {
    this.canonicalSnapshot = java.util.Objects.requireNonNull(canonicalSnapshot);
    this.nativeSnapshot = java.util.Objects.requireNonNull(nativeSnapshot);
  }

  @Override
  public void close() throws IOException {
    Throwable failure = null;
    try {
      delegate.close();
    } catch (Throwable error) {
      failure = error;
    }
    try {
      projection.close();
    } catch (Throwable error) {
      if (failure == null) failure = error;
      else failure.addSuppressed(error);
    }
    if (failure != null) org.apache.flink.util.ExceptionUtils.rethrowIOException(failure);
  }

  @Override
  public void dispose() {
    Throwable failure = null;
    try {
      delegate.dispose();
    } catch (Throwable error) {
      failure = error;
    }
    try {
      projection.close();
    } catch (Throwable error) {
      if (failure == null) failure = error;
      else failure.addSuppressed(error);
    }
    if (failure != null) org.apache.flink.util.ExceptionUtils.rethrow(failure);
  }

  @Override
  public void setCurrentKey(K key) {
    delegate.setCurrentKey(key);
  }

  @Override
  public K getCurrentKey() {
    return delegate.getCurrentKey();
  }

  public void setCurrentKeyAndKeyGroup(K key, int group) {
    tech.streamfusion.compat.StateCompat.setCurrentKeyAndGroup(delegate, key, group);
  }

  @Override
  public TypeSerializer<K> getKeySerializer() {
    return delegate.getKeySerializer();
  }

  @Override
  public KeyGroupRange getKeyGroupRange() {
    return delegate.getKeyGroupRange();
  }

  @Override
  @SuppressWarnings({"unchecked", "rawtypes"})
  public SavepointResources<K> savepoint() throws Exception {
    return canonicalSnapshot == null
        ? delegate.savepoint()
        : (SavepointResources) canonicalSnapshot.get();
  }

  @Override
  public RunnableFuture<SnapshotResult<KeyedStateHandle>> snapshot(
      long id, long timestamp, CheckpointStreamFactory factory, CheckpointOptions options)
      throws Exception {
    if (nativeSnapshot != null) nativeSnapshot.run();
    return delegate.snapshot(id, timestamp, factory, options);
  }

  @Override
  public void notifyCheckpointComplete(long id) throws Exception {
    if (delegate instanceof CheckpointListener)
      ((CheckpointListener) delegate).notifyCheckpointComplete(id);
  }

  @Override
  public void notifyCheckpointAborted(long id) throws Exception {
    if (delegate instanceof CheckpointListener)
      ((CheckpointListener) delegate).notifyCheckpointAborted(id);
  }

  @Override
  public <N, S extends State, T> void applyToAllKeys(
      N namespace,
      TypeSerializer<N> serializer,
      StateDescriptor<S, T> descriptor,
      KeyedStateFunction<K, S> function)
      throws Exception {
    delegate.applyToAllKeys(namespace, serializer, descriptor, function);
  }

  @Override
  public <N> Stream<K> getKeys(String name, N namespace) {
    return delegate.getKeys(name, namespace);
  }

  public <N> Stream<K> getKeys(List<String> names, N namespace) {
    return tech.streamfusion.compat.StateCompat.keys(delegate, names, namespace);
  }

  @Override
  public <N> Stream<Tuple2<K, N>> getKeysAndNamespaces(String name) {
    return delegate.getKeysAndNamespaces(name);
  }

  @Override
  public <N, S extends State, T> S getOrCreateKeyedState(
      TypeSerializer<N> serializer, StateDescriptor<S, T> descriptor) throws Exception {
    return delegate.getOrCreateKeyedState(serializer, descriptor);
  }

  @Override
  public <N, S extends State> S getPartitionedState(
      N namespace, TypeSerializer<N> serializer, StateDescriptor<S, ?> descriptor)
      throws Exception {
    return delegate.getPartitionedState(namespace, serializer, descriptor);
  }

  @Override
  public <N, SV, SEV, S extends State, IS extends S> IS createOrUpdateInternalState(
      TypeSerializer<N> serializer,
      StateDescriptor<S, SV> descriptor,
      StateSnapshotTransformFactory<SEV> transform)
      throws Exception {
    return delegate.createOrUpdateInternalState(serializer, descriptor, transform);
  }

  @Override
  public <T extends HeapPriorityQueueElement & PriorityComparable<? super T> & Keyed<?>>
      KeyGroupedInternalPriorityQueue<T> create(String name, TypeSerializer<T> serializer) {
    return delegate.create(name, serializer);
  }

  @Override
  public void registerKeySelectionListener(KeySelectionListener<K> listener) {
    delegate.registerKeySelectionListener(listener);
  }

  @Override
  public boolean deregisterKeySelectionListener(KeySelectionListener<K> listener) {
    return delegate.deregisterKeySelectionListener(listener);
  }

  @Override
  public boolean isSafeToReuseKVState() {
    return delegate.isSafeToReuseKVState();
  }

  public String getBackendTypeIdentifier() {
    return tech.streamfusion.compat.StateCompat.backendType(delegate);
  }
}
