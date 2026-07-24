package io.github.jordepic.streamfusion.state;

import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.CheckpointableKeyedStateBackend;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupedInternalPriorityQueue;
import org.apache.flink.runtime.state.Keyed;
import org.apache.flink.runtime.state.KeyedStateFunction;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.PriorityComparable;
import org.apache.flink.runtime.state.SavepointResources;
import org.apache.flink.runtime.state.SnapshotResult;
import org.apache.flink.runtime.state.SnapshotStrategyRunner;
import org.apache.flink.runtime.state.StateSnapshotTransformer.StateSnapshotTransformFactory;
import org.apache.flink.runtime.state.heap.HeapPriorityQueueElement;
import org.apache.flink.util.FileUtils;

import javax.annotation.Nonnull;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.RunnableFuture;
import java.util.function.Function;
import java.util.stream.Stream;

import static org.apache.flink.runtime.state.SnapshotExecutionType.ASYNCHRONOUS;

/**
 * The keyed state backend given to every keyed operator when the Paimon state backend is selected.
 * JVM-side keyed state (descriptors, timer queues) delegates untouched to the wrapped backend; a
 * native operator whose state lives in a local Paimon table registers its checkpoint hook here in
 * {@code initializeState}, and from then on this backend's snapshot is the operator's Paimon
 * commit, emitted as an {@link org.apache.flink.runtime.state.IncrementalRemoteKeyedStateHandle}.
 *
 * <p>The two channels are exclusive by construction: an operator that registered a native hook
 * must not also create JVM keyed state (there is exactly one keyed-state handle per operator per
 * checkpoint), and this backend fails fast if both are used.
 */
public final class PaimonKeyedStateBackend<K>
    implements CheckpointableKeyedStateBackend<K>, CheckpointListener {

  private final CheckpointableKeyedStateBackend<K> delegate;
  private final PaimonSnapshotStrategy snapshotStrategy;
  private final File workingDirectory;
  private final File tableDirectory;
  private final List<PaimonRestoredSource> restoredSources;
  private final CloseableRegistry cancelStreamRegistry = new CloseableRegistry();

  private boolean delegateStateUsed;

  PaimonKeyedStateBackend(
      CheckpointableKeyedStateBackend<K> delegate,
      PaimonSnapshotStrategy snapshotStrategy,
      File workingDirectory,
      List<PaimonRestoredSource> restoredSources) {
    this.delegate = delegate;
    this.snapshotStrategy = snapshotStrategy;
    this.workingDirectory = workingDirectory;
    this.tableDirectory = new File(workingDirectory, "table");
    this.restoredSources = restoredSources;
  }

  // ---- The native operator's surface -----------------------------------------------------------

  /** The local directory the operator's Paimon table lives in (created by the native side). */
  public String tableDirectory() {
    return tableDirectory.getAbsolutePath();
  }

  /** Restored checkpoint tables to adopt buckets from; empty on a fresh start. */
  public List<PaimonRestoredSource> restoredSources() {
    return restoredSources;
  }

  /**
   * Whether an external compactor (the Java Paimon glue module) owns table maintenance — the
   * operator then disables the native store's fallback compaction.
   */
  public boolean hasExternalCompactor() {
    return snapshotStrategy.hasExternalCompactor();
  }

  /** Registers the operator's native checkpoint hook; snapshots then go through Paimon commits. */
  public void registerNativeState(PaimonNativeState nativeState) {
    if (snapshotStrategy.hasNativeState()) {
      throw new IllegalStateException("a native state hook is already registered");
    }
    if (delegateStateUsed) {
      throw new IllegalStateException(
          "operator created JVM keyed state before registering native Paimon state; "
              + "the two channels are exclusive");
    }
    snapshotStrategy.registerNativeState(nativeState);
  }

  // ---- Snapshot ---------------------------------------------------------------------------------

  @Nonnull
  @Override
  public RunnableFuture<SnapshotResult<KeyedStateHandle>> snapshot(
      long checkpointId,
      long timestamp,
      @Nonnull CheckpointStreamFactory streamFactory,
      @Nonnull CheckpointOptions checkpointOptions)
      throws Exception {
    if (!snapshotStrategy.hasNativeState()) {
      return delegate.snapshot(checkpointId, timestamp, streamFactory, checkpointOptions);
    }
    if (delegateStateUsed) {
      throw new IllegalStateException(
          "operator holds both native Paimon state and JVM keyed state; cannot snapshot");
    }
    return new SnapshotStrategyRunner<>(
            "Paimon incremental snapshot", snapshotStrategy, cancelStreamRegistry, ASYNCHRONOUS)
        .snapshot(checkpointId, timestamp, streamFactory, checkpointOptions);
  }

  @Override
  public void notifyCheckpointComplete(long checkpointId) throws Exception {
    snapshotStrategy.notifyCheckpointComplete(checkpointId);
    if (delegate instanceof CheckpointListener) {
      ((CheckpointListener) delegate).notifyCheckpointComplete(checkpointId);
    }
  }

  @Override
  public void notifyCheckpointAborted(long checkpointId) throws Exception {
    snapshotStrategy.notifyCheckpointAborted(checkpointId);
    if (delegate instanceof CheckpointListener) {
      ((CheckpointListener) delegate).notifyCheckpointAborted(checkpointId);
    }
  }

  @Nonnull
  @Override
  public SavepointResources<K> savepoint() throws Exception {
    if (snapshotStrategy.hasNativeState()) {
      throw new UnsupportedOperationException(
          "canonical savepoints are not supported for native Paimon state; "
              + "use native-format savepoints");
    }
    return delegate.savepoint();
  }

  // ---- Lifecycle --------------------------------------------------------------------------------

  @Override
  public void dispose() {
    delegate.dispose();
    deleteWorkingDirectory();
  }

  @Override
  public void close() throws IOException {
    cancelStreamRegistry.close();
    delegate.close();
    deleteWorkingDirectory();
  }

  private void deleteWorkingDirectory() {
    try {
      FileUtils.deleteDirectory(workingDirectory);
    } catch (IOException e) {
      // The TM working directory is cleaned up on process exit; a leak here is not fatal.
    }
  }

  // ---- Pure delegation --------------------------------------------------------------------------

  @Override
  public KeyGroupRange getKeyGroupRange() {
    return delegate.getKeyGroupRange();
  }

  @Override
  public void setCurrentKey(K newKey) {
    delegate.setCurrentKey(newKey);
  }

  @Override
  public K getCurrentKey() {
    return delegate.getCurrentKey();
  }

  @Override
  public void setCurrentKeyAndKeyGroup(K newKey, int newKeyGroupIndex) {
    delegate.setCurrentKeyAndKeyGroup(newKey, newKeyGroupIndex);
  }

  @Override
  public TypeSerializer<K> getKeySerializer() {
    return delegate.getKeySerializer();
  }

  @Override
  public <N, S extends State, T> void applyToAllKeys(
      N namespace,
      TypeSerializer<N> namespaceSerializer,
      StateDescriptor<S, T> stateDescriptor,
      KeyedStateFunction<K, S> function)
      throws Exception {
    delegateStateUsed = true;
    delegate.applyToAllKeys(namespace, namespaceSerializer, stateDescriptor, function);
  }

  @Override
  public <N> Stream<K> getKeys(String state, N namespace) {
    return delegate.getKeys(state, namespace);
  }

  @Override
  public <N> Stream<K> getKeys(List<String> states, N namespace) {
    return delegate.getKeys(states, namespace);
  }

  @Override
  public <N> Stream<Tuple2<K, N>> getKeysAndNamespaces(String state) {
    return delegate.getKeysAndNamespaces(state);
  }

  @Override
  public <N, S extends State, T> S getOrCreateKeyedState(
      TypeSerializer<N> namespaceSerializer, StateDescriptor<S, T> stateDescriptor)
      throws Exception {
    delegateStateUsed = true;
    return delegate.getOrCreateKeyedState(namespaceSerializer, stateDescriptor);
  }

  @Override
  public <N, S extends State> S getPartitionedState(
      N namespace, TypeSerializer<N> namespaceSerializer, StateDescriptor<S, ?> stateDescriptor)
      throws Exception {
    delegateStateUsed = true;
    return delegate.getPartitionedState(namespace, namespaceSerializer, stateDescriptor);
  }

  @Override
  public void registerKeySelectionListener(KeySelectionListener<K> listener) {
    delegate.registerKeySelectionListener(listener);
  }

  @Override
  public boolean deregisterKeySelectionListener(KeySelectionListener<K> listener) {
    return delegate.deregisterKeySelectionListener(listener);
  }

  @Nonnull
  @Override
  public <N, SV, SEV, S extends State, IS extends S> IS createOrUpdateInternalState(
      @Nonnull TypeSerializer<N> namespaceSerializer,
      @Nonnull StateDescriptor<S, SV> stateDesc,
      @Nonnull StateSnapshotTransformFactory<SEV> snapshotTransformFactory)
      throws Exception {
    delegateStateUsed = true;
    return delegate.createOrUpdateInternalState(
        namespaceSerializer, stateDesc, snapshotTransformFactory);
  }

  @Nonnull
  @Override
  public <T extends HeapPriorityQueueElement & PriorityComparable<? super T> & Keyed<?>>
      KeyGroupedInternalPriorityQueue<T> create(
          @Nonnull String stateName, @Nonnull TypeSerializer<T> byteOrderedElementSerializer) {
    delegateStateUsed = true;
    return delegate.create(stateName, byteOrderedElementSerializer);
  }

  @Override
  public boolean isSafeToReuseKVState() {
    return delegate.isSafeToReuseKVState();
  }

  @Override
  public String getBackendTypeIdentifier() {
    return delegate.getBackendTypeIdentifier();
  }
}
