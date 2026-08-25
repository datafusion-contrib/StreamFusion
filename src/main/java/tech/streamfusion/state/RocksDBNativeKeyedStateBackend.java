package tech.streamfusion.state;

import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.runtime.memory.OpaqueMemoryResource;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.CheckpointableKeyedStateBackend;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
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
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.function.SupplierWithException;

import javax.annotation.Nonnull;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.RunnableFuture;
import java.util.stream.Stream;

import static org.apache.flink.runtime.state.SnapshotExecutionType.ASYNCHRONOUS;

/**
 * The keyed state backend given to every keyed operator when the RocksDB state backend is selected.
 * JVM-side keyed state (descriptors, timer queues) delegates untouched to the wrapped backend; a
 * native operator whose state lives in a local RocksDB table registers its checkpoint hook here in
 * {@code initializeState}, and from then on this backend's snapshot is the operator's RocksDB
 * commit, emitted as an {@link org.apache.flink.runtime.state.IncrementalRemoteKeyedStateHandle}.
 *
 * <p>The delegate backend materializes lazily: a native-state operator that never touches JVM keyed
 * state never pays for opening (and later deleting) an unused RocksDB instance. Metadata the
 * wrapper owns — key-group range, key serializer, the current key — is answered locally until then;
 * the first call that genuinely needs the delegate (state creation or access, timer queues,
 * snapshot or savepoint without a native hook) creates it and replays the buffered key context.
 * Restored JVM state forces eager materialization so it is re-snapshotted even when untouched.
 *
 * <p>The two channels are exclusive by construction: an operator that registered a native hook
 * must not also create JVM keyed state (there is exactly one keyed-state handle per operator per
 * checkpoint), and this backend fails fast if both are used.
 */
public final class RocksDBNativeKeyedStateBackend<K>
    implements CheckpointableKeyedStateBackend<K>, CheckpointListener {

  private final RocksDBNativeSnapshotStrategy snapshotStrategy;
  private final File workingDirectory;
  private final File tableDirectory;
  private final List<RocksDBRestoredSource> restoredSources;
  private final String optionsJson;
  private final OpaqueMemoryResource<NativeRocksSharedResources> sharedResources;
  private final CloseableRegistry cancelStreamRegistry = new CloseableRegistry();
  private final KeyGroupRange keyGroupRange;
  private final TypeSerializer<K> keySerializer;
  private final int numberOfKeyGroups;

  private SupplierWithException<CheckpointableKeyedStateBackend<K>, Exception> delegateSupplier;
  private CheckpointableKeyedStateBackend<K> delegate;
  private K bufferedKey;
  private int bufferedKeyGroup;

  private boolean delegateStateUsed;
  private boolean sharedResourcesReleased;

  RocksDBNativeKeyedStateBackend(
      SupplierWithException<CheckpointableKeyedStateBackend<K>, Exception> delegateSupplier,
      KeyGroupRange keyGroupRange,
      TypeSerializer<K> keySerializer,
      int numberOfKeyGroups,
      RocksDBNativeSnapshotStrategy snapshotStrategy,
      File workingDirectory,
      List<RocksDBRestoredSource> restoredSources,
      String optionsJson,
      OpaqueMemoryResource<NativeRocksSharedResources> sharedResources) {
    this.delegateSupplier = delegateSupplier;
    this.keyGroupRange = keyGroupRange;
    this.keySerializer = keySerializer;
    this.numberOfKeyGroups = numberOfKeyGroups;
    this.snapshotStrategy = snapshotStrategy;
    this.workingDirectory = workingDirectory;
    this.tableDirectory = new File(workingDirectory, "db");
    this.restoredSources = restoredSources;
    this.optionsJson = optionsJson;
    this.sharedResources = sharedResources;
    this.bufferedKeyGroup =
        keyGroupRange.getNumberOfKeyGroups() > 0 ? keyGroupRange.getStartKeyGroup() : 0;
  }

  // ---- Delegate materialization -----------------------------------------------------------------

  private CheckpointableKeyedStateBackend<K> delegate() throws Exception {
    if (delegate == null) {
      delegate = delegateSupplier.get();
      delegateSupplier = null;
      if (bufferedKey != null) {
        delegate.setCurrentKeyAndKeyGroup(bufferedKey, bufferedKeyGroup);
      }
    }
    return delegate;
  }

  private CheckpointableKeyedStateBackend<K> delegateUnchecked() {
    try {
      return delegate();
    } catch (Exception failure) {
      throw new FlinkRuntimeException("failed to create the delegate keyed state backend", failure);
    }
  }

  void materializeDelegate() throws Exception {
    delegate();
  }

  // ---- The native operator's surface -----------------------------------------------------------

  /** The local directory the operator's RocksDB table lives in (created by the native side). */
  public String tableDirectory() {
    return tableDirectory.getAbsolutePath();
  }

  /** Restored checkpoint tables to adopt buckets from; empty on a fresh start. */
  public List<RocksDBRestoredSource> restoredSources() {
    return restoredSources;
  }

  public String optionsJson() {
    return optionsJson;
  }

  /** The slot's shared native memory pool, or 0 when the job runs without one. */
  public long sharedResourcesHandle() {
    return sharedResources == null ? 0 : sharedResources.getResourceHandle().nativeHandle();
  }

  /**
   * Registers the operator's native checkpoint hook; snapshots then go through RocksDB commits.
   * The operator's idle-state retention (0 = off) rides along so table maintenance can
   * physically drop rows the read path already treats as expired.
   */
  public void registerNativeState(RocksDBNativeState nativeState, long stateTtlMillis) {
    if (snapshotStrategy.hasNativeState()) {
      throw new IllegalStateException("a native state hook is already registered");
    }
    if (delegateStateUsed) {
      throw new IllegalStateException(
          "operator created JVM keyed state before registering native RocksDB state; "
              + "the two channels are exclusive");
    }
    snapshotStrategy.registerNativeState(nativeState, stateTtlMillis);
  }

  /**
   * Reads and removes StreamFusion's reserved canonical state without claiming JVM state use. An
   * unmaterialized delegate is provably empty (restored delegate state materializes eagerly), so
   * this never opens the delegate just to find nothing.
   */
  public CanonicalRestore restoreCanonicalState(String operatorId) throws Exception {
    if (delegate == null) {
      return new CanonicalRestore(List.of(), Long.MIN_VALUE);
    }
    CanonicalNativeState.Restore restored = CanonicalNativeState.readAndClear(delegate, operatorId);
    return new CanonicalRestore(restored.partitions, restored.timerDeadline);
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
      return delegate().snapshot(checkpointId, timestamp, streamFactory, checkpointOptions);
    }
    if (delegateStateUsed) {
      throw new IllegalStateException(
          "operator holds both native RocksDB state and JVM keyed state; cannot snapshot");
    }
    // The sync phase decides per-file reuse (and links what the upload will read), so it needs
    // the options and factory the runner interface only hands to the async phase.
    snapshotStrategy.beforeSnapshot(checkpointOptions, streamFactory);
    return new SnapshotStrategyRunner<>(
            "RocksDB incremental snapshot", snapshotStrategy, cancelStreamRegistry, ASYNCHRONOUS)
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
      RocksDBNativeState nativeState = snapshotStrategy.nativeState();
      CanonicalNativeState.write(
          delegate(),
          nativeState.canonicalPartitions(),
          nativeState.canonicalOperatorId(),
          nativeState.canonicalTimerDeadline());
    }
    return delegate().savepoint();
  }

  public static final class CanonicalRestore {
    public final List<byte[]> partitions;
    public final long timerDeadline;

    private CanonicalRestore(List<byte[]> partitions, long timerDeadline) {
      this.partitions = partitions;
      this.timerDeadline = timerDeadline;
    }
  }

  // ---- Lifecycle --------------------------------------------------------------------------------

  @Override
  public void dispose() {
    // Shaping first: its thread must be quiescent before the tables are deleted.
    snapshotStrategy.close();
    if (delegate != null) {
      delegate.dispose();
    }
    deleteWorkingDirectory();
    releaseSharedResources();
  }

  @Override
  public void close() throws IOException {
    snapshotStrategy.close();
    cancelStreamRegistry.close();
    if (delegate != null) {
      delegate.close();
    }
    deleteWorkingDirectory();
    releaseSharedResources();
  }

  /** Returns this backend's lease; the native pool itself dies with the slot's last lease. */
  private void releaseSharedResources() {
    if (sharedResources != null && !sharedResourcesReleased) {
      sharedResourcesReleased = true;
      try {
        sharedResources.close();
      } catch (Exception failure) {
        throw new IllegalStateException("failed to release native RocksDB memory lease", failure);
      }
    }
  }

  private void deleteWorkingDirectory() {
    try {
      FileUtils.deleteDirectory(workingDirectory);
    } catch (IOException e) {
      // The TM working directory is cleaned up on process exit; a leak here is not fatal.
    }
  }

  // ---- Wrapper-owned metadata and key context ---------------------------------------------------

  @Override
  public KeyGroupRange getKeyGroupRange() {
    return keyGroupRange;
  }

  @Override
  public TypeSerializer<K> getKeySerializer() {
    return delegate == null ? keySerializer : delegate.getKeySerializer();
  }

  @Override
  public void setCurrentKey(K newKey) {
    if (delegate == null) {
      bufferedKeyGroup = KeyGroupRangeAssignment.assignToKeyGroup(newKey, numberOfKeyGroups);
      bufferedKey = newKey;
      return;
    }
    delegate.setCurrentKey(newKey);
  }

  @Override
  public K getCurrentKey() {
    return delegate == null ? bufferedKey : delegate.getCurrentKey();
  }

  int getCurrentKeyGroupIndex() {
    if (delegate == null) {
      return bufferedKeyGroup;
    }
    if (delegate instanceof AbstractKeyedStateBackend) {
      return ((AbstractKeyedStateBackend<?>) delegate).getCurrentKeyGroupIndex();
    }
    throw new IllegalStateException(
        "wrapped keyed backend does not expose its current key group: "
            + delegate.getClass().getName());
  }

  void clearCurrentKey() {
    if (delegate == null) {
      bufferedKey = null;
      if (keyGroupRange.getNumberOfKeyGroups() > 0) {
        bufferedKeyGroup = keyGroupRange.getStartKeyGroup();
      }
      return;
    }
    if (delegate instanceof AbstractKeyedStateBackend) {
      CanonicalNativeState.clearKeyContext((AbstractKeyedStateBackend<?>) delegate);
      return;
    }
    throw new IllegalStateException(
        "wrapped keyed backend does not expose its current key context: "
            + delegate.getClass().getName());
  }

  @Override
  public void setCurrentKeyAndKeyGroup(K newKey, int newKeyGroupIndex) {
    if (delegate == null) {
      bufferedKey = newKey;
      bufferedKeyGroup = newKeyGroupIndex;
      return;
    }
    delegate.setCurrentKeyAndKeyGroup(newKey, newKeyGroupIndex);
  }

  // ---- Delegate-materializing state access ------------------------------------------------------

  @Override
  public <N, S extends State, T> void applyToAllKeys(
      N namespace,
      TypeSerializer<N> namespaceSerializer,
      StateDescriptor<S, T> stateDescriptor,
      KeyedStateFunction<K, S> function)
      throws Exception {
    delegateStateUsed = true;
    delegate().applyToAllKeys(namespace, namespaceSerializer, stateDescriptor, function);
  }

  @Override
  public <N> Stream<K> getKeys(String state, N namespace) {
    return delegateUnchecked().getKeys(state, namespace);
  }

  @Override
  public <N> Stream<K> getKeys(List<String> states, N namespace) {
    return delegateUnchecked().getKeys(states, namespace);
  }

  @Override
  public <N> Stream<Tuple2<K, N>> getKeysAndNamespaces(String state) {
    return delegateUnchecked().getKeysAndNamespaces(state);
  }

  @Override
  public <N, S extends State, T> S getOrCreateKeyedState(
      TypeSerializer<N> namespaceSerializer, StateDescriptor<S, T> stateDescriptor)
      throws Exception {
    delegateStateUsed = true;
    return delegate().getOrCreateKeyedState(namespaceSerializer, stateDescriptor);
  }

  @Override
  public <N, S extends State> S getPartitionedState(
      N namespace, TypeSerializer<N> namespaceSerializer, StateDescriptor<S, ?> stateDescriptor)
      throws Exception {
    delegateStateUsed = true;
    return delegate().getPartitionedState(namespace, namespaceSerializer, stateDescriptor);
  }

  @Override
  public void registerKeySelectionListener(KeySelectionListener<K> listener) {
    delegateUnchecked().registerKeySelectionListener(listener);
  }

  @Override
  public boolean deregisterKeySelectionListener(KeySelectionListener<K> listener) {
    return delegate != null && delegate.deregisterKeySelectionListener(listener);
  }

  @Nonnull
  @Override
  public <N, SV, SEV, S extends State, IS extends S> IS createOrUpdateInternalState(
      @Nonnull TypeSerializer<N> namespaceSerializer,
      @Nonnull StateDescriptor<S, SV> stateDesc,
      @Nonnull StateSnapshotTransformFactory<SEV> snapshotTransformFactory)
      throws Exception {
    delegateStateUsed = true;
    return delegate().createOrUpdateInternalState(
        namespaceSerializer, stateDesc, snapshotTransformFactory);
  }

  @Nonnull
  @Override
  public <T extends HeapPriorityQueueElement & PriorityComparable<? super T> & Keyed<?>>
      KeyGroupedInternalPriorityQueue<T> create(
          @Nonnull String stateName, @Nonnull TypeSerializer<T> byteOrderedElementSerializer) {
    delegateStateUsed = true;
    return delegateUnchecked().create(stateName, byteOrderedElementSerializer);
  }

  @Override
  public boolean isSafeToReuseKVState() {
    return delegateUnchecked().isSafeToReuseKVState();
  }

  @Override
  public String getBackendTypeIdentifier() {
    return delegateUnchecked().getBackendTypeIdentifier();
  }
}
