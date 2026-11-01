package tech.streamfusion.operator;

import static org.apache.flink.util.Preconditions.checkState;

import java.util.List;
import java.util.function.BooleanSupplier;
import java.util.function.LongBinaryOperator;
import java.util.function.LongUnaryOperator;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.CDataDictionaryProvider;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.flink.runtime.state.CheckpointableKeyedStateBackend;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.table.types.logical.RowType;
import tech.streamfusion.Native;
import tech.streamfusion.arrow.ArrowConversion;
import tech.streamfusion.arrow.MapListRepresentation;
import tech.streamfusion.compat.FlinkCanonicalStreamOperator;
import tech.streamfusion.compat.RuntimeCompat;
import tech.streamfusion.state.CanonicalKeyedStateBackend;
import tech.streamfusion.state.CanonicalNativeState;
import tech.streamfusion.state.CanonicalProjection;
import tech.streamfusion.state.RocksDBNativeKeyedStateBackend;
import tech.streamfusion.state.RocksDBNativeState;
import tech.streamfusion.state.RocksDBNativeStateSupport;
import tech.streamfusion.state.StockCanonicalSnapshot;
import tech.streamfusion.state.StockStateContract;

/**
 * Lifecycle shared by every native operator whose hot state lives in Rust: the handle, its task
 * off-heap reservation, and the two ways that state can be checkpointed.
 *
 * <p>State travels one of two routes, decided once at {@link #initializeState}. On the native
 * RocksDB backend it checkpoints incrementally through the keyed state backend. Otherwise state
 * stays a Rust hot map and travels as reserved Flink managed keyed state, one versioned payload per
 * non-empty key group, which lets Flink's own protocol redistribute it on rescale. Legacy raw keyed
 * state remains readable.
 *
 * <p>Subclasses supply only the native calls that differ — create, restore, snapshot, close, and
 * the state-size probe — plus optional direct-RocksDB and processing-time-timer hooks. Everything
 * else (budget reservation, restore/snapshot plumbing, handle release) is identical across
 * operators and lives here.
 */
public abstract class AbstractNativeStatefulOperator<OUT>
    extends FlinkCanonicalStreamOperator<OUT> {

  private final String stateLabel;
  private final int[] keyTimestampPrecisions;
  private final int maxParallelism;

  protected transient BufferAllocator allocator;
  protected transient CDataDictionaryProvider dictionaries;
  protected transient long handle;

  private transient boolean rocksdbState;
  private transient boolean directRocksDBState;
  private transient RocksDBNativeStateSupport rocksdbSupport;
  private transient long rocksdbSnapshotStoreHandle;
  private transient NativeMemoryBudget memoryBudget;
  private transient long restoredProcessingTimeTimerDeadline;
  private StockStateContract stockStateContract;
  private transient CanonicalProjection canonicalProjection;

  public void setStockStateContract(StockStateContract contract) {
    stockStateContract = contract;
  }

  @Override
  public <K> org.apache.flink.runtime.state.KeyedStateBackend<K> getKeyedStateBackend() {
    var backend = super.<K>getKeyedStateBackend();
    return backend instanceof CanonicalKeyedStateBackend
        ? ((CanonicalKeyedStateBackend<K>) backend).delegate()
        : backend;
  }

  /**
   * @param stateLabel human-readable operator name, used in backend logs and errors
   * @param keyTimestampPrecisions recursive timestamp descriptors for the grouping key's BinaryRow
   *     layout, in field order — the native side needs them to reproduce Flink's key-group hash
   * @param maxParallelism Flink's max parallelism, mapping native key hashes onto raw key groups
   */
  protected AbstractNativeStatefulOperator(
      String stateLabel, int[] keyTimestampPrecisions, int maxParallelism) {
    this.stateLabel = stateLabel;
    this.keyTimestampPrecisions = keyTimestampPrecisions;
    if (maxParallelism <= 0) {
      throw new IllegalArgumentException(
          "native " + stateLabel + " state requires a positive max parallelism");
    }
    this.maxParallelism = maxParallelism;
  }

  // ---------------------------------------------------------------- native handle (subclass API)

  /** Creates a fresh native handle for an operator starting with no state. */
  protected abstract long createHandle();

  /** Rebuilds the native handle from the raw key-group payloads assigned to this subtask. */
  protected abstract long restoreRawHandle(byte[][] snapshots);

  /** Returns every non-empty raw key group from one native checkpoint pass. */
  protected abstract byte[][] snapshotRawPartitions();

  /** Logical partitions used by canonical savepoints; direct stores may override their exporter. */
  protected byte[][] snapshotCanonicalPartitions() {
    return snapshotRawPartitions();
  }

  /**
   * Captures immutable state for asynchronous backend serialization when an operator supports it.
   */
  protected boolean writeAsyncMemorySnapshot(
      CheckpointableKeyedStateBackend<?> backend, long timerDeadline) throws Exception {
    return false;
  }

  /** Releases the native handle. */
  protected abstract void closeHandle();

  /** The native state's tracked footprint in bytes (zero when unaccounted). */
  protected abstract long stateBytesHandle();

  // --------------------------------------------------------------------- direct RocksDB hooks

  protected RocksDBNativeStateSupport resolveRocksDBState(boolean rawStateRestored) {
    return resolveRocksDB(rawStateRestored, () -> true, 0);
  }

  /** True for an operator with a typed RocksDB data plane instead of key-group snapshot blobs. */
  protected boolean usesDirectRocksDBState() {
    return false;
  }

  /**
   * Creates the operator's typed RocksDB handle. {@code restoredPartitions} carries the key-group
   * blobs of a canonical or raw restore (empty on a fresh start or a native RocksDB restore); the
   * create decodes them once and bulk-writes the state into the typed store, so such a restore
   * still runs direct.
   */
  protected long createRocksDBHandle(
      RocksDBNativeStateSupport rocksdb, byte[][] restoredPartitions) {
    throw new UnsupportedOperationException("operator resolved RocksDB state without a create");
  }

  /**
   * The processing-time timer deadline the typed RocksDB store restored from its checkpoint — the
   * direct-store analog of the snapshot store's deadline. A timer-carrying operator with a typed
   * store overrides this with its handle's accessor.
   */
  protected long rocksdbTimerDeadline() {
    return Long.MIN_VALUE;
  }

  protected String[] checkpointRocksDBHandle(String snapshotDirectory) {
    byte[][] partitions = snapshotRawPartitions();
    long deadline =
        carriesProcessingTimeTimer() ? processingTimeTimerDeadlineForSnapshot() : Long.MIN_VALUE;
    return Native.checkpointRocksDBSnapshotStore(
        rocksdbSnapshotStoreHandle, partitions, deadline, snapshotDirectory);
  }

  protected final RocksDBNativeStateSupport resolveRocksDB(
      boolean rawStateRestored, BooleanSupplier operatorSupported, long stateTtlMillis) {
    return RocksDBNativeStateSupport.resolve(
        getKeyedStateBackend(), stateLabel, rawStateRestored, operatorSupported, stateTtlMillis);
  }

  // -------------------------------------------------------------------- processing-time recovery

  /**
   * Whether raw payloads carry a processing-time cleanup deadline. An operator that arms
   * processing-time timers must copy its deadline into every key group so Flink can redistribute
   * and re-arm it after recovery; a purely watermark-driven one writes the payloads bare.
   */
  protected boolean carriesProcessingTimeTimer() {
    return false;
  }

  /** The latest cleanup deadline to copy into every raw key group at checkpoint time. */
  protected long processingTimeTimerDeadlineForSnapshot() {
    return Long.MIN_VALUE;
  }

  /** The latest native processing-time cleanup deadline restored from the previous checkpoint. */
  protected final long restoredProcessingTimeTimerDeadline() {
    return restoredProcessingTimeTimerDeadline;
  }

  // -------------------------------------------------------------------------------- extra points

  /** Runs before the native handle is created, for state a subclass's create call depends on. */
  protected void beforeHandleCreation() {}

  /** Runs before raw partitions are written, for input a subclass still holds unflushed. */
  protected void beforeSnapshotState() {}

  // ------------------------------------------------------------------------------------ lifecycle

  @Override
  protected final boolean isUsingCustomRawKeyedState() {
    return true;
  }

  @Override
  public void initializeState(StateInitializationContext context) throws Exception {
    var backend = super.getKeyedStateBackend();
    canonicalProjection =
        backend instanceof CanonicalKeyedStateBackend
            ? ((CanonicalKeyedStateBackend<?>) backend).projection()
            : null;
    super.initializeState(context);
    if (stockStateContract != null) {
      stockStateContract.initializeOperatorState(
          context.getOperatorStateStore(), context.isRestored());
      if (context.isRestored()) restoreCanonicalWatermark(stockStateContract.restoredWatermark());
    }
    if (getKeyedStateBackend() != null) {
      int deployedMaxParallelism = RuntimeCompat.maxParallelism(getRuntimeContext());
      checkState(
          maxParallelism == deployedMaxParallelism,
          "Native %s state max parallelism changed after planning: expected %s key groups, deployed"
              + " %s",
          stateLabel,
          maxParallelism,
          deployedMaxParallelism);
    }
    List<byte[]> snapshots;
    if (carriesProcessingTimeTimer()) {
      RawKeyedState.TimedRestore restored = RawKeyedState.restoreWithTimer(context);
      snapshots = restored.snapshots();
      restoredProcessingTimeTimerDeadline = restored.deadline();
    } else {
      snapshots = RawKeyedState.restore(context);
      restoredProcessingTimeTimerDeadline = Long.MIN_VALUE;
    }
    if (getKeyedStateBackend() instanceof CheckpointableKeyedStateBackend) {
      CanonicalNativeState.Restore canonical;
      if (getKeyedStateBackend() instanceof RocksDBNativeKeyedStateBackend) {
        RocksDBNativeKeyedStateBackend.CanonicalRestore restored =
            ((RocksDBNativeKeyedStateBackend<?>) getKeyedStateBackend())
                .restoreCanonicalState(stateLabel);
        canonical = new CanonicalNativeState.Restore(restored.partitions, restored.timerDeadline);
      } else {
        canonical =
            CanonicalNativeState.readAndClear(
                (CheckpointableKeyedStateBackend<?>) getKeyedStateBackend(), stateLabel);
      }
      if (!canonical.partitions.isEmpty()) {
        if (!snapshots.isEmpty()) {
          throw new IllegalStateException("restore contains both raw and canonical native state");
        }
        snapshots.addAll(canonical.partitions);
        restoredProcessingTimeTimerDeadline = canonical.timerDeadline;
      }
    }
    memoryBudget = NativeMemoryBudget.registerFor(this);
    memoryBudget.registerStateMetric(getMetricGroup());
    beforeHandleCreation();
    RocksDBNativeStateSupport rocksdb = resolveRocksDBState(!snapshots.isEmpty());
    rocksdbState = rocksdb != null;
    if (rocksdbState) {
      rocksdbSupport = rocksdb;
      directRocksDBState = usesDirectRocksDBState();
      if (directRocksDBState) {
        handle = createRocksDBHandle(rocksdb, snapshots.toArray(new byte[0][]));
        if (carriesProcessingTimeTimer()) {
          restoredProcessingTimeTimerDeadline = rocksdbTimerDeadline();
        }
      } else {
        rocksdbSnapshotStoreHandle =
            Native.createRocksDBSnapshotStore(
                rocksdb.tableDirectory(),
                maxParallelism(),
                rocksdb.optionsJson(),
                rocksdb.sharedResourcesHandle(),
                rocksdb.sourceDirectories(),
                rocksdb.sourceSnapshotTokens(),
                rocksdb.keyGroupStart(),
                rocksdb.keyGroupEnd(),
                rocksdb.aligned());
        byte[][] restored =
            Native.restoreRocksDBSnapshotStorePartitions(rocksdbSnapshotStoreHandle);
        if (carriesProcessingTimeTimer()) {
          restoredProcessingTimeTimerDeadline =
              Native.rocksdbSnapshotStoreTimerDeadline(rocksdbSnapshotStoreHandle);
        }
        if (restored.length == 0 && !snapshots.isEmpty()) {
          restored = snapshots.toArray(new byte[0][]);
        }
        handle = restored.length == 0 ? createHandle() : restoreRawHandle(restored);
      }
      rocksdb.register(
          new RocksDBNativeState() {
            @Override
            public String[] checkpoint(String snapshotDirectory) {
              return checkpointRocksDBHandle(snapshotDirectory);
            }

            @Override
            public byte[][] canonicalPartitions() {
              return snapshotCanonicalPartitions();
            }

            @Override
            public String canonicalOperatorId() {
              return stateLabel;
            }

            @Override
            public long canonicalTimerDeadline() {
              return carriesProcessingTimeTimer()
                  ? processingTimeTimerDeadlineForSnapshot()
                  : Long.MIN_VALUE;
            }
          });
      initializeStockState();
      return;
    }
    handle =
        snapshots.isEmpty() ? createHandle() : restoreRawHandle(snapshots.toArray(new byte[0][]));
    initializeStockState();
  }

  private void initializeStockState() throws Exception {
    configureStateHandle();
    if (stockStateContract != null)
      stockStateContract.configureNativeState(handle, directRocksDBState());
    if (canonicalProjection != null && canonicalProjection.isRestored()) {
      if (stockStateContract == null) {
        throw new IllegalStateException(
            "stock savepoint has no native state contract for " + stateLabel);
      }
      stockStateContract.importState(canonicalProjection.backend(), handle, directRocksDBState());
      publishStateBytes();
    }
    String storage = rocksdbState ? directRocksDBState ? "rocksdb" : "rocksdb-snapshot" : "memory";
    getMetricGroup().gauge("nativeStateStorage", () -> storage);
  }

  /** Binds runtime modes before restored logical state is transferred into the native handle. */
  protected void configureStateHandle() {}

  protected long canonicalWatermark() {
    return Long.MIN_VALUE;
  }

  protected void restoreCanonicalWatermark(long watermark) {}

  protected final boolean hasStockStateContract() {
    return stockStateContract != null;
  }

  @Override
  public void open() throws Exception {
    super.open();
    allocator = NativeAllocator.SHARED;
    dictionaries = NativeAllocator.DICTIONARIES;
  }

  @Override
  protected org.apache.flink.streaming.api.operators.OperatorSnapshotFutures snapshotCanonicalState(
      long checkpointId,
      long timestamp,
      org.apache.flink.runtime.checkpoint.CheckpointOptions options,
      org.apache.flink.runtime.state.CheckpointStreamFactory streamFactory)
      throws Exception {
    if (stockStateContract == null
        || !options.getCheckpointType().isSavepoint()
        || ((org.apache.flink.runtime.checkpoint.SavepointType) options.getCheckpointType())
                .getFormatType()
            != org.apache.flink.core.execution.SavepointFormatType.CANONICAL) {
      return null;
    }
    if (canonicalProjection == null) {
      throw new IllegalStateException("stock canonical snapshot backend was not installed");
    }
    beforeSnapshotState();
    stockStateContract.snapshotOperatorState(canonicalWatermark());
    var backend = canonicalProjection.backend();
    stockStateContract.exportState(backend, handle, directRocksDBState());
    var result =
        StockCanonicalSnapshot.capture(
            backend,
            getOperatorStateBackend(),
            checkpointId,
            timestamp,
            streamFactory,
            options,
            getContainingTask().getCancelables());
    try {
      stockStateContract.clear(backend);
      return result;
    } catch (Exception | Error failure) {
      try {
        result.cancel();
      } catch (Exception | Error cleanup) {
        failure.addSuppressed(cleanup);
      }
      throw failure;
    }
  }

  @Override
  public void snapshotState(StateSnapshotContext context) throws Exception {
    super.snapshotState(context);
    beforeSnapshotState();
    if (stockStateContract != null) stockStateContract.snapshotOperatorState(canonicalWatermark());
    // Native RocksDB state checkpoints through the keyed backend. Standalone memory state uses the
    // same managed canonical representation that RocksDB materializes for a canonical savepoint.
    if (rocksdbState) {
      return;
    }
    CheckpointableKeyedStateBackend<?> backend =
        (CheckpointableKeyedStateBackend<?>) getKeyedStateBackend();
    long timerDeadline =
        carriesProcessingTimeTimer() ? processingTimeTimerDeadlineForSnapshot() : Long.MIN_VALUE;
    if (!writeAsyncMemorySnapshot(backend, timerDeadline)) {
      CanonicalNativeState.write(backend, snapshotCanonicalPartitions(), stateLabel, timerDeadline);
    }
  }

  @Override
  public void close() throws Exception {
    if (handle != 0) {
      closeHandle();
      handle = 0;
    }
    if (rocksdbSnapshotStoreHandle != 0) {
      Native.closeRocksDBSnapshotStore(rocksdbSnapshotStoreHandle);
      rocksdbSnapshotStoreHandle = 0;
    }
    if (rocksdbSupport != null) {
      rocksdbSupport.close();
      rocksdbSupport = null;
    }
    if (memoryBudget != null) {
      memoryBudget.close();
      memoryBudget = null;
    }
    if (canonicalProjection != null) canonicalProjection.close();
    super.close();
  }

  // ---------------------------------------------------------------------------------- accessors

  protected final boolean rocksdbState() {
    return rocksdbState;
  }

  /** Whether this operator uses its typed RocksDB handle rather than the generic snapshot store. */
  protected final boolean directRocksDBState() {
    return directRocksDBState;
  }

  /** Encoded owner of this operator's reservation in the TaskManager-wide off-heap pool. */
  protected final long memoryBudgetBytes() {
    return memoryBudget == null ? NativeMemoryBudget.UNACCOUNTED : memoryBudget.nativeHandle();
  }

  /** The Flink max parallelism used to map native BinaryRow hashes to raw state key groups. */
  protected final int maxParallelism() {
    return maxParallelism;
  }

  /** Recursive logical timestamp descriptors for the grouping key's BinaryRow layout. */
  protected final int[] keyTimestampPrecisions() {
    return keyTimestampPrecisions;
  }

  /**
   * Samples the native state size and publishes it to the operator's gauges; call after batches and
   * flushes on the task thread (the handle is not thread-safe). No-op without a budget — the native
   * side only tracks its footprint when accounted.
   */
  protected final void publishStateBytes() {
    if (memoryBudget != null && handle != 0) {
      memoryBudget.publishStateBytes(stateBytesHandle());
    }
  }

  // ------------------------------------------------------------------------------ schema exports

  /**
   * Exports a row type as an FFI Arrow schema for the duration of one native call; the native side
   * consumes the schema contents, the wrapper struct is released here.
   */
  protected static long withRowSchema(RowType rowType, LongUnaryOperator call) {
    return withRowSchema(rowType, false, call);
  }

  protected static long withMapListSchema(RowType rowType, LongUnaryOperator call) {
    return withRowSchema(rowType, true, call);
  }

  private static long withRowSchema(RowType rowType, boolean mapList, LongUnaryOperator call) {
    try (ArrowSchema schema = ArrowSchema.allocateNew(NativeAllocator.SHARED)) {
      Data.exportSchema(
          NativeAllocator.SHARED,
          rowSchema(rowType, mapList),
          NativeAllocator.DICTIONARIES,
          schema);
      return call.applyAsLong(schema.memoryAddress());
    }
  }

  /** Two-input variant of {@link #withRowSchema}, for the joins' left and right row types. */
  protected static long withRowSchemas(
      RowType leftType, RowType rightType, LongBinaryOperator call) {
    return withRowSchemas(leftType, rightType, false, call);
  }

  protected static long withMapListSchemas(
      RowType leftType, RowType rightType, LongBinaryOperator call) {
    return withRowSchemas(leftType, rightType, true, call);
  }

  private static long withRowSchemas(
      RowType leftType, RowType rightType, boolean mapList, LongBinaryOperator call) {
    try (ArrowSchema left = ArrowSchema.allocateNew(NativeAllocator.SHARED);
        ArrowSchema right = ArrowSchema.allocateNew(NativeAllocator.SHARED)) {
      Data.exportSchema(
          NativeAllocator.SHARED, rowSchema(leftType, mapList), NativeAllocator.DICTIONARIES, left);
      Data.exportSchema(
          NativeAllocator.SHARED,
          rowSchema(rightType, mapList),
          NativeAllocator.DICTIONARIES,
          right);
      return call.applyAsLong(left.memoryAddress(), right.memoryAddress());
    }
  }

  private static org.apache.arrow.vector.types.pojo.Schema rowSchema(
      RowType rowType, boolean mapList) {
    var schema = ArrowConversion.toArrowSchema(rowType);
    return mapList ? MapListRepresentation.forIpc(schema) : schema;
  }
}
