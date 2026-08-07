package tech.streamfusion.operator;

import tech.streamfusion.Native;
import tech.streamfusion.arrow.ArrowConversion;
import tech.streamfusion.state.RocksDBNativeStateSupport;
import tech.streamfusion.planner.NativeConfig;
import java.util.List;
import java.util.function.BooleanSupplier;
import java.util.function.LongBinaryOperator;
import java.util.function.LongUnaryOperator;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.CDataDictionaryProvider;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.table.types.logical.RowType;

/**
 * Lifecycle shared by every native operator whose hot state lives in Rust: the handle, its
 * task off-heap reservation, and the two ways that state can be checkpointed.
 *
 * <p>State travels one of two routes, decided once at {@link #initializeState}. On the native
 * RocksDB backend it checkpoints incrementally through the keyed state backend. Otherwise state
 * stays a Rust hot map and travels as Flink <em>raw keyed state</em>, one payload per non-empty key
 * group, which lets Flink's own protocol redistribute it on rescale.
 *
 * <p>Subclasses supply only the native calls that differ — create, restore, snapshot, close, and
 * the state-size probe — plus optional direct-RocksDB and processing-time-timer hooks. Everything else
 * (budget reservation, restore/snapshot plumbing, handle release) is identical across operators and
 * lives here.
 */
public abstract class AbstractNativeStatefulOperator<OUT> extends AbstractStreamOperator<OUT> {

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

  protected long createRocksDBHandle(RocksDBNativeStateSupport rocksdb) {
    throw new UnsupportedOperationException("operator resolved RocksDB state without a create");
  }

  protected String[] checkpointRocksDBHandle(String snapshotDirectory) {
    byte[][] partitions = snapshotRawPartitions();
    long deadline =
        carriesProcessingTimeTimer()
            ? processingTimeTimerDeadlineForSnapshot()
            : Long.MIN_VALUE;
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
    super.initializeState(context);
    List<byte[]> snapshots;
    if (carriesProcessingTimeTimer()) {
      RawKeyedState.TimedRestore restored = RawKeyedState.restoreWithTimer(context);
      snapshots = restored.snapshots();
      restoredProcessingTimeTimerDeadline = restored.deadline();
    } else {
      snapshots = RawKeyedState.restore(context);
      restoredProcessingTimeTimerDeadline = Long.MIN_VALUE;
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
        handle = createRocksDBHandle(rocksdb);
      } else {
        rocksdbSnapshotStoreHandle =
            Native.createRocksDBSnapshotStore(
                rocksdb.tableDirectory(),
                maxParallelism(),
                rocksdb.optionsJson(),
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
        handle = restored.length == 0 ? createHandle() : restoreRawHandle(restored);
      }
      rocksdb.register(this::checkpointRocksDBHandle);
      return;
    }
    handle =
        snapshots.isEmpty() ? createHandle() : restoreRawHandle(snapshots.toArray(new byte[0][]));
  }

  @Override
  public void open() throws Exception {
    super.open();
    allocator = NativeAllocator.SHARED;
    dictionaries = NativeAllocator.DICTIONARIES;
  }

  @Override
  public void snapshotState(StateSnapshotContext context) throws Exception {
    super.snapshotState(context);
    beforeSnapshotState();
    // Native RocksDB state checkpoints through the keyed backend. Only standalone memory state
    // travels as raw keyed-state blobs.
    if (rocksdbState) {
      return;
    }
    if (carriesProcessingTimeTimer()) {
      RawKeyedState.snapshotPartitionsWithTimer(
          context, snapshotRawPartitions(), processingTimeTimerDeadlineForSnapshot());
    } else {
      RawKeyedState.snapshotPartitions(context, snapshotRawPartitions());
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
    if (memoryBudget != null) {
      memoryBudget.close();
      memoryBudget = null;
    }
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
    if (memoryBudget != null) {
      long stateBytes = stateBytesHandle();
      memoryBudget.publishStateBytes(stateBytes);
      long configuredFlush = NativeConfig.rocksDBWriteBufferBytes();
      long sharedPressureFlush =
          Math.max(1L << 20, TaskOffHeapMemory.capacityBytes() / 16);
      long flushThreshold = Math.min(configuredFlush, sharedPressureFlush);
      if (rocksdbState
          && rocksdbSupport != null
          && stateBytes > 0
          && (stateBytes >= flushThreshold
              || TaskOffHeapMemory.availableBytes() < flushThreshold)) {
        rocksdbSupport.flushForMemoryPressure();
      }
    }
  }

  // ------------------------------------------------------------------------------ schema exports

  /**
   * Exports a row type as an FFI Arrow schema for the duration of one native call; the native side
   * consumes the schema contents, the wrapper struct is released here.
   */
  protected static long withRowSchema(RowType rowType, LongUnaryOperator call) {
    try (ArrowSchema schema = ArrowSchema.allocateNew(NativeAllocator.SHARED)) {
      Data.exportSchema(
          NativeAllocator.SHARED,
          ArrowConversion.toArrowSchema(rowType),
          NativeAllocator.DICTIONARIES,
          schema);
      return call.applyAsLong(schema.memoryAddress());
    }
  }

  /** Two-input variant of {@link #withRowSchema}, for the joins' left and right row types. */
  protected static long withRowSchemas(RowType leftType, RowType rightType, LongBinaryOperator call) {
    try (ArrowSchema left = ArrowSchema.allocateNew(NativeAllocator.SHARED);
        ArrowSchema right = ArrowSchema.allocateNew(NativeAllocator.SHARED)) {
      Data.exportSchema(
          NativeAllocator.SHARED,
          ArrowConversion.toArrowSchema(leftType),
          NativeAllocator.DICTIONARIES,
          left);
      Data.exportSchema(
          NativeAllocator.SHARED,
          ArrowConversion.toArrowSchema(rightType),
          NativeAllocator.DICTIONARIES,
          right);
      return call.applyAsLong(left.memoryAddress(), right.memoryAddress());
    }
  }
}
