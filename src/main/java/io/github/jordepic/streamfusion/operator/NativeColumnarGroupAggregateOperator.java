package io.github.jordepic.streamfusion.operator;

import io.github.jordepic.streamfusion.Native;
import io.github.jordepic.streamfusion.operator.MiniBatchMetrics.FlushReason;
import io.github.jordepic.streamfusion.planner.NativeConfig;
import io.github.jordepic.streamfusion.state.PaimonKeyedStateBackend;
import io.github.jordepic.streamfusion.state.PaimonRestoredSource;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.CDataDictionaryProvider;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

/**
 * Non-windowed {@code GROUP BY} aggregation, fed Arrow batches and emitting Arrow batches (the
 * native kernel reads/writes the row kind on the batch's {@code $row_kind$} column). A native
 * changelog chain pays no per-operator transpose; the row↔Arrow conversion happens only at the host
 * edges (inserted by the transition pass), and each keyed shuffle stays columnar where the input is a
 * columnar producer.
 */
public class NativeColumnarGroupAggregateOperator extends AbstractStreamOperator<ArrowBatch>
    implements OneInputStreamOperator<ArrowBatch, ArrowBatch> {

  private final int[] aggregateKinds;
  private final int[] valueTypes;
  private final int[] valueColumns;
  private final int[] keyColumns;
  private final int[] filterColumns;
  private final int[] countColumns;
  private final int[] distinctViewColumns;
  private final int recordCountColumn;
  private final boolean generateUpdateBefore;
  private final boolean miniBatch;
  private final long miniBatchSize;
  private final int[] keyTimestampPrecisions;
  private final int maxParallelism;

  private transient BufferAllocator allocator;
  private transient CDataDictionaryProvider dictionaries;
  private transient long handle;
  private transient boolean paimonState;
  private transient MiniBatchBoundary boundary;
  private transient MiniBatchMetrics miniBatchMetrics;
  private transient ManagedMemoryBudget memoryBudget;

  public NativeColumnarGroupAggregateOperator(
      int[] aggregateKinds,
      int[] valueTypes,
      int[] valueColumns,
      int[] keyColumns,
      int[] filterColumns,
      int[] countColumns,
      int[] distinctViewColumns,
      int recordCountColumn,
      boolean generateUpdateBefore,
      boolean miniBatch,
      long miniBatchSize,
      int[] keyTimestampPrecisions,
      int maxParallelism) {
    this.aggregateKinds = aggregateKinds;
    this.valueTypes = valueTypes;
    this.valueColumns = valueColumns;
    this.keyColumns = keyColumns;
    this.filterColumns = filterColumns;
    this.countColumns = countColumns;
    this.distinctViewColumns = distinctViewColumns;
    this.recordCountColumn = recordCountColumn;
    this.generateUpdateBefore = generateUpdateBefore;
    this.miniBatch = miniBatch;
    this.miniBatchSize = miniBatchSize;
    this.keyTimestampPrecisions = keyTimestampPrecisions;
    if (maxParallelism <= 0) {
      throw new IllegalArgumentException("native group aggregate state requires a positive max parallelism");
    }
    this.maxParallelism = maxParallelism;
  }

  @Override
  protected boolean isUsingCustomRawKeyedState() {
    return true;
  }

  @Override
  public void initializeState(StateInitializationContext context) throws Exception {
    super.initializeState(context);
    java.util.List<byte[]> snapshots = RawKeyedState.restore(context);
    memoryBudget = ManagedMemoryBudget.reserveFor(this);
    // The Paimon backend takes over only when the job selected it, this build carries it, the
    // aggregate list is persistable, and no raw keyed state arrived (a checkpoint written by the
    // memory backend restores on the memory backend — no silent migration).
    PaimonKeyedStateBackend<?> paimonBackend =
        getKeyedStateBackend() instanceof PaimonKeyedStateBackend
            ? (PaimonKeyedStateBackend<?>) getKeyedStateBackend()
            : null;
    paimonState =
        paimonBackend != null
            && snapshots.isEmpty()
            && Native.paimonStateAvailable()
            && Native.paimonGroupAggregatorSupported(aggregateKinds, valueTypes);
    if (paimonState) {
      java.util.List<PaimonRestoredSource> sources = paimonBackend.restoredSources();
      String[] sourceDirs = new String[sources.size()];
      long[] sourceSnapshots = new long[sources.size()];
      for (int i = 0; i < sources.size(); i++) {
        sourceDirs[i] = sources.get(i).directory();
        sourceSnapshots[i] = sources.get(i).snapshotId();
      }
      handle =
          Native.createPaimonGroupAggregator(
              aggregateKinds, valueTypes, valueColumns, keyColumns, keyTimestampPrecisions,
              filterColumns, countColumns, distinctViewColumns, recordCountColumn,
              generateUpdateBefore, miniBatch, memoryBudget.bytes(),
              paimonBackend.tableDirectory(), maxParallelism,
              NativeConfig.paimonFileFormat(), NativeConfig.paimonFileCompression(),
              sourceDirs, sourceSnapshots,
              paimonBackend.getKeyGroupRange().getStartKeyGroup(),
              paimonBackend.getKeyGroupRange().getEndKeyGroup());
      long nativeHandle = handle;
      paimonBackend.registerNativeState(
          linkDir -> Native.checkpointPaimonGroupAggregator(nativeHandle, linkDir));
      return;
    }
    if (paimonBackend != null) {
      LOG.info(
          "group aggregate falls back to memory state under the Paimon backend "
              + "(unsupported aggregate list, missing native feature, or raw-state restore)");
    }
    handle =
        snapshots.isEmpty()
            ? Native.createGroupAggregator(
                aggregateKinds, valueTypes, valueColumns, keyColumns, keyTimestampPrecisions,
                filterColumns, countColumns, distinctViewColumns, recordCountColumn,
                generateUpdateBefore, miniBatch, memoryBudget.bytes())
            : Native.restoreGroupAggregatorPartitions(
                aggregateKinds, valueTypes, valueColumns, keyColumns, keyTimestampPrecisions,
                filterColumns, countColumns, distinctViewColumns, recordCountColumn, generateUpdateBefore,
                miniBatch,
                snapshots.toArray(new byte[0][]),
                memoryBudget.bytes());
  }

  @Override
  public void open() throws Exception {
    super.open();
    allocator = NativeAllocator.SHARED;
    dictionaries = NativeAllocator.DICTIONARIES;
    if (miniBatch) {
      boundary = new MiniBatchBoundary(miniBatchSize);
      miniBatchMetrics = new MiniBatchMetrics(getMetricGroup());
    }
  }

  @Override
  public void processElement(StreamRecord<ArrowBatch> element) {
    VectorSchemaRoot in = element.getValue().root();
    if (!miniBatch) {
      try {
        update(in);
      } finally {
        in.close();
      }
      publishStateBytes();
      return;
    }

    int rows = in.getRowCount();
    miniBatchMetrics.onPhysicalBatch();
    try {
      if (rows == 0) {
        update(in);
      } else {
        int offset = 0;
        while (offset < rows) {
          boolean firstContribution = offset == 0 || boundary.bufferedRows() == 0;
          int length = boundary.nextSliceLength(rows - offset);
          if (length < rows - offset) {
            miniBatchMetrics.onPhysicalBatchSplit();
          }
          if (offset == 0 && length == rows) {
            update(in);
          } else {
            try (VectorSchemaRoot slice = in.slice(offset, length)) {
              update(slice);
            }
          }
          miniBatchMetrics.onSlice(length, firstContribution);
          offset += length;
          if (boundary.onSlice(length)) {
            flushBundle(FlushReason.COUNT);
          }
        }
      }
    } finally {
      in.close();
    }
    publishStateBytes();
  }

  private void update(VectorSchemaRoot in) {
    BufferAllocator inAllocator =
        in.getFieldVectors().isEmpty() ? allocator : in.getFieldVectors().get(0).getAllocator();
    try (ArrowArray inArray = ArrowArray.allocateNew(inAllocator);
        ArrowSchema inSchema = ArrowSchema.allocateNew(inAllocator);
        ArrowArray outArray = ArrowArray.allocateNew(allocator);
        ArrowSchema outSchema = ArrowSchema.allocateNew(allocator)) {
      Data.exportVectorSchemaRoot(inAllocator, in, dictionaries, inArray, inSchema);
      if (paimonState) {
        Native.updatePaimonGroupAggregator(
            handle,
            inArray.memoryAddress(),
            inSchema.memoryAddress(),
            outArray.memoryAddress(),
            outSchema.memoryAddress());
      } else {
        Native.updateGroupAggregator(
            handle,
            inArray.memoryAddress(),
            inSchema.memoryAddress(),
            outArray.memoryAddress(),
            outSchema.memoryAddress());
      }
      VectorSchemaRoot out =
          Data.importVectorSchemaRoot(allocator, outArray, outSchema, dictionaries);
      if (out.getRowCount() > 0) {
        output.collect(new StreamRecord<>(new ArrowBatch(out)));
      } else {
        out.close();
      }
    }
  }

  @Override
  public void processWatermark(Watermark mark) throws Exception {
    if (miniBatch) {
      flushBundle(FlushReason.WATERMARK);
      publishStateBytes();
    }
    super.processWatermark(mark);
  }

  @Override
  public void prepareSnapshotPreBarrier(long checkpointId) throws Exception {
    if (miniBatch) {
      flushBundle(FlushReason.CHECKPOINT);
    }
    super.prepareSnapshotPreBarrier(checkpointId);
  }

  @Override
  public void finish() throws Exception {
    if (miniBatch) {
      flushBundle(FlushReason.FINISH);
    }
    super.finish();
  }

  private void flushBundle(FlushReason reason) {
    long transientBytes =
        paimonState
            ? Native.paimonGroupAggregatorStagingBytes(handle)
            : Native.groupAggregatorStagingBytes(handle);
    long touchedKeys =
        paimonState
            ? Native.paimonGroupAggregatorStagedKeys(handle)
            : Native.groupAggregatorStagedKeys(handle);
    try (ArrowArray outArray = ArrowArray.allocateNew(allocator);
        ArrowSchema outSchema = ArrowSchema.allocateNew(allocator)) {
      if (paimonState) {
        Native.flushPaimonGroupAggregator(handle, outArray.memoryAddress(), outSchema.memoryAddress());
      } else {
        Native.flushGroupAggregator(handle, outArray.memoryAddress(), outSchema.memoryAddress());
      }
      VectorSchemaRoot out =
          Data.importVectorSchemaRoot(allocator, outArray, outSchema, dictionaries);
      int outputRows = out.getRowCount();
      miniBatchMetrics.onFlush(reason, outputRows, touchedKeys, transientBytes);
      if (outputRows > 0) {
        output.collect(new StreamRecord<>(new ArrowBatch(out)));
      } else {
        out.close();
      }
    }
    boundary.reset();
  }

  /** Samples the native state size for the operator's gauges; task-thread only. */
  private void publishStateBytes() {
    if (memoryBudget.bounded()) {
      memoryBudget.publishStateBytes(
          paimonState
              ? Native.paimonGroupAggregatorStateBytes(handle)
              : Native.groupAggregatorStateBytes(handle));
    }
  }

  @Override
  public void snapshotState(StateSnapshotContext context) throws Exception {
    super.snapshotState(context);
    // Paimon state checkpoints through the keyed state backend's snapshot (an incremental Paimon
    // commit); only memory state travels as raw keyed-state blobs.
    if (!paimonState) {
      RawKeyedState.snapshotPartitions(
          context,
          Native.snapshotGroupAggregatorPartitions(
              handle, maxParallelism, keyTimestampPrecisions));
    }
  }

  @Override
  public void close() throws Exception {
    if (handle != 0) {
      if (paimonState) {
        Native.closePaimonGroupAggregator(handle);
      } else {
        Native.closeGroupAggregator(handle);
      }
      handle = 0;
    }
    if (memoryBudget != null) {
      memoryBudget.close();
      memoryBudget = null;
    }
    super.close();
  }
}
