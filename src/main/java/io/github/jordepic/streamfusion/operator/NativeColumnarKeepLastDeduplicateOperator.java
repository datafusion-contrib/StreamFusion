package io.github.jordepic.streamfusion.operator;

import io.github.jordepic.streamfusion.Native;
import io.github.jordepic.streamfusion.arrow.ArrowConversion;
import io.github.jordepic.streamfusion.operator.MiniBatchMetrics.FlushReason;
import io.github.jordepic.streamfusion.planner.NativeConfig;
import io.github.jordepic.streamfusion.state.PaimonNativeStateSupport;
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
import org.apache.flink.table.types.logical.RowType;

/**
 * Columnar eager (push→emit) deduplication: Arrow in, Arrow out. Serves the three non-buffered dedup
 * variants — rowtime keep-last ({@code RowTimeDeduplicateFunction}), proctime keep-last ({@code
 * ProcTimeDeduplicateKeepLastRowFunction}), and proctime keep-first ({@code
 * ProcTimeDeduplicateKeepFirstRowFunction}). Keep-last keeps the winning row per key and emits a
 * retract changelog eagerly on each input batch ({@code +I} for a key's first row, {@code
 * -U}(previous)/{@code +U}(new) on replacement — the kind rides the {@code $row_kind$} column);
 * keep-first emits each key's first row ({@code +I}, insert-only) and drops the rest. A rowtime order
 * keeps the max-rowtime row; proctime uses arrival order. Insert-only input. Keys are co-located by
 * the columnar shuffle; the per-key stored row and the checkpointed handle state live here. (Rowtime
 * keep-first is watermark-buffered — see {@link NativeColumnarDeduplicateOperator}.)
 */
public class NativeColumnarKeepLastDeduplicateOperator extends AbstractStreamOperator<ArrowBatch>
    implements OneInputStreamOperator<ArrowBatch, ArrowBatch> {

  private final int[] partitionColumns;
  private final int[] keyTimestampPrecisions;
  private final int rowtimeColumn;
  private final RowType rowType;
  private final boolean generateUpdateBefore;
  private final boolean rowtimeOrdered;
  private final boolean keepFirst;
  private final boolean miniBatch;
  private final long miniBatchSize;
  private final int maxParallelism;

  private transient BufferAllocator allocator;
  private transient CDataDictionaryProvider dictionaries;
  private transient long handle;
  private transient boolean paimonState;
  private transient MiniBatchBoundary boundary;
  private transient MiniBatchMetrics miniBatchMetrics;
  private transient ManagedMemoryBudget memoryBudget;

  public NativeColumnarKeepLastDeduplicateOperator(
      int[] partitionColumns,
      int[] keyTimestampPrecisions,
      int rowtimeColumn,
      RowType rowType,
      boolean generateUpdateBefore,
      boolean rowtimeOrdered,
      boolean keepFirst,
      boolean miniBatch,
      long miniBatchSize,
      int maxParallelism) {
    this.partitionColumns = partitionColumns;
    this.keyTimestampPrecisions = keyTimestampPrecisions;
    this.rowtimeColumn = rowtimeColumn;
    this.rowType = rowType;
    this.generateUpdateBefore = generateUpdateBefore;
    this.rowtimeOrdered = rowtimeOrdered;
    this.keepFirst = keepFirst;
    this.miniBatch = miniBatch && !keepFirst;
    this.miniBatchSize = miniBatchSize;
    if (maxParallelism <= 0) {
      throw new IllegalArgumentException("native keep-last state requires a positive max parallelism");
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
    PaimonNativeStateSupport paimon =
        PaimonNativeStateSupport.resolve(
            getKeyedStateBackend(),
            "keep-last deduplicate",
            !snapshots.isEmpty(),
            () ->
                withRowSchema(address -> Native.paimonRowStateSupported(address) ? 1L : 0L) != 0);
    paimonState = paimon != null;
    if (paimonState) {
      handle =
          withRowSchema(
              rowSchemaAddress ->
                  Native.createPaimonKeepLastDeduplicator(
                      partitionColumns,
                      keyTimestampPrecisions,
                      rowtimeColumn,
                      rowSchemaAddress,
                      generateUpdateBefore,
                      rowtimeOrdered,
                      keepFirst,
                      miniBatch,
                      memoryBudget.bytes(),
                      paimon.tableDirectory(),
                      maxParallelism,
                      NativeConfig.paimonFileFormat(),
                      NativeConfig.paimonFileCompression(),
                      paimon.sourceDirectories(),
                      paimon.sourceSnapshotIds(),
                      paimon.keyGroupStart(),
                      paimon.keyGroupEnd()));
      long nativeHandle = handle;
      paimon.register(linkDir -> Native.checkpointPaimonKeepLastDeduplicator(nativeHandle, linkDir));
      return;
    }
    handle =
        snapshots.isEmpty()
            ? Native.createKeepLastDeduplicator(
                partitionColumns,
                keyTimestampPrecisions,
                rowtimeColumn,
                generateUpdateBefore,
                rowtimeOrdered,
                keepFirst,
                miniBatch,
                memoryBudget.bytes())
            : Native.restoreKeepLastDeduplicatorPartitions(
                partitionColumns,
                keyTimestampPrecisions,
                rowtimeColumn,
                generateUpdateBefore,
                rowtimeOrdered,
                keepFirst,
                miniBatch,
                snapshots.toArray(new byte[0][]),
                memoryBudget.bytes());
  }

  /**
   * Exports the input row type as an FFI Arrow schema for the duration of one native call; the
   * native side consumes the schema contents, the wrapper struct is released here.
   */
  private long withRowSchema(java.util.function.LongUnaryOperator call) {
    try (ArrowSchema rowSchema = ArrowSchema.allocateNew(NativeAllocator.SHARED)) {
      Data.exportSchema(
          NativeAllocator.SHARED,
          ArrowConversion.toArrowSchema(rowType),
          NativeAllocator.DICTIONARIES,
          rowSchema);
      return call.applyAsLong(rowSchema.memoryAddress());
    }
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
        push(in);
      } finally {
        in.close();
      }
      publishStateBytes();
      return;
    }
    int rows = in.getRowCount();
    miniBatchMetrics.onPhysicalBatch();
    try {
      int offset = 0;
      while (offset < rows) {
        boolean firstContribution = offset == 0 || boundary.bufferedRows() == 0;
        int length = boundary.nextSliceLength(rows - offset);
        if (length < rows - offset) {
          miniBatchMetrics.onPhysicalBatchSplit();
        }
        if (offset == 0 && length == rows) {
          push(in);
        } else {
          try (VectorSchemaRoot slice = in.slice(offset, length)) {
            push(slice);
          }
        }
        miniBatchMetrics.onSlice(length, firstContribution);
        offset += length;
        if (boundary.onSlice(length)) {
          flushBundle(FlushReason.COUNT);
        }
      }
    } finally {
      in.close();
    }
    publishStateBytes();
  }

  private void push(VectorSchemaRoot in) {
    BufferAllocator inAllocator =
        in.getFieldVectors().isEmpty() ? allocator : in.getFieldVectors().get(0).getAllocator();
    try (ArrowArray inArray = ArrowArray.allocateNew(inAllocator);
        ArrowSchema inSchema = ArrowSchema.allocateNew(inAllocator);
        ArrowArray outArray = ArrowArray.allocateNew(allocator);
        ArrowSchema outSchema = ArrowSchema.allocateNew(allocator)) {
      Data.exportVectorSchemaRoot(inAllocator, in, dictionaries, inArray, inSchema);
      if (paimonState) {
        Native.pushPaimonKeepLastDeduplicator(
            handle,
            inArray.memoryAddress(),
            inSchema.memoryAddress(),
            outArray.memoryAddress(),
            outSchema.memoryAddress());
      } else {
        Native.pushKeepLastDeduplicator(
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
            ? Native.paimonKeepLastDeduplicatorStagingBytes(handle)
            : Native.keepLastDeduplicatorStagingBytes(handle);
    long touchedKeys =
        paimonState
            ? Native.paimonKeepLastDeduplicatorStagedKeys(handle)
            : Native.keepLastDeduplicatorStagedKeys(handle);
    try (ArrowArray outArray = ArrowArray.allocateNew(allocator);
        ArrowSchema outSchema = ArrowSchema.allocateNew(allocator)) {
      if (paimonState) {
        Native.flushPaimonKeepLastDeduplicator(
            handle, outArray.memoryAddress(), outSchema.memoryAddress());
      } else {
        Native.flushKeepLastDeduplicator(
            handle, outArray.memoryAddress(), outSchema.memoryAddress());
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
              ? Native.paimonKeepLastDeduplicatorStateBytes(handle)
              : Native.keepLastDeduplicatorStateBytes(handle));
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
          Native.snapshotKeepLastDeduplicatorPartitions(
              handle, maxParallelism, keyTimestampPrecisions));
    }
  }

  @Override
  public void close() throws Exception {
    if (handle != 0) {
      if (paimonState) {
        Native.closePaimonKeepLastDeduplicator(handle);
      } else {
        Native.closeKeepLastDeduplicator(handle);
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
