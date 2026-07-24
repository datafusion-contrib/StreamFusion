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
 * Changelog normalization (Flink's {@code ChangelogNormalize}), fed Arrow batches and emitting Arrow
 * batches. Keeps the last full row per unique key and turns an upsert/duplicate-bearing changelog
 * into a regular INSERT/UPDATE_BEFORE/UPDATE_AFTER/DELETE changelog (the row kind read and written on
 * the batch's {@code $row_kind$} column). With mini-batch disabled it emits synchronously per input
 * batch. With mini-batch enabled it emits the first-preimage/final-postimage transition at the
 * logical count/watermark/checkpoint/end boundary. Columnar in and out, so it pays no per-operator
 * transpose; the keyed shuffle stays columnar where the input is a columnar producer.
 */
public class NativeColumnarChangelogNormalizeOperator extends AbstractStreamOperator<ArrowBatch>
    implements OneInputStreamOperator<ArrowBatch, ArrowBatch> {

  private final int[] keyColumns;
  private final int[] keyTimestampPrecisions;
  private final RowType rowType;
  private final boolean generateUpdateBefore;
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

  public NativeColumnarChangelogNormalizeOperator(
      int[] keyColumns,
      int[] keyTimestampPrecisions,
      RowType rowType,
      boolean generateUpdateBefore,
      boolean miniBatch,
      long miniBatchSize,
      int maxParallelism) {
    this.keyColumns = keyColumns;
    this.keyTimestampPrecisions = keyTimestampPrecisions;
    this.rowType = rowType;
    this.generateUpdateBefore = generateUpdateBefore;
    this.miniBatch = miniBatch;
    this.miniBatchSize = miniBatchSize;
    if (maxParallelism <= 0) {
      throw new IllegalArgumentException(
          "native changelog-normalization state requires a positive max parallelism");
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
            "changelog normalize",
            !snapshots.isEmpty(),
            () ->
                withRowSchema(address -> Native.paimonRowStateSupported(address) ? 1L : 0L) != 0);
    paimonState = paimon != null;
    if (paimonState) {
      handle =
          withRowSchema(
              rowSchemaAddress ->
                  Native.createPaimonChangelogNormalizer(
                      keyColumns,
                      keyTimestampPrecisions,
                      rowSchemaAddress,
                      generateUpdateBefore,
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
      paimon.register(
          linkDir -> Native.checkpointPaimonChangelogNormalizer(nativeHandle, linkDir));
      return;
    }
    handle =
        snapshots.isEmpty()
            ? Native.createChangelogNormalizer(
                keyColumns,
                keyTimestampPrecisions,
                generateUpdateBefore,
                miniBatch,
                memoryBudget.bytes())
            : Native.restoreChangelogNormalizerPartitions(
                keyColumns,
                keyTimestampPrecisions,
                generateUpdateBefore,
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
      if (rows == 0) {
        push(in);
      } else {
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
        Native.pushPaimonChangelogNormalizer(
            handle,
            inArray.memoryAddress(),
            inSchema.memoryAddress(),
            outArray.memoryAddress(),
            outSchema.memoryAddress());
      } else {
        Native.pushChangelogNormalizer(
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
            ? Native.paimonChangelogNormalizerStagingBytes(handle)
            : Native.changelogNormalizerStagingBytes(handle);
    long touchedKeys =
        paimonState
            ? Native.paimonChangelogNormalizerStagedKeys(handle)
            : Native.changelogNormalizerStagedKeys(handle);
    try (ArrowArray outArray = ArrowArray.allocateNew(allocator);
        ArrowSchema outSchema = ArrowSchema.allocateNew(allocator)) {
      if (paimonState) {
        Native.flushPaimonChangelogNormalizer(
            handle, outArray.memoryAddress(), outSchema.memoryAddress());
      } else {
        Native.flushChangelogNormalizer(
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
              ? Native.paimonChangelogNormalizerStateBytes(handle)
              : Native.changelogNormalizerStateBytes(handle));
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
          Native.snapshotChangelogNormalizerPartitions(
              handle, maxParallelism, keyTimestampPrecisions));
    }
  }

  @Override
  public void close() throws Exception {
    if (handle != 0) {
      if (paimonState) {
        Native.closePaimonChangelogNormalizer(handle);
      } else {
        Native.closeChangelogNormalizer(handle);
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
