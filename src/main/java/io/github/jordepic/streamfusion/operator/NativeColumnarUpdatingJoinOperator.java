package io.github.jordepic.streamfusion.operator;

import io.github.jordepic.streamfusion.Native;
import io.github.jordepic.streamfusion.arrow.ArrowConversion;
import io.github.jordepic.streamfusion.operator.MiniBatchMetrics.FlushReason;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.CDataDictionaryProvider;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.types.logical.RowType;

/**
 * Regular (non-windowed) updating join, fed Arrow batches on both inputs and emitting Arrow batches.
 * Supports INNER, LEFT/RIGHT/FULL outer, and SEMI/ANTI: the native joiner keeps a per-side keyed
 * multiset and, for the outer/semi/anti families, a per-row match-degree to emit and retract
 * null-padded (outer) or bare (semi/anti) rows as a row's degree crosses 0↔1 — a faithful port of
 * Flink's {@code StreamingJoinOperator}/{@code StreamingSemiAntiJoinOperator}. The changelog flows
 * Arrow with no per-operator transpose; the row↔Arrow conversion happens only at the host edges. Each
 * input batch is folded into its side and the join changelog it produces is emitted immediately
 * (carrying the changelog kind on the output batch's {@code $row_kind$} column).
 *
 * <p>The left/right input schemas are handed to the joiner at construction (exported through the C
 * Data Interface) so an outer join can type the null-padding for a side before its first batch
 * arrives.
 */
public class NativeColumnarUpdatingJoinOperator extends AbstractStreamOperator<ArrowBatch>
    implements TwoInputStreamOperator<ArrowBatch, ArrowBatch, ArrowBatch> {

  private final int[] leftKeys;
  private final int[] rightKeys;
  private final int joinType;
  private final RowType leftType;
  private final RowType rightType;
  // Residual non-equi predicate, encoded over the joined [left.., right..] row (empty = none); the
  // same encoding the filter engine consumes.
  private final int[] predKinds;
  private final int[] predPayload;
  private final int[] predChildCounts;
  private final long[] predLongs;
  private final double[] predDoubles;
  private final String[] predStrings;
  private final NativeUdf.Binding predBinding;
  private final int[] keyTimestampPrecisions;
  private final boolean miniBatch;
  private final long miniBatchSize;
  private final int maxParallelism;

  private transient BufferAllocator allocator;
  private transient CDataDictionaryProvider dictionaries;
  private transient long handle;
  private transient MiniBatchBoundary boundary;
  private transient MiniBatchMetrics miniBatchMetrics;
  private transient ManagedMemoryBudget memoryBudget;

  public NativeColumnarUpdatingJoinOperator(
      int[] leftKeys,
      int[] rightKeys,
      int joinType,
      RowType leftType,
      RowType rightType,
      int[] predKinds,
      int[] predPayload,
      int[] predChildCounts,
      long[] predLongs,
      double[] predDoubles,
      String[] predStrings,
      NativeUdf.Binding predBinding,
      int[] keyTimestampPrecisions,
      boolean miniBatch,
      long miniBatchSize,
      int maxParallelism) {
    this.leftKeys = leftKeys;
    this.rightKeys = rightKeys;
    this.joinType = joinType;
    this.leftType = leftType;
    this.rightType = rightType;
    this.predKinds = predKinds;
    this.predPayload = predPayload;
    this.predChildCounts = predChildCounts;
    this.predLongs = predLongs;
    this.predDoubles = predDoubles;
    this.predStrings = predStrings;
    this.predBinding = predBinding;
    this.keyTimestampPrecisions = keyTimestampPrecisions;
    this.miniBatch = miniBatch;
    this.miniBatchSize = miniBatchSize;
    if (maxParallelism <= 0) {
      throw new IllegalArgumentException("native updating join state requires a positive max parallelism");
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
    java.util.List<byte[]> rawSnapshots = RawKeyedState.restore(context);
    // The joiner needs both sides' Arrow schemas up front (to type outer null-padding); export them
    // through the C Data Interface for the create/restore call to import.
    BufferAllocator alloc = NativeAllocator.SHARED;
    CDataDictionaryProvider dicts = NativeAllocator.DICTIONARIES;
    try (ArrowSchema leftSchema = ArrowSchema.allocateNew(alloc);
        ArrowSchema rightSchema = ArrowSchema.allocateNew(alloc)) {
      Data.exportSchema(alloc, ArrowConversion.toArrowSchema(leftType), dicts, leftSchema);
      Data.exportSchema(alloc, ArrowConversion.toArrowSchema(rightType), dicts, rightSchema);
      long[] boundPredLongs = predBinding.bind(predLongs);
      memoryBudget = ManagedMemoryBudget.reserveFor(this);
      if (!rawSnapshots.isEmpty()) {
        handle =
            Native.restoreUpdatingJoinerPartitions(
                leftKeys,
                rightKeys,
                joinType,
                leftSchema.memoryAddress(),
                rightSchema.memoryAddress(),
                predKinds,
                predPayload,
                predChildCounts,
                boundPredLongs,
                predDoubles,
                predStrings,
                miniBatch,
                rawSnapshots.toArray(new byte[0][]),
                memoryBudget.bytes());
      } else {
        handle =
            Native.createUpdatingJoiner(
                  leftKeys,
                  rightKeys,
                  joinType,
                  leftSchema.memoryAddress(),
                  rightSchema.memoryAddress(),
                  predKinds,
                  predPayload,
                  predChildCounts,
                  boundPredLongs,
                  predDoubles,
                  predStrings,
                  miniBatch,
                  memoryBudget.bytes());
      }
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
  public void processElement1(StreamRecord<ArrowBatch> element) {
    process(element.getValue(), true);
    publishStateBytes();
  }

  @Override
  public void processElement2(StreamRecord<ArrowBatch> element) {
    process(element.getValue(), false);
    publishStateBytes();
  }

  private void process(ArrowBatch batch, boolean left) {
    VectorSchemaRoot in = batch.root();
    if (!miniBatch) {
      join(in, left);
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
          joinOpen(in, left);
        } else {
          try (VectorSchemaRoot slice = in.slice(offset, length)) {
            joinOpen(slice, left);
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
  }

  /** Samples the native state size for the operator's gauges; task-thread only. */
  private void publishStateBytes() {
    if (memoryBudget.bounded()) {
      memoryBudget.publishStateBytes(Native.updatingJoinerStateBytes(handle));
    }
  }

  private void join(VectorSchemaRoot in, boolean left) {
    try {
      joinOpen(in, left);
    } finally {
      in.close();
    }
  }

  private void joinOpen(VectorSchemaRoot in, boolean left) {
    BufferAllocator inAllocator =
        in.getFieldVectors().isEmpty() ? allocator : in.getFieldVectors().get(0).getAllocator();
    try (ArrowArray inArray = ArrowArray.allocateNew(inAllocator);
        ArrowSchema inSchema = ArrowSchema.allocateNew(inAllocator);
        ArrowArray outArray = ArrowArray.allocateNew(allocator);
        ArrowSchema outSchema = ArrowSchema.allocateNew(allocator)) {
      Data.exportVectorSchemaRoot(inAllocator, in, dictionaries, inArray, inSchema);
      if (left) {
        Native.pushLeftUpdatingJoiner(
            handle, inArray.memoryAddress(), inSchema.memoryAddress(),
            outArray.memoryAddress(), outSchema.memoryAddress());
      } else {
        Native.pushRightUpdatingJoiner(
            handle, inArray.memoryAddress(), inSchema.memoryAddress(),
            outArray.memoryAddress(), outSchema.memoryAddress());
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
    long transientBytes = Native.updatingJoinerStagingBytes(handle);
    long touchedKeys = Native.updatingJoinerStagedKeys(handle);
    try (ArrowArray outArray = ArrowArray.allocateNew(allocator);
        ArrowSchema outSchema = ArrowSchema.allocateNew(allocator)) {
      Native.flushUpdatingJoiner(handle, outArray.memoryAddress(), outSchema.memoryAddress());
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

  @Override
  public void snapshotState(StateSnapshotContext context) throws Exception {
    super.snapshotState(context);
    RawKeyedState.snapshotPartitions(
        context,
        Native.snapshotUpdatingJoinerPartitions(
            handle, maxParallelism, keyTimestampPrecisions));
  }

  @Override
  public void close() throws Exception {
    if (handle != 0) {
      Native.closeUpdatingJoiner(handle);
      handle = 0;
    }
    if (memoryBudget != null) {
      memoryBudget.close();
      memoryBudget = null;
    }
    predBinding.unbind();
    super.close();
  }
}
