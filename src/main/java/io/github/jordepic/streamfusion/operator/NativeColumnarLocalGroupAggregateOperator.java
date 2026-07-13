package io.github.jordepic.streamfusion.operator;

import io.github.jordepic.streamfusion.Native;
import io.github.jordepic.streamfusion.operator.MiniBatchMetrics.FlushReason;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.CDataDictionaryProvider;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

/**
 * Local half of a two-phase non-windowed {@code GROUP BY}, columnar in and out — the Arrow analog of
 * Flink's {@code MapBundleOperator} wrapping {@code MiniBatchLocalGroupAggFunction}. It buffers a
 * mini-batch of rows into per-key accumulators (held in the native handle) and flushes one partial
 * row per key downstream to the native global merge.
 *
 * <p>Flush is driven exactly like Flink's bundle: the mini-batch marker the {@link
 * NativeColumnarMiniBatchAssignerOperator} emits arrives as a {@link Watermark} ({@link
 * #processWatermark}), a size trigger caps the buffer at {@code miniBatchSize} rows, and the buffer
 * is always drained before a checkpoint ({@link #prepareSnapshotPreBarrier}) and at end of input
 * ({@link #finish}). Because it is drained ahead of every barrier the buffer is transient — there is
 * no checkpointed state here; the durable state lives in the global half.
 */
public class NativeColumnarLocalGroupAggregateOperator extends AbstractStreamOperator<ArrowBatch>
    implements OneInputStreamOperator<ArrowBatch, ArrowBatch> {

  private final int[] aggregateKinds;
  private final int[] valueTypes;
  private final int[] valueColumns;
  private final int[] filterColumns;
  private final int[] keyColumns;
  private final int[] distinctViewSources;
  private final long miniBatchSize;

  private transient BufferAllocator allocator;
  private transient CDataDictionaryProvider dictionaries;
  private transient long handle;
  private transient MiniBatchBoundary boundary;
  private transient MiniBatchMetrics miniBatchMetrics;
  private transient ManagedMemoryBudget memoryBudget;

  public NativeColumnarLocalGroupAggregateOperator(
      int[] aggregateKinds,
      int[] valueTypes,
      int[] valueColumns,
      int[] filterColumns,
      int[] keyColumns,
      int[] distinctViewSources,
      long miniBatchSize) {
    this.aggregateKinds = aggregateKinds;
    this.valueTypes = valueTypes;
    this.valueColumns = valueColumns;
    this.filterColumns = filterColumns;
    this.keyColumns = keyColumns;
    this.distinctViewSources = distinctViewSources;
    this.miniBatchSize = miniBatchSize;
  }

  @Override
  public void open() throws Exception {
    super.open();
    allocator = NativeAllocator.SHARED;
    dictionaries = NativeAllocator.DICTIONARIES;
    memoryBudget = ManagedMemoryBudget.reserveFor(this);
    handle =
        Native.createLocalGroupAggregator(
            aggregateKinds,
            valueTypes,
            valueColumns,
            filterColumns,
            keyColumns,
            distinctViewSources,
            memoryBudget.bytes());
    boundary = new MiniBatchBoundary(miniBatchSize);
    miniBatchMetrics = new MiniBatchMetrics(getMetricGroup());
  }

  @Override
  public void processElement(StreamRecord<ArrowBatch> element) {
    VectorSchemaRoot in = element.getValue().root();
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
        ArrowSchema inSchema = ArrowSchema.allocateNew(inAllocator)) {
      Data.exportVectorSchemaRoot(inAllocator, in, dictionaries, inArray, inSchema);
      Native.updateLocalGroupAggregator(handle, inArray.memoryAddress(), inSchema.memoryAddress());
    }
  }

  @Override
  public void processWatermark(Watermark mark) throws Exception {
    flushBundle(FlushReason.WATERMARK);
    publishStateBytes();
    super.processWatermark(mark);
  }

  /** Samples the native state size for the operator's gauges; task-thread only. */
  private void publishStateBytes() {
    if (memoryBudget.bounded()) {
      memoryBudget.publishStateBytes(Native.localGroupAggregatorStateBytes(handle));
    }
  }

  @Override
  public void prepareSnapshotPreBarrier(long checkpointId) throws Exception {
    flushBundle(FlushReason.CHECKPOINT);
    super.prepareSnapshotPreBarrier(checkpointId);
  }

  @Override
  public void finish() throws Exception {
    flushBundle(FlushReason.FINISH);
    super.finish();
  }

  private void flushBundle(FlushReason reason) {
    long transientBytes = Native.localGroupAggregatorStateBytes(handle);
    try (ArrowArray outArray = ArrowArray.allocateNew(allocator);
        ArrowSchema outSchema = ArrowSchema.allocateNew(allocator)) {
      Native.flushLocalGroupAggregator(handle, outArray.memoryAddress(), outSchema.memoryAddress());
      VectorSchemaRoot partial =
          Data.importVectorSchemaRoot(allocator, outArray, outSchema, dictionaries);
      int outputRows = partial.getRowCount();
      miniBatchMetrics.onFlush(reason, outputRows, outputRows, transientBytes);
      if (partial.getRowCount() > 0) {
        output.collect(new StreamRecord<>(new ArrowBatch(partial)));
      } else {
        partial.close();
      }
    }
    boundary.reset();
  }

  @Override
  public void close() throws Exception {
    if (handle != 0) {
      Native.closeLocalGroupAggregator(handle);
      handle = 0;
    }
    if (memoryBudget != null) {
      memoryBudget.close();
      memoryBudget = null;
    }
    super.close();
  }
}
