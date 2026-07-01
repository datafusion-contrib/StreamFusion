package io.github.jordepic.streamfusion.operator;

import io.github.jordepic.streamfusion.Native;
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
  private final int[] keyColumns;
  private final long miniBatchSize;

  private transient BufferAllocator allocator;
  private transient CDataDictionaryProvider dictionaries;
  private transient long handle;
  private transient long bufferedRows;
  private transient ManagedMemoryBudget memoryBudget;

  public NativeColumnarLocalGroupAggregateOperator(
      int[] aggregateKinds,
      int[] valueTypes,
      int[] valueColumns,
      int[] keyColumns,
      long miniBatchSize) {
    this.aggregateKinds = aggregateKinds;
    this.valueTypes = valueTypes;
    this.valueColumns = valueColumns;
    this.keyColumns = keyColumns;
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
            aggregateKinds, valueTypes, valueColumns, keyColumns, memoryBudget.bytes());
    bufferedRows = 0;
  }

  @Override
  public void processElement(StreamRecord<ArrowBatch> element) {
    VectorSchemaRoot in = element.getValue().root();
    long rows = in.getRowCount();
    BufferAllocator inAllocator =
        in.getFieldVectors().isEmpty() ? allocator : in.getFieldVectors().get(0).getAllocator();
    try (ArrowArray inArray = ArrowArray.allocateNew(inAllocator);
        ArrowSchema inSchema = ArrowSchema.allocateNew(inAllocator)) {
      Data.exportVectorSchemaRoot(inAllocator, in, dictionaries, inArray, inSchema);
      Native.updateLocalGroupAggregator(handle, inArray.memoryAddress(), inSchema.memoryAddress());
    } finally {
      in.close();
    }
    bufferedRows += rows;
    // Size trigger: cap the buffer like Flink's CountBundleTrigger (mini-batch.size); a non-positive
    // size disables it, leaving the marker / checkpoint / end-of-input flushes.
    if (miniBatchSize > 0 && bufferedRows >= miniBatchSize) {
      flushBundle();
    }
  }

  @Override
  public void processWatermark(Watermark mark) throws Exception {
    flushBundle(); // the mini-batch marker — flush the bundle, then propagate the watermark
    super.processWatermark(mark);
  }

  @Override
  public void prepareSnapshotPreBarrier(long checkpointId) throws Exception {
    flushBundle(); // drain before the barrier so the buffer never needs checkpointing
    super.prepareSnapshotPreBarrier(checkpointId);
  }

  @Override
  public void finish() throws Exception {
    flushBundle();
    super.finish();
  }

  private void flushBundle() {
    try (ArrowArray outArray = ArrowArray.allocateNew(allocator);
        ArrowSchema outSchema = ArrowSchema.allocateNew(allocator)) {
      Native.flushLocalGroupAggregator(handle, outArray.memoryAddress(), outSchema.memoryAddress());
      VectorSchemaRoot partial =
          Data.importVectorSchemaRoot(allocator, outArray, outSchema, dictionaries);
      if (partial.getRowCount() > 0) {
        output.collect(new StreamRecord<>(new ArrowBatch(partial)));
      } else {
        partial.close();
      }
    }
    bufferedRows = 0;
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
