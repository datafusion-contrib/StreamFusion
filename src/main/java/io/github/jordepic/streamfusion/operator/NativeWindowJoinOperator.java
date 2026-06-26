package io.github.jordepic.streamfusion.operator;

import io.github.jordepic.streamfusion.Native;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.CDataDictionaryProvider;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

/**
 * Columnar event-time INNER window join (Arrow in on both inputs, Arrow out): the join of two
 * windowing-TVF inputs on their equi-join key within the same window. Each input row carries the
 * {@code window_start}/{@code window_end} columns assigned upstream. Both inputs are buffered
 * natively; on a watermark the native joiner emits the INNER matches of every window the watermark
 * has closed and evicts them — a buffer-then-emit-on-watermark flow (unlike the interval join, which
 * emits as rows arrive). The buffering, the per-window hash join, and the eviction live natively;
 * this layer moves batches across the bridge and owns the handle's checkpointed state.
 *
 * <p>Flink delivers each input's watermark to {@link #processWatermark1}/{@link #processWatermark2};
 * the base operator combines them into the minimum and calls {@link #processWatermark}, which closes
 * the windows that minimum has passed before forwarding it downstream.
 */
public class NativeWindowJoinOperator extends AbstractStreamOperator<ArrowBatch>
    implements TwoInputStreamOperator<ArrowBatch, ArrowBatch, ArrowBatch> {

  private final int[] leftKeys;
  private final int[] rightKeys;
  private final int leftWindowStart;
  private final int leftWindowEnd;
  private final int rightWindowStart;
  private final int rightWindowEnd;
  private final EncodedPredicate predicate;

  private transient BufferAllocator allocator;
  private transient CDataDictionaryProvider dictionaries;
  private transient long handle;
  private transient ListState<byte[]> handleState;

  public NativeWindowJoinOperator(
      int[] leftKeys,
      int[] rightKeys,
      int leftWindowStart,
      int leftWindowEnd,
      int rightWindowStart,
      int rightWindowEnd,
      EncodedPredicate predicate) {
    this.leftKeys = leftKeys;
    this.rightKeys = rightKeys;
    this.leftWindowStart = leftWindowStart;
    this.leftWindowEnd = leftWindowEnd;
    this.rightWindowStart = rightWindowStart;
    this.rightWindowEnd = rightWindowEnd;
    this.predicate = predicate;
  }

  @Override
  public void initializeState(StateInitializationContext context) throws Exception {
    super.initializeState(context);
    handleState =
        context
            .getOperatorStateStore()
            .getListState(
                new ListStateDescriptor<>(
                    "streamfusion-window-join-handle",
                    PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO));
    byte[] snapshot = null;
    for (byte[] entry : handleState.get()) {
      snapshot = entry;
    }
    handle =
        snapshot == null
            ? Native.createWindowJoiner(
                leftKeys,
                rightKeys,
                leftWindowStart,
                leftWindowEnd,
                rightWindowStart,
                rightWindowEnd,
                predicate.kinds,
                predicate.payload,
                predicate.childCounts,
                predicate.longs,
                predicate.doubles,
                predicate.strings)
            : Native.restoreWindowJoiner(
                leftKeys,
                rightKeys,
                leftWindowStart,
                leftWindowEnd,
                rightWindowStart,
                rightWindowEnd,
                predicate.kinds,
                predicate.payload,
                predicate.childCounts,
                predicate.longs,
                predicate.doubles,
                predicate.strings,
                snapshot);
  }

  @Override
  public void open() throws Exception {
    super.open();
    allocator = NativeAllocator.SHARED;
    dictionaries = NativeAllocator.DICTIONARIES;
  }

  @Override
  public void processElement1(StreamRecord<ArrowBatch> element) {
    buffer(element.getValue(), true);
  }

  @Override
  public void processElement2(StreamRecord<ArrowBatch> element) {
    buffer(element.getValue(), false);
  }

  /** Hands a batch to its side of the joiner, which buffers it (no output until a watermark). */
  private void buffer(ArrowBatch batch, boolean left) {
    VectorSchemaRoot in = batch.root();
    BufferAllocator inAllocator =
        in.getFieldVectors().isEmpty() ? allocator : in.getFieldVectors().get(0).getAllocator();
    try (ArrowArray array = ArrowArray.allocateNew(inAllocator);
        ArrowSchema schema = ArrowSchema.allocateNew(inAllocator)) {
      Data.exportVectorSchemaRoot(inAllocator, in, dictionaries, array, schema);
      if (left) {
        Native.pushLeftWindowJoiner(handle, array.memoryAddress(), schema.memoryAddress());
      } else {
        Native.pushRightWindowJoiner(handle, array.memoryAddress(), schema.memoryAddress());
      }
    } finally {
      in.close();
    }
  }

  @Override
  public void processWatermark(Watermark mark) throws Exception {
    try (ArrowArray array = ArrowArray.allocateNew(allocator);
        ArrowSchema schema = ArrowSchema.allocateNew(allocator)) {
      Native.flushWindowJoiner(
          handle, mark.getTimestamp(), array.memoryAddress(), schema.memoryAddress());
      VectorSchemaRoot out = Data.importVectorSchemaRoot(allocator, array, schema, dictionaries);
      if (out.getRowCount() > 0) {
        output.collect(new StreamRecord<>(new ArrowBatch(out)));
      } else {
        out.close(); // no windows closed (or no matches) this watermark
      }
    }
    super.processWatermark(mark);
  }

  @Override
  public void snapshotState(StateSnapshotContext context) throws Exception {
    super.snapshotState(context);
    handleState.clear();
    handleState.add(Native.snapshotWindowJoiner(handle));
  }

  @Override
  public void close() throws Exception {
    if (handle != 0) {
      Native.closeWindowJoiner(handle);
      handle = 0;
    }
    super.close();
  }
}
