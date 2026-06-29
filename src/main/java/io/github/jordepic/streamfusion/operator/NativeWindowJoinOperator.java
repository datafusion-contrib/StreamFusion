package io.github.jordepic.streamfusion.operator;

import io.github.jordepic.streamfusion.Native;
import io.github.jordepic.streamfusion.arrow.ArrowConversion;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.CDataDictionaryProvider;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.api.common.operators.ProcessingTimeService.ProcessingTimeCallback;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.types.logical.RowType;

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
 *
 * <p>A **proctime** window join instead closes windows on the processing-time clock: the upstream
 * proctime TVF assigns each row to the window(s) covering the clock, and this operator fires a
 * processing-time timer at each window end (chaining to the next slide boundary while windows remain
 * open, like the proctime window aggregate). It ignores watermarks in that mode and drains on finish.
 */
public class NativeWindowJoinOperator extends AbstractStreamOperator<ArrowBatch>
    implements TwoInputStreamOperator<ArrowBatch, ArrowBatch, ArrowBatch>, ProcessingTimeCallback {

  private final int[] leftKeys;
  private final int[] rightKeys;
  private final int leftWindowStart;
  private final int leftWindowEnd;
  private final int rightWindowStart;
  private final int rightWindowEnd;
  private final int joinType;
  private final RowType leftType;
  private final RowType rightType;
  private final EncodedPredicate predicate;
  private final boolean proctime;
  private final long windowMillis;
  private final long slideMillis;
  private final boolean cumulative;

  private transient BufferAllocator allocator;
  private transient CDataDictionaryProvider dictionaries;
  private transient long handle;
  private transient ListState<byte[]> handleState;
  private transient long registeredTimer;
  private transient long maxOpenEnd;

  public NativeWindowJoinOperator(
      int[] leftKeys,
      int[] rightKeys,
      int leftWindowStart,
      int leftWindowEnd,
      int rightWindowStart,
      int rightWindowEnd,
      int joinType,
      RowType leftType,
      RowType rightType,
      EncodedPredicate predicate,
      boolean proctime,
      long windowMillis,
      long slideMillis,
      boolean cumulative) {
    this.leftKeys = leftKeys;
    this.rightKeys = rightKeys;
    this.leftWindowStart = leftWindowStart;
    this.leftWindowEnd = leftWindowEnd;
    this.rightWindowStart = rightWindowStart;
    this.rightWindowEnd = rightWindowEnd;
    this.joinType = joinType;
    this.leftType = leftType;
    this.rightType = rightType;
    this.predicate = predicate;
    this.proctime = proctime;
    this.windowMillis = windowMillis;
    this.slideMillis = slideMillis;
    this.cumulative = cumulative;
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
    BufferAllocator alloc = NativeAllocator.SHARED;
    CDataDictionaryProvider dicts = NativeAllocator.DICTIONARIES;
    try (ArrowSchema leftSchema = ArrowSchema.allocateNew(alloc);
        ArrowSchema rightSchema = ArrowSchema.allocateNew(alloc)) {
      Data.exportSchema(alloc, ArrowConversion.toArrowSchema(leftType), dicts, leftSchema);
      Data.exportSchema(alloc, ArrowConversion.toArrowSchema(rightType), dicts, rightSchema);
      handle =
          snapshot == null
              ? Native.createWindowJoiner(
                  leftKeys,
                  rightKeys,
                  leftWindowStart,
                  leftWindowEnd,
                  rightWindowStart,
                  rightWindowEnd,
                  joinType,
                  leftSchema.memoryAddress(),
                  rightSchema.memoryAddress(),
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
                  joinType,
                  leftSchema.memoryAddress(),
                  rightSchema.memoryAddress(),
                  predicate.kinds,
                  predicate.payload,
                  predicate.childCounts,
                  predicate.longs,
                  predicate.doubles,
                  predicate.strings,
                  snapshot);
    }
  }

  @Override
  public void open() throws Exception {
    super.open();
    allocator = NativeAllocator.SHARED;
    dictionaries = NativeAllocator.DICTIONARIES;
    registeredTimer = Long.MIN_VALUE;
    maxOpenEnd = Long.MIN_VALUE;
  }

  @Override
  public void processElement1(StreamRecord<ArrowBatch> element) {
    buffer(element.getValue(), true);
    onProctimeInput();
  }

  @Override
  public void processElement2(StreamRecord<ArrowBatch> element) {
    buffer(element.getValue(), false);
    onProctimeInput();
  }

  /**
   * After buffering a proctime batch, close any window the clock has passed and (re)schedule the
   * timer at the next window-end boundary while windows remain open — the same chained-timer model as
   * the proctime window aggregate, here driving the two-input join's flush.
   */
  private void onProctimeInput() {
    if (!proctime) {
      return;
    }
    long now = getProcessingTimeService().getCurrentProcessingTime();
    flush(now);
    maxOpenEnd = Math.max(maxOpenEnd, latestWindowEnd(now));
    scheduleNextTimer(now);
  }

  @Override
  public void onProcessingTime(long time) {
    long now = getProcessingTimeService().getCurrentProcessingTime();
    flush(now);
    scheduleNextTimer(now);
  }

  private void scheduleNextTimer(long now) {
    long boundary = Math.floorDiv(now, slideMillis) * slideMillis + slideMillis;
    if (boundary <= maxOpenEnd && boundary > registeredTimer) {
      getProcessingTimeService().registerTimer(boundary, this);
      registeredTimer = boundary;
    }
  }

  private long latestWindowEnd(long now) {
    return cumulative
        ? Math.floorDiv(now, windowMillis) * windowMillis + windowMillis
        : Math.floorDiv(now, slideMillis) * slideMillis + windowMillis;
  }

  @Override
  public void finish() throws Exception {
    if (proctime) {
      flush(Long.MAX_VALUE); // end of input: close every remaining window
    }
    super.finish();
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
    // Proctime joins close on the processing-time clock, not the watermark; just forward it.
    if (!proctime) {
      flush(mark.getTimestamp());
    }
    super.processWatermark(mark);
  }

  /** Emits and evicts every window whose end the given threshold has passed. */
  private void flush(long threshold) {
    try (ArrowArray array = ArrowArray.allocateNew(allocator);
        ArrowSchema schema = ArrowSchema.allocateNew(allocator)) {
      Native.flushWindowJoiner(handle, threshold, array.memoryAddress(), schema.memoryAddress());
      VectorSchemaRoot out = Data.importVectorSchemaRoot(allocator, array, schema, dictionaries);
      if (out.getRowCount() > 0) {
        output.collect(new StreamRecord<>(new ArrowBatch(out)));
      } else {
        out.close(); // no windows closed (or no matches) at this threshold
      }
    }
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
