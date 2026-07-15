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
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.types.logical.RowType;

/**
 * Columnar event-time INNER interval join (Arrow in on both inputs, Arrow out): the join of
 * {@code a JOIN b ON a.k = b.k AND a.rt BETWEEN b.rt + lower AND b.rt + upper}. Each input batch is
 * handed to the native joiner, which buffers it per equi-join key and immediately returns the rows
 * it matches against the other side already buffered — so a pair is emitted once, when the second of
 * its two rows arrives. The buffering, the probe, and the watermark-driven eviction all live
 * natively; this layer moves batches across the bridge and owns the handle's checkpointed state.
 *
 * <p>Flink delivers each input's watermark to {@link #processWatermark1}/{@link #processWatermark2};
 * the base operator combines them into the minimum and calls {@link #processWatermark}, which we
 * override to advance the joiner's eviction frontier before forwarding the watermark downstream.
 *
 * <p>A **proctime** interval join times each row by the operator's processing-time clock (Flink's
 * {@code ProcTimeIntervalJoin} uses the clock, not a row value): the row's time column is stamped with
 * the clock when it is pushed, the interval is measured in processing time, and eviction advances on
 * the clock. A buffered row arriving at {@code now} can no longer match once the clock passes {@code
 * now + max(upper, -lower)} (a future arrival's time only grows), so each batch registers a cleanup
 * timer there; the last one drains the tail (for an outer join, emitting the null-pads). Watermarks
 * are ignored in that mode and the remaining buffer is drained on finish.
 */
public class NativeIntervalJoinOperator extends AbstractStreamOperator<ArrowBatch>
    implements TwoInputStreamOperator<ArrowBatch, ArrowBatch, ArrowBatch>, ProcessingTimeCallback {

  private final int[] leftKeys;
  private final int[] rightKeys;
  private final int leftTime;
  private final int rightTime;
  private final long lowerMillis;
  private final long upperMillis;
  private final int joinType;
  private final RowType leftType;
  private final RowType rightType;
  private final EncodedPredicate predicate;
  private final boolean proctime;
  private final int[] keyTimestampPrecisions;
  private final int maxParallelism;

  private transient BufferAllocator allocator;
  private transient CDataDictionaryProvider dictionaries;
  private transient long handle;
  private transient ManagedMemoryBudget memoryBudget;
  private transient long restoredProcessingTimeTimerDeadline;
  private transient long registeredTimer;

  public NativeIntervalJoinOperator(
      int[] leftKeys,
      int[] rightKeys,
      int leftTime,
      int rightTime,
      long lowerMillis,
      long upperMillis,
      int joinType,
      RowType leftType,
      RowType rightType,
      EncodedPredicate predicate,
      boolean proctime,
      int[] keyTimestampPrecisions,
      int maxParallelism) {
    this.leftKeys = leftKeys;
    this.rightKeys = rightKeys;
    this.leftTime = leftTime;
    this.rightTime = rightTime;
    this.lowerMillis = lowerMillis;
    this.upperMillis = upperMillis;
    this.joinType = joinType;
    this.leftType = leftType;
    this.rightType = rightType;
    this.predicate = predicate;
    this.proctime = proctime;
    this.keyTimestampPrecisions = keyTimestampPrecisions;
    if (maxParallelism <= 0) {
      throw new IllegalArgumentException("native interval join state requires a positive max parallelism");
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
    RawKeyedState.TimedRestore restored = RawKeyedState.restoreWithTimer(context);
    java.util.List<byte[]> rawSnapshots = restored.snapshots();
    restoredProcessingTimeTimerDeadline = restored.deadline();
    // Hand both sides' Arrow schemas to the joiner up front so an outer join can type the null-padding
    // for a side before that side's first batch arrives.
    BufferAllocator alloc = NativeAllocator.SHARED;
    CDataDictionaryProvider dicts = NativeAllocator.DICTIONARIES;
    try (ArrowSchema leftSchema = ArrowSchema.allocateNew(alloc);
        ArrowSchema rightSchema = ArrowSchema.allocateNew(alloc)) {
      Data.exportSchema(alloc, ArrowConversion.toArrowSchema(leftType), dicts, leftSchema);
      Data.exportSchema(alloc, ArrowConversion.toArrowSchema(rightType), dicts, rightSchema);
      predicate.bind();
      memoryBudget = ManagedMemoryBudget.reserveFor(this);
      if (!rawSnapshots.isEmpty()) {
        handle =
            Native.restoreIntervalJoinerPartitions(
                leftKeys,
                rightKeys,
                leftTime,
                rightTime,
                lowerMillis,
                upperMillis,
                joinType,
                leftSchema.memoryAddress(),
                rightSchema.memoryAddress(),
                predicate.kinds,
                predicate.payload,
                predicate.childCounts,
                predicate.boundLongs(),
                predicate.doubles,
                predicate.strings,
                rawSnapshots.toArray(new byte[0][]),
                memoryBudget.bytes());
      } else {
        handle =
            Native.createIntervalJoiner(
                  leftKeys,
                  rightKeys,
                  leftTime,
                  rightTime,
                  lowerMillis,
                  upperMillis,
                  joinType,
                  leftSchema.memoryAddress(),
                  rightSchema.memoryAddress(),
                  predicate.kinds,
                  predicate.payload,
                  predicate.childCounts,
                  predicate.boundLongs(),
                  predicate.doubles,
                  predicate.strings,
                  memoryBudget.bytes());
      }
    }
  }

  @Override
  public void open() throws Exception {
    super.open();
    allocator = NativeAllocator.SHARED;
    dictionaries = NativeAllocator.DICTIONARIES;
    registeredTimer = Long.MIN_VALUE;
    if (proctime && restoredProcessingTimeTimerDeadline != Long.MIN_VALUE) {
      long deadline = restoredProcessingTimeTimerDeadline;
      long now = getProcessingTimeService().getCurrentProcessingTime();
      if (deadline <= now) {
        advance(now);
      } else {
        getProcessingTimeService().registerTimer(deadline, this);
        registeredTimer = deadline;
      }
    }
  }

  @Override
  public void processElement1(StreamRecord<ArrowBatch> element) {
    join(element.getValue(), true);
    publishStateBytes();
  }

  @Override
  public void processElement2(StreamRecord<ArrowBatch> element) {
    join(element.getValue(), false);
    publishStateBytes();
  }

  /** Samples the native state size for the operator's gauges; task-thread only. */
  private void publishStateBytes() {
    if (memoryBudget.bounded()) {
      memoryBudget.publishStateBytes(Native.intervalJoinerStateBytes(handle));
    }
  }

  /** Pushes a batch to its side of the joiner and emits the matched pairs it returns (if any). */
  private void join(ArrowBatch batch, boolean left) {
    long now = proctime ? getProcessingTimeService().getCurrentProcessingTime() : 0;
    VectorSchemaRoot in = batch.root();
    BufferAllocator inAllocator =
        in.getFieldVectors().isEmpty() ? allocator : in.getFieldVectors().get(0).getAllocator();
    try (ArrowArray inArray = ArrowArray.allocateNew(inAllocator);
        ArrowSchema inSchema = ArrowSchema.allocateNew(inAllocator);
        ArrowArray outArray = ArrowArray.allocateNew(allocator);
        ArrowSchema outSchema = ArrowSchema.allocateNew(allocator)) {
      Data.exportVectorSchemaRoot(inAllocator, in, dictionaries, inArray, inSchema);
      if (left) {
        Native.pushLeftIntervalJoiner(
            handle,
            inArray.memoryAddress(),
            inSchema.memoryAddress(),
            outArray.memoryAddress(),
            outSchema.memoryAddress(),
            proctime,
            now);
      } else {
        Native.pushRightIntervalJoiner(
            handle,
            inArray.memoryAddress(),
            inSchema.memoryAddress(),
            outArray.memoryAddress(),
            outSchema.memoryAddress(),
            proctime,
            now);
      }
      VectorSchemaRoot out = Data.importVectorSchemaRoot(allocator, outArray, outSchema, dictionaries);
      if (out.getRowCount() > 0) {
        output.collect(new StreamRecord<>(new ArrowBatch(out)));
      } else {
        out.close(); // no matches for this batch
      }
    } finally {
      in.close();
    }
    if (proctime) {
      advance(now); // trim the buffers / emit outer null-pads the clock has passed
      // A row buffered at `now` is dead once the clock passes now + max(upper, -lower); schedule a
      // cleanup there so even with no further input the tail (and outer null-pads) drains. now only
      // advances, so the latest boundary scheduled covers every row buffered as of now.
      long horizon = Math.max(Math.max(upperMillis, -lowerMillis), 0);
      long boundary = now + Math.max(horizon, 1); // strictly future, so the timer actually fires
      if (boundary > registeredTimer) {
        getProcessingTimeService().registerTimer(boundary, this);
        registeredTimer = boundary;
      }
    }
  }

  @Override
  public void onProcessingTime(long time) {
    advance(getProcessingTimeService().getCurrentProcessingTime());
    publishStateBytes();
  }

  @Override
  public void finish() throws Exception {
    if (proctime) {
      advance(Long.MAX_VALUE); // end of input: evict everything (drains outer null-pads)
    }
    super.finish();
  }

  @Override
  public void processWatermark(Watermark mark) throws Exception {
    // Proctime joins evict on the processing-time clock, not the watermark; just forward it.
    if (!proctime) {
      advance(mark.getTimestamp());
      publishStateBytes();
    }
    super.processWatermark(mark);
  }

  /** Advances the eviction frontier, emitting any null-padded rows for evicted unmatched outer rows. */
  private void advance(long threshold) {
    try (ArrowArray outArray = ArrowArray.allocateNew(allocator);
        ArrowSchema outSchema = ArrowSchema.allocateNew(allocator)) {
      Native.advanceIntervalJoiner(
          handle, threshold, outArray.memoryAddress(), outSchema.memoryAddress());
      VectorSchemaRoot out = Data.importVectorSchemaRoot(allocator, outArray, outSchema, dictionaries);
      if (out.getRowCount() > 0) {
        output.collect(new StreamRecord<>(new ArrowBatch(out)));
      } else {
        out.close();
      }
    }
  }

  @Override
  public void snapshotState(StateSnapshotContext context) throws Exception {
    super.snapshotState(context);
    RawKeyedState.snapshotPartitionsWithTimer(
        context,
        Native.snapshotIntervalJoinerPartitions(
            handle, maxParallelism, keyTimestampPrecisions),
        proctime ? registeredTimer : Long.MIN_VALUE);
  }

  @Override
  public void close() throws Exception {
    if (handle != 0) {
      Native.closeIntervalJoiner(handle);
      handle = 0;
    }
    if (memoryBudget != null) {
      memoryBudget.close();
      memoryBudget = null;
    }
    predicate.unbind();
    super.close();
  }
}
