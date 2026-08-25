package tech.streamfusion.operator;

import tech.streamfusion.Native;
import tech.streamfusion.planner.NativeConfig;
import tech.streamfusion.state.RocksDBNativeStateSupport;
import java.util.function.LongBinaryOperator;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.api.common.operators.ProcessingTimeService.ProcessingTimeCallback;
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
public class NativeIntervalJoinOperator extends AbstractNativeStatefulOperator<ArrowBatch>
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
    super("interval join", keyTimestampPrecisions, maxParallelism);
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
  }

  // Rows are timed by the clock in proctime mode and evicted on a processing-time timer, so the
  // eviction deadline must survive recovery inside every raw key group.
  @Override
  protected boolean carriesProcessingTimeTimer() {
    return true;
  }

  @Override
  protected long processingTimeTimerDeadlineForSnapshot() {
    return proctime ? registeredTimer : Long.MIN_VALUE;
  }

  @Override
  protected void beforeHandleCreation() {
    predicate.bind(new org.apache.flink.table.functions.FunctionContext(getRuntimeContext()));
  }

  // A proctime interval join re-arms its cleanup timer from the snapshot-store deadline after
  // recovery, so it stays on the generic snapshot store; the event-time join is purely
  // watermark-driven.
  @Override
  protected boolean usesDirectRocksDBState() {
    return !proctime
        && withSchemas((l, r) -> Native.rocksdbIntervalJoinerSupported(l, r) ? 1L : 0L) != 0L;
  }

  @Override
  protected long createRocksDBHandle(RocksDBNativeStateSupport rocksdb) {
    return withSchemas(
        (l, r) ->
            Native.createRocksDBIntervalJoiner(
                leftKeys, rightKeys, keyTimestampPrecisions(),
                leftTime, rightTime, lowerMillis, upperMillis, joinType, l, r,
                predicate.kinds, predicate.payload, predicate.childCounts,
                predicate.boundLongs(), predicate.doubles, predicate.strings,
                memoryBudgetBytes(),
                rocksdb.tableDirectory(), maxParallelism(), rocksdb.optionsJson(),
                rocksdb.sharedResourcesHandle(), rocksdb.sourceDirectories(),
                rocksdb.sourceSnapshotTokens(), rocksdb.keyGroupStart(), rocksdb.keyGroupEnd(),
                rocksdb.aligned()));
  }

  @Override
  protected String[] checkpointRocksDBHandle(String snapshotDirectory) {
    return directRocksDBState()
        ? Native.checkpointRocksDBIntervalJoiner(handle, snapshotDirectory)
        : super.checkpointRocksDBHandle(snapshotDirectory);
  }

  @Override
  protected byte[][] snapshotCanonicalPartitions() {
    return directRocksDBState()
        ? Native.snapshotRocksDBIntervalJoinerPartitions(handle)
        : snapshotRawPartitions();
  }

  @Override
  protected long createHandle() {
    return withSchemas(
        (l, r) ->
            Native.createIntervalJoiner(
                leftKeys, rightKeys, leftTime, rightTime, lowerMillis, upperMillis, joinType, l, r,
                predicate.kinds, predicate.payload, predicate.childCounts, predicate.boundLongs(),
                predicate.doubles, predicate.strings, memoryBudgetBytes()));
  }

  @Override
  protected long restoreRawHandle(byte[][] snapshots) {
    return withSchemas(
        (l, r) ->
            Native.restoreIntervalJoinerPartitions(
                leftKeys, rightKeys, leftTime, rightTime, lowerMillis, upperMillis, joinType, l, r,
                predicate.kinds, predicate.payload, predicate.childCounts, predicate.boundLongs(),
                predicate.doubles, predicate.strings, snapshots, memoryBudgetBytes()));
  }

  @Override
  protected byte[][] snapshotRawPartitions() {
    return Native.snapshotIntervalJoinerPartitions(
        handle, maxParallelism(), keyTimestampPrecisions());
  }

  @Override
  protected void closeHandle() {
    if (directRocksDBState()) {
      Native.closeRocksDBIntervalJoiner(handle);
    } else {
      Native.closeIntervalJoiner(handle);
    }
  }

  @Override
  protected long stateBytesHandle() {
    return directRocksDBState()
        ? Native.rocksdbIntervalJoinerStateBytes(handle)
        : Native.intervalJoinerStateBytes(handle);
  }

  /** Exports both side row types as FFI Arrow schemas for the duration of one native call. */
  private long withSchemas(LongBinaryOperator call) {
    return withRowSchemas(leftType, rightType, call);
  }

  @Override
  public void open() throws Exception {
    super.open();
    registeredTimer = Long.MIN_VALUE;
    if (proctime && restoredProcessingTimeTimerDeadline() != Long.MIN_VALUE) {
      long deadline = restoredProcessingTimeTimerDeadline();
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
    ColumnarRecordMetrics.countIngested(getMetricGroup(), element.getValue().rowCount());
    join(element.getValue(), true);
    publishStateBytes();
  }

  @Override
  public void processElement2(StreamRecord<ArrowBatch> element) {
    ColumnarRecordMetrics.countIngested(getMetricGroup(), element.getValue().rowCount());
    join(element.getValue(), false);
    publishStateBytes();
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
      if (directRocksDBState()) {
        if (left) {
          Native.pushLeftRocksDBIntervalJoiner(
              handle,
              inArray.memoryAddress(),
              inSchema.memoryAddress(),
              outArray.memoryAddress(),
              outSchema.memoryAddress(),
              proctime,
              now);
        } else {
          Native.pushRightRocksDBIntervalJoiner(
              handle,
              inArray.memoryAddress(),
              inSchema.memoryAddress(),
              outArray.memoryAddress(),
              outSchema.memoryAddress(),
              proctime,
              now);
        }
      } else if (left) {
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
        ColumnarRecordMetrics.emit(output, getMetricGroup(), new ArrowBatch(out));
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
      if (directRocksDBState()) {
        Native.advanceRocksDBIntervalJoiner(
            handle, threshold, outArray.memoryAddress(), outSchema.memoryAddress());
      } else {
        Native.advanceIntervalJoiner(
            handle, threshold, outArray.memoryAddress(), outSchema.memoryAddress());
      }
      VectorSchemaRoot out = Data.importVectorSchemaRoot(allocator, outArray, outSchema, dictionaries);
      if (out.getRowCount() > 0) {
        ColumnarRecordMetrics.emit(output, getMetricGroup(), new ArrowBatch(out));
      } else {
        out.close();
      }
    }
  }

  @Override
  public void close() throws Exception {
    super.close();
    predicate.unbind();
  }
}
