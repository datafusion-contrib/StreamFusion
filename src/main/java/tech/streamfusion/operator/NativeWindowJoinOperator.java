package tech.streamfusion.operator;

import tech.streamfusion.Native;
import tech.streamfusion.planner.NativeConfig;
import tech.streamfusion.state.PaimonNativeStateSupport;
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
public class NativeWindowJoinOperator extends AbstractNativeStatefulOperator<ArrowBatch>
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

  private transient long registeredTimer;
  private transient long maxOpenEnd;
  private transient FlinkWindowJoinMetrics flinkWindowMetrics;

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
      boolean cumulative,
      int[] keyTimestampPrecisions,
      int maxParallelism) {
    super("window join", keyTimestampPrecisions, maxParallelism);
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

  // A proctime window join closes on processing-time timers, so the deadline must travel in every
  // raw key group; an event-time one writes the frame with no deadline.
  @Override
  protected boolean carriesProcessingTimeTimer() {
    return true;
  }

  @Override
  protected long processingTimeTimerDeadlineForSnapshot() {
    return proctime ? maxOpenEnd : Long.MIN_VALUE;
  }

  @Override
  protected void beforeHandleCreation() {
    predicate.bind(new org.apache.flink.table.functions.FunctionContext(getRuntimeContext()));
  }

  @Override
  protected PaimonNativeStateSupport resolvePaimonState(boolean rawStateRestored) {
    // A proctime window join closes on processing-time timers whose deadline travels in raw
    // state, so only the event-time mode is Paimon-eligible.
    if (proctime) {
      return null;
    }
    return resolvePaimon(
        rawStateRestored,
        () ->
            withSchemas(
                    (l, r) ->
                        Native.paimonRowStateSupported(l) && Native.paimonRowStateSupported(r)
                            ? 1L
                            : 0L)
                != 0);
  }

  @Override
  protected long createPaimonHandle(PaimonNativeStateSupport paimon) {
    return withSchemas(
        (l, r) ->
            Native.createPaimonWindowJoiner(
                leftKeys,
                rightKeys,
                leftWindowStart,
                leftWindowEnd,
                rightWindowStart,
                rightWindowEnd,
                joinType,
                l,
                r,
                predicate.kinds,
                predicate.payload,
                predicate.childCounts,
                predicate.boundLongs(),
                predicate.doubles,
                predicate.strings,
                keyTimestampPrecisions(),
                memoryBudgetBytes(),
                paimon.tableDirectory(),
                maxParallelism(),
                NativeConfig.paimonBuckets(),
                NativeConfig.paimonFileFormat(),
                NativeConfig.paimonFileCompression(),
                paimon.sourceDirectories(),
                paimon.sourceSnapshotTokens(),
                paimon.keyGroupStart(),
                paimon.keyGroupEnd(),
                paimon.aligned()));
  }

  @Override
  protected String[] checkpointPaimonHandle() {
    return Native.checkpointPaimonWindowJoiner(handle);
  }

  @Override
  protected long createHandle() {
    return withSchemas(
        (l, r) ->
            Native.createWindowJoiner(
                leftKeys,
                rightKeys,
                leftWindowStart,
                leftWindowEnd,
                rightWindowStart,
                rightWindowEnd,
                joinType,
                l,
                r,
                predicate.kinds,
                predicate.payload,
                predicate.childCounts,
                predicate.boundLongs(),
                predicate.doubles,
                predicate.strings,
                memoryBudgetBytes()));
  }

  @Override
  protected long restoreRawHandle(byte[][] snapshots) {
    return withSchemas(
        (l, r) ->
            Native.restoreWindowJoinerPartitions(
                leftKeys,
                rightKeys,
                leftWindowStart,
                leftWindowEnd,
                rightWindowStart,
                rightWindowEnd,
                joinType,
                l,
                r,
                predicate.kinds,
                predicate.payload,
                predicate.childCounts,
                predicate.boundLongs(),
                predicate.doubles,
                predicate.strings,
                snapshots,
                memoryBudgetBytes()));
  }

  @Override
  protected byte[][] snapshotRawPartitions() {
    return Native.snapshotWindowJoinerPartitions(
        handle, maxParallelism(), keyTimestampPrecisions());
  }

  @Override
  protected void closeHandle() {
    if (paimonState()) {
      Native.closePaimonWindowJoiner(handle);
    } else {
      Native.closeWindowJoiner(handle);
    }
  }

  @Override
  protected long stateBytesHandle() {
    return paimonState()
        ? Native.paimonWindowJoinerStateBytes(handle)
        : Native.windowJoinerStateBytes(handle);
  }

  /** Exports both side row types as FFI Arrow schemas for the duration of one native call. */
  private long withSchemas(LongBinaryOperator call) {
    return withRowSchemas(leftType, rightType, call);
  }

  @Override
  public void open() throws Exception {
    super.open();
    flinkWindowMetrics =
        new FlinkWindowJoinMetrics(getMetricGroup(), getProcessingTimeService());
    registeredTimer = Long.MIN_VALUE;
    maxOpenEnd = restoredProcessingTimeTimerDeadline();
    if (proctime && maxOpenEnd != Long.MIN_VALUE) {
      long now = getProcessingTimeService().getCurrentProcessingTime();
      if (maxOpenEnd <= now) {
        flush(now);
      } else {
        scheduleNextTimer(now);
      }
    }
  }

  @Override
  public void processElement1(StreamRecord<ArrowBatch> element) {
    ColumnarRecordMetrics.countIngested(getMetricGroup(), element.getValue().rowCount());
    buffer(element.getValue(), true);
    onProctimeInput();
    publishStateBytes();
  }

  @Override
  public void processElement2(StreamRecord<ArrowBatch> element) {
    ColumnarRecordMetrics.countIngested(getMetricGroup(), element.getValue().rowCount());
    buffer(element.getValue(), false);
    onProctimeInput();
    publishStateBytes();
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
    publishStateBytes();
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
      if (paimonState()) {
        if (left) {
          Native.pushLeftPaimonWindowJoiner(handle, array.memoryAddress(), schema.memoryAddress());
        } else {
          Native.pushRightPaimonWindowJoiner(handle, array.memoryAddress(), schema.memoryAddress());
        }
      } else if (left) {
        Native.pushLeftWindowJoiner(handle, array.memoryAddress(), schema.memoryAddress());
      } else {
        Native.pushRightWindowJoiner(handle, array.memoryAddress(), schema.memoryAddress());
      }
    } finally {
      in.close();
    }
    flinkWindowMetrics.reportLateRecords(
        paimonState()
            ? Native.paimonWindowJoinerLateDrops(handle, true)
            : Native.windowJoinerLateDrops(handle, true),
        paimonState()
            ? Native.paimonWindowJoinerLateDrops(handle, false)
            : Native.windowJoinerLateDrops(handle, false));
  }

  @Override
  public void processWatermark(Watermark mark) throws Exception {
    if (!proctime) {
      flinkWindowMetrics.onWatermark(mark.getTimestamp());
    }
    // Proctime joins close on the processing-time clock, not the watermark; just forward it.
    if (!proctime) {
      flush(mark.getTimestamp());
      publishStateBytes();
    }
    super.processWatermark(mark);
  }

  /** Emits and evicts every window whose end the given threshold has passed. */
  private void flush(long threshold) {
    try (ArrowArray array = ArrowArray.allocateNew(allocator);
        ArrowSchema schema = ArrowSchema.allocateNew(allocator)) {
      if (paimonState()) {
        Native.flushPaimonWindowJoiner(
            handle, threshold, array.memoryAddress(), schema.memoryAddress());
      } else {
        Native.flushWindowJoiner(handle, threshold, array.memoryAddress(), schema.memoryAddress());
      }
      VectorSchemaRoot out = Data.importVectorSchemaRoot(allocator, array, schema, dictionaries);
      if (out.getRowCount() > 0) {
        ColumnarRecordMetrics.emit(output, getMetricGroup(), new ArrowBatch(out));
      } else {
        out.close(); // no windows closed (or no matches) at this threshold
      }
    }
  }

  @Override
  public void close() throws Exception {
    super.close();
    predicate.unbind();
  }
}
