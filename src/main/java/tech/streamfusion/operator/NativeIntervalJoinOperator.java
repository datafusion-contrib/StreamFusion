package tech.streamfusion.operator;

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
import tech.streamfusion.Native;
import tech.streamfusion.state.RocksDBNativeStateSupport;

/**
 * Columnar event-time INNER interval join (Arrow in on both inputs, Arrow out): the join of {@code
 * a JOIN b ON a.k = b.k AND a.rt BETWEEN b.rt + lower AND b.rt + upper}. Each input batch is handed
 * to the native joiner, which buffers it per equi-join key and immediately returns the rows it
 * matches against the other side already buffered — so a pair is emitted once, when the second of
 * its two rows arrives. The buffering, the probe, and the watermark-driven eviction all live
 * natively; this layer moves batches across the bridge and owns the handle's checkpointed state.
 *
 * <p>Flink delivers each input's watermark to {@link #processWatermark1}/{@link
 * #processWatermark2}; the base operator combines them into the minimum and calls {@link
 * #processWatermark}, which we override to advance the joiner's eviction frontier before forwarding
 * the watermark downstream with Flink's interval-bound output delay.
 *
 * <p>A **proctime** interval join times each row by the operator's processing-time clock (Flink's
 * {@code ProcTimeIntervalJoin} uses the clock, not a row value): the row's time column is stamped
 * with the clock when it is pushed, the interval is measured in processing time, and eviction
 * advances on the clock. A buffered row arriving at {@code now} can no longer match once the clock
 * passes {@code now + max(upper, -lower)} (a future arrival's time only grows), so each batch
 * registers a cleanup timer there; the last one drains the tail (for an outer join, emitting the
 * null-pads). Watermarks do not advance that clock. Canonical-bound operators preserve the original
 * SQL payload alongside a private cache clock and wait for real callbacks even at end of input; the
 * legacy unbound path stamps the row time field and drains its remaining buffer on finish.
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
  private boolean preserveProcessingPayload;

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
  public void setStockStateContract(tech.streamfusion.state.StockStateContract contract) {
    if (contract instanceof tech.streamfusion.state.StockIntervalJoinStateContract) {
      var interval = (tech.streamfusion.state.StockIntervalJoinStateContract) contract;
      if (proctime != interval.processingTime())
        throw new IllegalArgumentException(
            "The stock interval clock mode differs from the native operator");
      preserveProcessingPayload = interval.processingTime();
    }
    super.setStockStateContract(contract);
  }

  @Override
  protected long processingTimeTimerDeadlineForSnapshot() {
    return proctime ? registeredTimer : Long.MIN_VALUE;
  }

  @Override
  protected void beforeHandleCreation() {
    predicate.bind(new org.apache.flink.table.functions.FunctionContext(getRuntimeContext()));
  }

  // Proctime interval joins share the event-time state layout (rows buffer with clock
  // timestamps and evict on the clock instead of the watermark — the direct push already carries
  // both); the cleanup deadline rides the typed store's reserved key, so both modes run on the
  // direct store.
  @Override
  protected boolean usesDirectRocksDBState() {
    return withSchemas((l, r) -> Native.rocksdbIntervalJoinerSupported(l, r) ? 1L : 0L) != 0L;
  }

  @Override
  protected long createRocksDBHandle(
      RocksDBNativeStateSupport rocksdb, byte[][] restoredPartitions) {
    return withSchemas(
        (l, r) ->
            Native.createRocksDBIntervalJoiner(
                leftKeys,
                rightKeys,
                keyTimestampPrecisions(),
                nativeTime(true),
                nativeTime(false),
                lowerMillis,
                upperMillis,
                joinType,
                l,
                r,
                predicate.kinds,
                predicate.payload,
                predicate.childCounts,
                predicate.boundLongs(),
                predicate.doubles,
                predicate.strings,
                memoryBudgetBytes(),
                rocksdb.tableDirectory(),
                maxParallelism(),
                rocksdb.optionsJson(),
                rocksdb.sharedResourcesHandle(),
                rocksdb.sourceDirectories(),
                rocksdb.sourceSnapshotTokens(),
                rocksdb.keyGroupStart(),
                rocksdb.keyGroupEnd(),
                rocksdb.aligned(),
                restoredPartitions,
                restoredProcessingTimeTimerDeadline()));
  }

  @Override
  protected String[] checkpointRocksDBHandle(String snapshotDirectory) {
    return directRocksDBState()
        ? Native.checkpointRocksDBIntervalJoiner(
            handle, processingTimeTimerDeadlineForSnapshot(), snapshotDirectory)
        : super.checkpointRocksDBHandle(snapshotDirectory);
  }

  @Override
  protected long rocksdbTimerDeadline() {
    return Native.rocksdbIntervalJoinerTimerDeadline(handle);
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
                leftKeys,
                rightKeys,
                nativeTime(true),
                nativeTime(false),
                lowerMillis,
                upperMillis,
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
            Native.restoreIntervalJoinerPartitions(
                leftKeys,
                rightKeys,
                nativeTime(true),
                nativeTime(false),
                lowerMillis,
                upperMillis,
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
    return Native.snapshotIntervalJoinerPartitions(
        handle, maxParallelism(), keyTimestampPrecisions());
  }

  @Override
  protected java.util.function.LongConsumer memoryStateCopier() {
    final long source = handle;
    return target -> Native.copyIntervalJoinCanonicalMemoryState(source, target);
  }

  @Override
  protected void closeHandle() {
    closeCapturedHandle(handle, directRocksDBState());
  }

  @Override
  protected void closeCapturedHandle(long handle, boolean persistent) {
    if (persistent) {
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
    return withRowSchemas(nativeType(leftType), nativeType(rightType), call);
  }

  private RowType nativeType(RowType type) {
    if (!preserveProcessingPayload) return type;
    var fields = new java.util.ArrayList<RowType.RowField>();
    for (var field : type.getFields()) {
      var physical = field.getType();
      // Flink carries PROCTIME indicators as NULL placeholders, even when declared NOT NULL.
      if (org.apache.flink.table.types.logical.utils.LogicalTypeChecks.isProctimeAttribute(
          physical)) physical = physical.copy(true);
      fields.add(new RowType.RowField(field.getName(), physical));
    }
    String name = "__interval_cache_clock__";
    while (type.getFieldNames().contains(name)) name += "_";
    fields.add(
        new RowType.RowField(name, new org.apache.flink.table.types.logical.BigIntType(false)));
    return new RowType(fields);
  }

  private int nativeTime(boolean left) {
    return preserveProcessingPayload
        ? (left ? leftType : rightType).getFieldCount()
        : left ? leftTime : rightTime;
  }

  private void rearmCanonicalProcessingTimer() {
    long[] next = Native.intervalJoinCleanupDeadline(handle);
    long now = getProcessingTimeService().getCurrentProcessingTime();
    if (next.length != 0 && next[0] <= now) {
      advance(now);
      next = Native.intervalJoinCleanupDeadline(handle);
      if (next.length != 0 && next[0] <= now)
        throw new IllegalStateException("Interval processing cleanup did not advance its deadline");
    }
    if (next.length == 0) {
      registeredTimer = Long.MIN_VALUE;
      return;
    }
    if (next[0] != registeredTimer) getProcessingTimeService().registerTimer(next[0], this);
    registeredTimer = next[0];
  }

  @Override
  public void open() throws Exception {
    super.open();
    registeredTimer = Long.MIN_VALUE;
    if (preserveProcessingPayload) {
      rearmCanonicalProcessingTimer();
      return;
    }
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
      VectorSchemaRoot out =
          Data.importVectorSchemaRoot(allocator, outArray, outSchema, dictionaries);
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
      if (preserveProcessingPayload) {
        rearmCanonicalProcessingTimer();
        return;
      }
      // A row buffered at `now` is dead once the clock passes now + max(upper, -lower); schedule a
      // cleanup there so even with no further input the tail (and outer null-pads) drains. now only
      // advances, so the latest boundary scheduled covers every row buffered as of now.
      long horizon = Math.max(Math.max(upperMillis, -lowerMillis), 0);
      long boundary;
      try {
        boundary = Math.addExact(now, Math.addExact(horizon, 1));
      } catch (ArithmeticException overflow) {
        boundary = Long.MAX_VALUE;
      }
      if (boundary > registeredTimer) {
        getProcessingTimeService().registerTimer(boundary, this);
        registeredTimer = boundary;
      }
    }
  }

  @Override
  public void onProcessingTime(long time) {
    if (preserveProcessingPayload && time != registeredTimer) return;
    if (preserveProcessingPayload) registeredTimer = Long.MIN_VALUE;
    advance(getProcessingTimeService().getCurrentProcessingTime());
    publishStateBytes();
    if (preserveProcessingPayload) rearmCanonicalProcessingTimer();
  }

  @Override
  public void finish() throws Exception {
    // Stock processing-time state waits for real callbacks; EOF does not advance its clock.
    if (proctime && !preserveProcessingPayload) {
      advance(Long.MAX_VALUE); // end of input: evict everything (drains outer null-pads)
    }
    super.finish();
  }

  @Override
  public void processWatermark(Watermark mark) throws Exception {
    // Proctime joins evict on the processing-time clock, not the watermark; just forward it.
    if (proctime) {
      super.processWatermark(mark);
      return;
    }
    advance(mark.getTimestamp());
    publishStateBytes();
    var timeServices = getTimeServiceManager();
    if (timeServices.isPresent()) {
      timeServices.get().advanceWatermark(mark);
    }
    long delay = Math.max(-lowerMillis, upperMillis);
    output.emitWatermark(delay == 0 ? mark : new Watermark(mark.getTimestamp() - delay));
  }

  /**
   * Advances the eviction frontier, emitting any null-padded rows for evicted unmatched outer rows.
   */
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
      VectorSchemaRoot out =
          Data.importVectorSchemaRoot(allocator, outArray, outSchema, dictionaries);
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
