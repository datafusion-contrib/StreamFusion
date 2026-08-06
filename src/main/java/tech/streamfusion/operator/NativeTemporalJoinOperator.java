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
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.types.logical.RowType;

/**
 * Columnar event-time temporal table join (Arrow in on both inputs, Arrow out):
 * {@code probe JOIN versioned FOR SYSTEM_TIME AS OF probe.rowtime ON probe.k = versioned.k}. The
 * probe (left) input is a regular stream; the build (right) input is a versioned changelog (an
 * upsert/retract stream keyed by the equi-join key). Both inputs are buffered natively; on a watermark
 * the native joiner emits, for each buffered probe row the watermark has passed, the join with the
 * build version valid at the probe row's time — a buffer-then-emit-on-watermark flow like the window
 * join, but resolving a versioned lookup rather than a window match. The versioned state, the
 * per-probe-row lookup, and the state cleanup live natively; this layer moves batches across the
 * bridge and owns the handle's checkpointed state.
 *
 * <p>Flink delivers each input's watermark to {@code processWatermark1}/{@code processWatermark2}; the
 * base operator combines them into the minimum and calls {@link #processWatermark}, which advances the
 * joiner (emitting the now-resolvable probe rows) before forwarding the watermark downstream. Output
 * carries the changelog kind on the {@code $row_kind$} column (the probe row's kind, as Flink does).
 *
 * <p>Only INNER and LEFT are possible (Flink rejects RIGHT/FULL for a temporal join), so only the
 * build side can be absent; a LEFT join null-pads a probe row that finds no valid version. Processing
 * time is intentionally unsupported — Flink itself rejects a processing-time temporal table join.
 */
public class NativeTemporalJoinOperator extends AbstractNativeStatefulOperator<ArrowBatch>
    implements TwoInputStreamOperator<ArrowBatch, ArrowBatch, ArrowBatch> {

  private final int[] leftKeys;
  private final int[] rightKeys;
  private final int leftTime;
  private final int rightTime;
  private final int joinType;
  private final RowType leftType;
  private final RowType rightType;
  private final EncodedPredicate predicate;
  private final long stateTtlMillis;

  public NativeTemporalJoinOperator(
      int[] leftKeys,
      int[] rightKeys,
      int leftTime,
      int rightTime,
      int joinType,
      RowType leftType,
      RowType rightType,
      EncodedPredicate predicate,
      int[] keyTimestampPrecisions,
      long stateTtlMillis,
      int maxParallelism) {
    super("temporal join", keyTimestampPrecisions, maxParallelism);
    this.leftKeys = leftKeys;
    this.rightKeys = rightKeys;
    this.leftTime = leftTime;
    this.rightTime = rightTime;
    this.joinType = joinType;
    this.leftType = leftType;
    this.rightType = rightType;
    this.predicate = predicate;
    this.stateTtlMillis = stateTtlMillis;
  }

  @Override
  protected void beforeHandleCreation() {
    predicate.bind(new org.apache.flink.table.functions.FunctionContext(getRuntimeContext()));
  }

  @Override
  protected PaimonNativeStateSupport resolvePaimonState(boolean rawStateRestored) {
    // Deliberately the retention-less resolvePaimon: the persistent deadlines are not truthful
    // per-row clocks (a deferred or re-armed deadline must never drive a physical drop), so the
    // maintenance session gets no record-level expiry options — physical cleanup happens through
    // the operator's own staged tombstones when a deadline fires.
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
            Native.createPaimonTemporalJoiner(
                leftKeys,
                rightKeys,
                leftTime,
                rightTime,
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
                stateTtlMillis,
                getProcessingTimeService().getCurrentProcessingTime(),
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
    return Native.checkpointPaimonTemporalJoiner(handle);
  }

  @Override
  protected long createHandle() {
    return withSchemas(
        (l, r) ->
            Native.createTemporalJoiner(
                leftKeys,
                rightKeys,
                leftTime,
                rightTime,
                joinType,
                l,
                r,
                predicate.kinds,
                predicate.payload,
                predicate.childCounts,
                predicate.boundLongs(),
                predicate.doubles,
                predicate.strings,
                stateTtlMillis,
                memoryBudgetBytes()));
  }

  @Override
  protected long restoreRawHandle(byte[][] snapshots) {
    return withSchemas(
        (l, r) ->
            Native.restoreTemporalJoinerPartitions(
                leftKeys,
                rightKeys,
                leftTime,
                rightTime,
                joinType,
                l,
                r,
                predicate.kinds,
                predicate.payload,
                predicate.childCounts,
                predicate.boundLongs(),
                predicate.doubles,
                predicate.strings,
                stateTtlMillis,
                getProcessingTimeService().getCurrentProcessingTime(),
                snapshots,
                memoryBudgetBytes()));
  }

  @Override
  protected byte[][] snapshotRawPartitions() {
    return Native.snapshotTemporalJoinerPartitions(
        handle, maxParallelism(), keyTimestampPrecisions());
  }

  @Override
  protected void closeHandle() {
    Native.closeTemporalJoiner(handle);
  }

  @Override
  protected long stateBytesHandle() {
    return Native.temporalJoinerStateBytes(handle);
  }

  /**
   * Exports both side row types as FFI Arrow schemas for the duration of one native call. The
   * joiner takes both up front so a LEFT join can type the null-padding for the build side before
   * that side's first batch arrives.
   */
  private long withSchemas(LongBinaryOperator call) {
    return withRowSchemas(leftType, rightType, call);
  }

  @Override
  public void open() throws Exception {
    super.open();
  }

  @Override
  public void processElement1(StreamRecord<ArrowBatch> element) {
    ColumnarRecordMetrics.countIngested(getMetricGroup(), element.getValue().rowCount());
    buffer(element.getValue(), true);
    publishStateBytes();
  }

  @Override
  public void processElement2(StreamRecord<ArrowBatch> element) {
    ColumnarRecordMetrics.countIngested(getMetricGroup(), element.getValue().rowCount());
    buffer(element.getValue(), false);
    publishStateBytes();
  }

  /** Hands a batch to its side of the joiner, which buffers it (no output until a watermark). */
  private void buffer(ArrowBatch batch, boolean left) {
    VectorSchemaRoot in = batch.root();
    BufferAllocator inAllocator =
        in.getFieldVectors().isEmpty() ? allocator : in.getFieldVectors().get(0).getAllocator();
    try (ArrowArray array = ArrowArray.allocateNew(inAllocator);
        ArrowSchema schema = ArrowSchema.allocateNew(inAllocator)) {
      Data.exportVectorSchemaRoot(inAllocator, in, dictionaries, array, schema);
      // Flink's cleanup-timer clock: the processing-time service is System.currentTimeMillis in
      // production and harness-controlled in tests, so expiry is deterministic to test.
      long now = getProcessingTimeService().getCurrentProcessingTime();
      if (left) {
        Native.pushLeftTemporalJoiner(handle, array.memoryAddress(), schema.memoryAddress(), now);
      } else {
        Native.pushRightTemporalJoiner(handle, array.memoryAddress(), schema.memoryAddress(), now);
      }
    } finally {
      in.close();
    }
  }

  @Override
  public void processWatermark(Watermark mark) throws Exception {
    advance(mark.getTimestamp());
    publishStateBytes();
    super.processWatermark(mark);
  }

  @Override
  public void finish() throws Exception {
    advance(Long.MAX_VALUE); // end of input: resolve every remaining buffered probe row
    super.finish();
  }

  /** Advances the watermark, emitting the joined rows for buffered probe rows it has passed. */
  private void advance(long watermark) {
    try (ArrowArray array = ArrowArray.allocateNew(allocator);
        ArrowSchema schema = ArrowSchema.allocateNew(allocator)) {
      Native.advanceTemporalJoiner(
          handle,
          watermark,
          getProcessingTimeService().getCurrentProcessingTime(),
          array.memoryAddress(),
          schema.memoryAddress());
      VectorSchemaRoot out = Data.importVectorSchemaRoot(allocator, array, schema, dictionaries);
      if (out.getRowCount() > 0) {
        ColumnarRecordMetrics.emit(output, getMetricGroup(), new ArrowBatch(out));
      } else {
        out.close(); // no probe rows resolvable at this watermark
      }
    }
  }

  @Override
  public void close() throws Exception {
    super.close();
    predicate.unbind();
  }
}
