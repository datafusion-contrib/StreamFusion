package tech.streamfusion.operator;

import tech.streamfusion.Native;
import tech.streamfusion.operator.MiniBatchMetrics.FlushReason;
import tech.streamfusion.planner.NativeConfig;
import tech.streamfusion.state.PaimonNativeStateSupport;
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
public class NativeColumnarUpdatingJoinOperator
    extends AbstractNativeStatefulOperator<ArrowBatch>
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
  private final boolean leftJoinKeyUnique;
  private final boolean rightJoinKeyUnique;
  private final boolean miniBatch;
  private final long miniBatchSize;
  private final long leftStateTtlMillis;
  private final long rightStateTtlMillis;

  private transient long[] boundPredLongs;
  private transient MiniBatchBoundary boundary;
  private transient MiniBatchMetrics miniBatchMetrics;
  private transient BatchCoalescer leftCoalescer;
  private transient BatchCoalescer rightCoalescer;

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
      boolean leftJoinKeyUnique,
      boolean rightJoinKeyUnique,
      boolean miniBatch,
      long miniBatchSize,
      long leftStateTtlMillis,
      long rightStateTtlMillis,
      int maxParallelism) {
    super("updating join", keyTimestampPrecisions, maxParallelism);
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
    this.leftJoinKeyUnique = leftJoinKeyUnique;
    this.rightJoinKeyUnique = rightJoinKeyUnique;
    this.miniBatch = miniBatch;
    this.miniBatchSize = miniBatchSize;
    this.leftStateTtlMillis = leftStateTtlMillis;
    this.rightStateTtlMillis = rightStateTtlMillis;
  }

  // The residual predicate's bound longs must exist before any create/restore call reads them.
  @Override
  protected void beforeHandleCreation() {
    boundPredLongs =
        predBinding.bind(
            predLongs,
            new org.apache.flink.table.functions.FunctionContext(getRuntimeContext()));
  }

  @Override
  protected PaimonNativeStateSupport resolvePaimonState(boolean rawStateRestored) {
    // The support object exposes one retention for the compactor's physical cleanup, but the
    // sides expire independently: a single bound is only safe when BOTH sides expire, and then
    // only the larger one (dropping rows older than max(left, right) can never drop a live row
    // on either side). With either side unbounded, no shared cleanup bound exists.
    long compactionTtlMillis =
        leftStateTtlMillis == 0 || rightStateTtlMillis == 0
            ? 0
            : Math.max(leftStateTtlMillis, rightStateTtlMillis);
    return resolvePaimon(
        rawStateRestored,
        () -> rowTypeSupported(leftType) && rowTypeSupported(rightType),
        compactionTtlMillis);
  }

  /** Whether one side's row type is persistable, probed over a one-call FFI schema export. */
  private static boolean rowTypeSupported(RowType rowType) {
    return withRowSchema(rowType, address -> Native.paimonRowStateSupported(address) ? 1L : 0L) != 0;
  }

  // The joiner needs both sides' Arrow schemas up front (to type outer null-padding); they are
  // exported through the C Data Interface for the create/restore call to import.
  @Override
  protected long createPaimonHandle(PaimonNativeStateSupport paimon) {
    return withRowSchemas(
        leftType,
        rightType,
        (left, right) ->
            Native.createPaimonUpdatingJoiner(
                leftKeys,
                rightKeys,
                keyTimestampPrecisions(),
                joinType,
                left,
                right,
                predKinds,
                predPayload,
                predChildCounts,
                boundPredLongs,
                predDoubles,
                predStrings,
                leftJoinKeyUnique,
                rightJoinKeyUnique,
                miniBatch,
                leftStateTtlMillis,
                rightStateTtlMillis,
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
    return Native.checkpointPaimonUpdatingJoiner(handle);
  }

  @Override
  protected long createHandle() {
    return withRowSchemas(
        leftType,
        rightType,
        (left, right) ->
            Native.createUpdatingJoiner(
                leftKeys,
                rightKeys,
                keyTimestampPrecisions(),
                joinType,
                left,
                right,
                predKinds,
                predPayload,
                predChildCounts,
                boundPredLongs,
                predDoubles,
                predStrings,
                leftJoinKeyUnique,
                rightJoinKeyUnique,
                miniBatch,
                leftStateTtlMillis,
                rightStateTtlMillis,
                memoryBudgetBytes()));
  }

  @Override
  protected long restoreRawHandle(byte[][] snapshots) {
    return withRowSchemas(
        leftType,
        rightType,
        (left, right) ->
            Native.restoreUpdatingJoinerPartitions(
                leftKeys,
                rightKeys,
                keyTimestampPrecisions(),
                joinType,
                left,
                right,
                predKinds,
                predPayload,
                predChildCounts,
                boundPredLongs,
                predDoubles,
                predStrings,
                leftJoinKeyUnique,
                rightJoinKeyUnique,
                miniBatch,
                leftStateTtlMillis,
                rightStateTtlMillis,
                getProcessingTimeService().getCurrentProcessingTime(),
                snapshots,
                memoryBudgetBytes()));
  }

  @Override
  protected byte[][] snapshotRawPartitions() {
    return Native.snapshotUpdatingJoinerPartitions(
        handle, maxParallelism(), keyTimestampPrecisions());
  }

  @Override
  protected void closeHandle() {
    if (paimonState()) {
      Native.closePaimonUpdatingJoiner(handle);
    } else {
      Native.closeUpdatingJoiner(handle);
    }
  }

  @Override
  protected long stateBytesHandle() {
    return paimonState()
        ? Native.paimonUpdatingJoinerStateBytes(handle)
        : Native.updatingJoinerStateBytes(handle);
  }

  @Override
  public void open() throws Exception {
    super.open();
    if (miniBatch) {
      boundary = new MiniBatchBoundary(miniBatchSize);
      miniBatchMetrics = new MiniBatchMetrics(getMetricGroup());
    }
    leftCoalescer = BatchCoalescer.create(getProcessingTimeService(), in -> ingest(in, true));
    rightCoalescer = BatchCoalescer.create(getProcessingTimeService(), in -> ingest(in, false));
  }

  @Override
  public void processElement1(StreamRecord<ArrowBatch> element) {
    ColumnarRecordMetrics.countIngested(getMetricGroup(), element.getValue().rowCount());
    ingestSide(element.getValue().root(), true);
  }

  @Override
  public void processElement2(StreamRecord<ArrowBatch> element) {
    ColumnarRecordMetrics.countIngested(getMetricGroup(), element.getValue().rowCount());
    ingestSide(element.getValue().root(), false);
  }

  private void ingestSide(VectorSchemaRoot in, boolean left) {
    if (leftCoalescer == null) {
      ingest(in, left);
      return;
    }
    // The join changelog depends on the interleaving of the two inputs, so cross-side order must
    // survive coalescing: draining the other side before buffering this one means at most one side
    // is ever pending, and batches still reach the joiner in arrival order.
    (left ? rightCoalescer : leftCoalescer).flush();
    (left ? leftCoalescer : rightCoalescer).add(in);
  }

  private void ingest(VectorSchemaRoot in, boolean left) {
    process(in, left);
    publishStateBytes();
  }

  private void process(VectorSchemaRoot in, boolean left) {
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
      // Flink's TtlTimeProvider clock: the processing-time service is System.currentTimeMillis in
      // production and harness-controlled in tests, so expiry is deterministic to test.
      long now = getProcessingTimeService().getCurrentProcessingTime();
      if (paimonState()) {
        if (left) {
          Native.pushLeftPaimonUpdatingJoiner(
              handle, inArray.memoryAddress(), inSchema.memoryAddress(), now,
              outArray.memoryAddress(), outSchema.memoryAddress());
        } else {
          Native.pushRightPaimonUpdatingJoiner(
              handle, inArray.memoryAddress(), inSchema.memoryAddress(), now,
              outArray.memoryAddress(), outSchema.memoryAddress());
        }
      } else if (left) {
        Native.pushLeftUpdatingJoiner(
            handle, inArray.memoryAddress(), inSchema.memoryAddress(), now,
            outArray.memoryAddress(), outSchema.memoryAddress());
      } else {
        Native.pushRightUpdatingJoiner(
            handle, inArray.memoryAddress(), inSchema.memoryAddress(), now,
            outArray.memoryAddress(), outSchema.memoryAddress());
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
  public void processWatermark(Watermark mark) throws Exception {
    flushCoalescers();
    if (miniBatch) {
      flushBundle(FlushReason.WATERMARK);
      publishStateBytes();
    }
    super.processWatermark(mark);
  }

  @Override
  public void prepareSnapshotPreBarrier(long checkpointId) throws Exception {
    flushCoalescers();
    if (miniBatch) {
      flushBundle(FlushReason.CHECKPOINT);
    }
    super.prepareSnapshotPreBarrier(checkpointId);
  }

  @Override
  public void finish() throws Exception {
    flushCoalescers();
    if (miniBatch) {
      flushBundle(FlushReason.FINISH);
    }
    super.finish();
  }

  private void flushCoalescers() {
    if (leftCoalescer != null) {
      leftCoalescer.flush();
      rightCoalescer.flush();
    }
  }

  private void flushBundle(FlushReason reason) {
    long transientBytes =
        paimonState()
            ? Native.paimonUpdatingJoinerStagingBytes(handle)
            : Native.updatingJoinerStagingBytes(handle);
    long touchedKeys =
        paimonState()
            ? Native.paimonUpdatingJoinerStagedKeys(handle)
            : Native.updatingJoinerStagedKeys(handle);
    try (ArrowArray outArray = ArrowArray.allocateNew(allocator);
        ArrowSchema outSchema = ArrowSchema.allocateNew(allocator)) {
      if (paimonState()) {
        Native.flushPaimonUpdatingJoiner(
            handle, outArray.memoryAddress(), outSchema.memoryAddress());
      } else {
        Native.flushUpdatingJoiner(handle, outArray.memoryAddress(), outSchema.memoryAddress());
      }
      VectorSchemaRoot out =
          Data.importVectorSchemaRoot(allocator, outArray, outSchema, dictionaries);
      int outputRows = out.getRowCount();
      miniBatchMetrics.onFlush(reason, outputRows, touchedKeys, transientBytes);
      if (outputRows > 0) {
        ColumnarRecordMetrics.emit(output, getMetricGroup(), new ArrowBatch(out));
      } else {
        out.close();
      }
    }
    boundary.reset();
  }

  @Override
  public void close() throws Exception {
    if (leftCoalescer != null) {
      leftCoalescer.close();
      rightCoalescer.close();
      leftCoalescer = null;
      rightCoalescer = null;
    }
    super.close();
    predBinding.unbind();
  }
}
