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
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.types.logical.RowType;

/**
 * Append-only streaming Top-N, fed Arrow batches and emitting Arrow batches. The changelog flows
 * Arrow with no per-operator transpose; each partitioned shuffle stays columnar where the input is a
 * columnar producer, and the row↔Arrow conversion happens only at the host edges. The output batch
 * carries the changelog kind on its {@code $row_kind$} column.
 */
public class NativeColumnarTopNOperator extends AbstractNativeStatefulOperator<ArrowBatch>
    implements OneInputStreamOperator<ArrowBatch, ArrowBatch> {

  private final int[] partitionColumns;
  private final RowType rowType;
  private final int[] sortIndices;
  private final int[] sortAscending;
  private final int[] sortNullsFirst;
  private final long offset;
  private final long limit;
  private final boolean outputRankNumber;
  private final boolean retracting;
  // Update-fast mode (Flink's UpdatableTopNFunction shape): the unique-key columns identifying the
  // row a record replaces; null for the append-only and retracting rankers.
  private final int[] rowKeyColumns;
  private final int[] rowKeyTimestampPrecisions;
  private final boolean netDiff;
  private final long miniBatchSize;
  private final long stateTtlMillis;

  private transient MiniBatchBoundary boundary;
  private transient MiniBatchMetrics miniBatchMetrics;
  private transient BatchCoalescer coalescer;

  public NativeColumnarTopNOperator(
      int[] partitionColumns,
      int[] keyTimestampPrecisions,
      RowType rowType,
      int[] sortIndices,
      int[] sortAscending,
      int[] sortNullsFirst,
      long offset,
      long limit,
      boolean outputRankNumber,
      boolean retracting,
      int[] rowKeyColumns,
      int[] rowKeyTimestampPrecisions,
      boolean netDiff,
      long miniBatchSize,
      long stateTtlMillis,
      int maxParallelism) {
    super("top-n", keyTimestampPrecisions, maxParallelism);
    this.partitionColumns = partitionColumns;
    this.rowType = rowType;
    this.sortIndices = sortIndices;
    this.sortAscending = sortAscending;
    this.sortNullsFirst = sortNullsFirst;
    this.offset = offset;
    this.limit = limit;
    this.outputRankNumber = outputRankNumber;
    this.retracting = retracting;
    this.rowKeyColumns = rowKeyColumns;
    this.rowKeyTimestampPrecisions = rowKeyTimestampPrecisions;
    this.netDiff = netDiff;
    this.miniBatchSize = miniBatchSize;
    this.stateTtlMillis = stateTtlMillis;
  }

  /** Whether this is Flink's UpdatableTopNFunction shape (a unique-keyed changelog input). */
  private boolean updateFast() {
    return rowKeyColumns != null;
  }

  @Override
  protected PaimonNativeStateSupport resolvePaimonState(boolean rawStateRestored) {
    // The ordinary rankers run on the Paimon list store: the append-only buffer is capped at N,
    // and the retracting buffer — unbounded, like Flink's own retractable Top-N state — rewrites a
    // touched partition once per checkpoint, strictly less than the per-record state rewrite
    // Flink's RetractableTopNFunction pays on RocksDB. The update-fast ranker runs on its own
    // row-keyed map shape (per-entry flushes under the row's unique-key bytes).
    // The support's retention is the compactor's per-row physical-cleanup bound. It is only
    // meaningful where every persisted row's ts is individually truthful — the append-only
    // ranker's per-element clocks, and the update-fast ranker's per-row-key clocks. The
    // retracting ranker expires the WHOLE buffer on the head element's clock and persists ts 0 on
    // the tail rows, so per-row cleanup would drop live state; its table advertises no retention
    // (the operator still expires logically).
    long compactionTtlMillis = retracting ? 0 : stateTtlMillis;
    return resolvePaimon(
        rawStateRestored,
        () ->
            withRowSchema(rowType, address -> Native.paimonRowStateSupported(address) ? 1L : 0L)
                != 0,
        compactionTtlMillis);
  }

  @Override
  protected long createPaimonHandle(PaimonNativeStateSupport paimon) {
    if (updateFast()) {
      return withRowSchema(
          rowType,
          rowSchemaAddress ->
              Native.createPaimonUpdateFastTopNRanker(
                  partitionColumns,
                  keyTimestampPrecisions(),
                  rowKeyColumns,
                  rowKeyTimestampPrecisions,
                  sortIndices,
                  sortAscending,
                  sortNullsFirst,
                  rowSchemaAddress,
                  limit,
                  outputRankNumber,
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
    return withRowSchema(
        rowType,
        rowSchemaAddress ->
            Native.createPaimonTopNRanker(
                partitionColumns,
                keyTimestampPrecisions(),
                sortIndices,
                sortAscending,
                sortNullsFirst,
                rowSchemaAddress,
                offset,
                limit,
                outputRankNumber,
                retracting,
                netDiff,
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
    return Native.checkpointPaimonTopNRanker(handle);
  }

  @Override
  protected long createHandle() {
    if (updateFast()) {
      return Native.createUpdateFastTopNRanker(
          partitionColumns,
          keyTimestampPrecisions(),
          rowKeyColumns,
          rowKeyTimestampPrecisions,
          sortIndices,
          sortAscending,
          sortNullsFirst,
          limit,
          outputRankNumber,
          stateTtlMillis,
          memoryBudgetBytes());
    }
    return Native.createTopNRanker(
        partitionColumns,
        keyTimestampPrecisions(),
        sortIndices,
        sortAscending,
        sortNullsFirst,
        offset,
        limit,
        outputRankNumber,
        retracting,
        netDiff,
        stateTtlMillis,
        memoryBudgetBytes());
  }

  @Override
  protected long restoreRawHandle(byte[][] snapshots) {
    if (updateFast()) {
      return Native.restoreUpdateFastTopNRankerPartitions(
          partitionColumns,
          keyTimestampPrecisions(),
          rowKeyColumns,
          rowKeyTimestampPrecisions,
          sortIndices,
          sortAscending,
          sortNullsFirst,
          limit,
          outputRankNumber,
          stateTtlMillis,
          getProcessingTimeService().getCurrentProcessingTime(),
          snapshots,
          memoryBudgetBytes());
    }
    return Native.restoreTopNRankerPartitions(
        partitionColumns,
        keyTimestampPrecisions(),
        sortIndices,
        sortAscending,
        sortNullsFirst,
        offset,
        limit,
        outputRankNumber,
        retracting,
        netDiff,
        stateTtlMillis,
        getProcessingTimeService().getCurrentProcessingTime(),
        snapshots,
        memoryBudgetBytes());
  }

  @Override
  protected byte[][] snapshotRawPartitions() {
    return Native.snapshotTopNRankerPartitions(handle, maxParallelism(), keyTimestampPrecisions());
  }

  @Override
  protected void closeHandle() {
    if (paimonState()) {
      Native.closePaimonTopNRanker(handle);
    } else {
      Native.closeTopNRanker(handle);
    }
  }

  @Override
  protected long stateBytesHandle() {
    return paimonState()
        ? Native.paimonTopNRankerStateBytes(handle)
        : Native.topNRankerStateBytes(handle);
  }

  @Override
  public void open() throws Exception {
    super.open();
    if (netDiff) {
      boundary = new MiniBatchBoundary(miniBatchSize);
      miniBatchMetrics = new MiniBatchMetrics(getMetricGroup());
    }
    coalescer = BatchCoalescer.create(getProcessingTimeService(), this::ingest);
  }

  @Override
  public void processElement(StreamRecord<ArrowBatch> element) {
    ColumnarRecordMetrics.countIngested(getMetricGroup(), element.getValue().rowCount());
    VectorSchemaRoot in = element.getValue().root();
    if (coalescer != null) {
      coalescer.add(in);
    } else {
      ingest(in);
    }
  }

  private void ingest(VectorSchemaRoot in) {
    if (!netDiff) {
      try {
        push(in);
      } finally {
        in.close();
      }
      publishStateBytes();
      return;
    }

    int rows = in.getRowCount();
    miniBatchMetrics.onPhysicalBatch();
    try {
      if (rows == 0) {
        push(in);
      } else {
        int offset = 0;
        while (offset < rows) {
          boolean firstContribution = offset == 0 || boundary.bufferedRows() == 0;
          int length = boundary.nextSliceLength(rows - offset);
          if (length < rows - offset) {
            miniBatchMetrics.onPhysicalBatchSplit();
          }
          if (offset == 0 && length == rows) {
            push(in);
          } else {
            try (VectorSchemaRoot slice = in.slice(offset, length)) {
              push(slice);
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

  private void push(VectorSchemaRoot in) {
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
        Native.pushPaimonTopNRanker(
            handle,
            inArray.memoryAddress(),
            inSchema.memoryAddress(),
            now,
            outArray.memoryAddress(),
            outSchema.memoryAddress());
      } else {
        Native.pushTopNRanker(
            handle,
            inArray.memoryAddress(),
            inSchema.memoryAddress(),
            now,
            outArray.memoryAddress(),
            outSchema.memoryAddress());
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
    if (coalescer != null) {
      coalescer.flush();
    }
    if (netDiff) {
      flushBundle(FlushReason.WATERMARK);
      publishStateBytes();
    }
    super.processWatermark(mark);
  }

  @Override
  public void prepareSnapshotPreBarrier(long checkpointId) throws Exception {
    if (coalescer != null) {
      coalescer.flush();
    }
    if (netDiff) {
      flushBundle(FlushReason.CHECKPOINT);
    }
    super.prepareSnapshotPreBarrier(checkpointId);
  }

  @Override
  public void finish() throws Exception {
    if (coalescer != null) {
      coalescer.flush();
    }
    if (netDiff) {
      flushBundle(FlushReason.FINISH);
    }
    super.finish();
  }

  private void flushBundle(FlushReason reason) {
    long transientBytes =
        paimonState()
            ? Native.paimonTopNRankerStagingBytes(handle)
            : Native.topNRankerStagingBytes(handle);
    long touchedPartitions =
        paimonState()
            ? Native.paimonTopNRankerStagedKeys(handle)
            : Native.topNRankerStagedPartitions(handle);
    try (ArrowArray outArray = ArrowArray.allocateNew(allocator);
        ArrowSchema outSchema = ArrowSchema.allocateNew(allocator)) {
      if (paimonState()) {
        Native.flushPaimonTopNRanker(handle, outArray.memoryAddress(), outSchema.memoryAddress());
      } else {
        Native.flushTopNRanker(handle, outArray.memoryAddress(), outSchema.memoryAddress());
      }
      VectorSchemaRoot out =
          Data.importVectorSchemaRoot(allocator, outArray, outSchema, dictionaries);
      int outputRows = out.getRowCount();
      miniBatchMetrics.onFlush(reason, outputRows, touchedPartitions, transientBytes);
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
    if (coalescer != null) {
      coalescer.close();
      coalescer = null;
    }
    super.close();
  }
}
