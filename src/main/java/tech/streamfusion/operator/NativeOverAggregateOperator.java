package tech.streamfusion.operator;

import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.TimeStampMilliVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.types.logical.RowType;
import tech.streamfusion.Native;
import tech.streamfusion.arrow.TimestampAccessor;
import tech.streamfusion.state.RocksDBNativeStateSupport;

/**
 * Columnar event-time {@code OVER (… ORDER BY rt RANGE UNBOUNDED PRECEDING)} aggregation: Arrow in,
 * Arrow out. Each input batch is buffered natively; on a watermark the native aggregator emits the
 * rows it has completed (rowtime past the watermark) with the running aggregate column(s) appended,
 * the input columns passing through — so the data stays columnar end to end. The buffering, the
 * per-key running fold, and the late-data drop all live in the native operator; this layer only
 * moves batches across the bridge and owns the handle's checkpointed state. On the RocksDB backend
 * the pending rows and the per-key fold state live in the persistent store (write buffers + disk
 * tables) and the watermark firing is a range read over both; memory state travels as raw
 * keyed-state blobs.
 */
public class NativeOverAggregateOperator extends AbstractNativeStatefulOperator<ArrowBatch>
    implements OneInputStreamOperator<ArrowBatch, ArrowBatch> {

  private final int timeColumn;
  private final int[] valueColumns;
  private final int[] keyColumns;
  private final int[] valueTypes;
  private final int[] aggregateKinds;
  private final int frameKind;
  private final long frameOffset;
  private final boolean proctime;
  private final long stateTtlMillis;
  private final RowType rowType;
  private boolean eventCleanupTimers;
  private RowType constantType;
  private byte[] constantValues;
  private transient RowType nativeRowType;
  private transient org.apache.flink.table.data.RowData constantRow;
  private transient boolean mapListInput;
  private transient Counter numLateRecordsDropped;
  private transient long reportedLateRecords;
  private transient java.util.concurrent.ScheduledFuture<?> processingCleanup;
  private transient long processingCleanupDeadline;
  private transient boolean processingCleanupArmed;
  private transient long processingCleanupGeneration;

  public NativeOverAggregateOperator(
      int timeColumn,
      int[] valueColumns,
      int[] keyColumns,
      int[] valueTypes,
      int[] aggregateKinds,
      int frameKind,
      long frameOffset,
      boolean proctime,
      int[] keyTimestampPrecisions,
      long stateTtlMillis,
      RowType rowType,
      int maxParallelism) {
    super("over aggregate", keyTimestampPrecisions, maxParallelism);
    this.timeColumn = timeColumn;
    this.valueColumns = valueColumns;
    this.keyColumns = keyColumns;
    this.valueTypes = valueTypes;
    this.aggregateKinds = aggregateKinds;
    this.frameKind = frameKind;
    this.frameOffset = frameOffset;
    this.proctime = proctime;
    this.stateTtlMillis = stateTtlMillis;
    this.rowType = rowType;
  }

  public NativeOverAggregateOperator withEventTimeCleanupTimers() {
    if (proctime || (frameKind != 0 && frameKind != 3) || stateTtlMillis <= 1)
      throw new IllegalArgumentException("Event cleanup requires retained unbounded rowtime OVER");
    eventCleanupTimers = true;
    return this;
  }

  @Override
  protected java.util.function.LongConsumer memoryStateCopier() {
    final long source = handle;
    return target -> Native.copyOverCanonicalMemoryState(source, target);
  }

  @Override
  protected void prepareCapturedReader(long captured) {
    Native.prepareOverCanonicalReadView(captured);
  }

  @Override
  protected void configureCapturedHandle(long captured, boolean persistent) {
    if (eventCleanupTimers) Native.bindOverEventCleanupTimers(captured);
  }

  @Override
  protected void configureStateHandle() {
    if (eventCleanupTimers) Native.bindOverEventCleanupTimers(handle);
  }

  public NativeOverAggregateOperator withConstantArguments(RowType type, byte[] values) {
    if (type == null || type.getFieldCount() == 0 || values == null || values.length == 0)
      throw new IllegalArgumentException(
          "OVER constant arguments require types and encoded values");
    var fields = new java.util.ArrayList<RowType.RowField>();
    var names = new java.util.HashSet<>(rowType.getFieldNames());
    for (int i = 0; i < type.getFieldCount(); i++) {
      String name = "__over_constant_" + i;
      while (!names.add(name)) name += "_";
      fields.add(new RowType.RowField(name, type.getTypeAt(i)));
    }
    constantType = new RowType(fields);
    constantValues = values.clone();
    nativeRowType = null;
    constantRow = null;
    return this;
  }

  private RowType nativeRowType() {
    if (constantType == null) return rowType;
    if (nativeRowType == null) {
      var fields = new java.util.ArrayList<>(rowType.getFields());
      fields.addAll(constantType.getFields());
      nativeRowType = new RowType(fields);
    }
    return nativeRowType;
  }

  @Override
  protected void beforeHandleCreation() {
    super.beforeHandleCreation();
    mapListInput =
        tech.streamfusion.arrow.MapListRepresentation.requiredForCounts(
            nativeRowType(), valueColumns, aggregateKinds);
  }

  private VectorSchemaRoot constantBatch(int rows, BufferAllocator allocator) {
    if (constantRow == null) {
      try {
        constantRow =
            new org.apache.flink.table.runtime.typeutils.RowDataSerializer(constantType)
                .deserialize(
                    new org.apache.flink.core.memory.DataInputDeserializer(constantValues));
      } catch (java.io.IOException failure) {
        throw new IllegalStateException("Cannot decode OVER constant arguments", failure);
      }
    }
    return RowDataArrowConverter.write(
        java.util.Collections.nCopies(rows, constantRow), constantType, allocator);
  }

  private VectorSchemaRoot withoutConstantArguments(VectorSchemaRoot root) {
    if (constantType == null || root.getRowCount() == 0) return root;
    int inputArity = rowType.getFieldCount();
    int nativeArity = nativeRowType().getFieldCount();
    if (root.getFieldVectors().size() < nativeArity) {
      root.close();
      throw new IllegalStateException("OVER output is missing constant argument columns");
    }
    var fields = new java.util.ArrayList<>(root.getFieldVectors().subList(0, inputArity));
    fields.addAll(root.getFieldVectors().subList(nativeArity, root.getFieldVectors().size()));
    var result = new VectorSchemaRoot(fields);
    result.setRowCount(root.getRowCount());
    // The remaining vectors move to result; only the internal arguments are released here.
    for (int i = inputArity; i < nativeArity; i++) root.getVector(i).close();
    return result;
  }

  @Override
  protected boolean usesDirectRocksDBState() {
    return withRowSchema(
            nativeRowType(),
            schemaAddress ->
                Native.rocksdbOverAggregatorSupported(
                        valueTypes, aggregateKinds, frameKind, proctime, schemaAddress)
                    ? 1
                    : 0)
        == 1;
  }

  @Override
  protected long createRocksDBHandle(
      RocksDBNativeStateSupport rocksdb, byte[][] restoredPartitions) {
    long now = getProcessingTimeService().getCurrentProcessingTime();
    return withRowSchema(
        nativeRowType(),
        schemaAddress ->
            Native.createRocksDBOverAggregator(
                valueTypes,
                aggregateKinds,
                timeColumn,
                valueColumns,
                keyColumns,
                frameKind,
                frameOffset,
                proctime,
                keyTimestampPrecisions(),
                stateTtlMillis,
                now,
                memoryBudgetBytes(),
                schemaAddress,
                rocksdb.tableDirectory(),
                maxParallelism(),
                rocksdb.optionsJson(),
                rocksdb.sharedResourcesHandle(),
                rocksdb.sourceDirectories(),
                rocksdb.sourceSnapshotTokens(),
                rocksdb.keyGroupStart(),
                rocksdb.keyGroupEnd(),
                rocksdb.aligned(),
                restoredPartitions));
  }

  @Override
  protected String[] checkpointRocksDBHandle(String snapshotDirectory) {
    return directRocksDBState()
        ? Native.checkpointRocksDBOverAggregator(handle, snapshotDirectory)
        : super.checkpointRocksDBHandle(snapshotDirectory);
  }

  @Override
  protected byte[][] snapshotCanonicalPartitions() {
    return directRocksDBState()
        ? Native.snapshotRocksDBOverAggregatorPartitions(
            handle, maxParallelism(), keyTimestampPrecisions())
        : snapshotRawPartitions();
  }

  @Override
  protected long createHandle() {
    return Native.createOverAggregator(
        valueTypes,
        aggregateKinds,
        timeColumn,
        valueColumns,
        keyColumns,
        frameKind,
        frameOffset,
        proctime,
        stateTtlMillis,
        memoryBudgetBytes());
  }

  @Override
  protected long restoreRawHandle(byte[][] snapshots) {
    long restoredHandle =
        Native.restoreOverAggregatorPartitions(
            valueTypes,
            aggregateKinds,
            timeColumn,
            valueColumns,
            keyColumns,
            frameKind,
            frameOffset,
            proctime,
            stateTtlMillis,
            getProcessingTimeService().getCurrentProcessingTime(),
            snapshots,
            memoryBudgetBytes());
    try {
      boolean canonical =
          getContainingTask()
              .getEnvironment()
              .getTaskStateManager()
              .prioritizedOperatorState(getOperatorID())
              .getJobManagerManagedKeyedState()
              .stream()
              .anyMatch(
                  state ->
                      state instanceof org.apache.flink.runtime.state.SavepointKeyedStateHandle);
      if (canonical) Native.restoreOverAggregatorCanonicalRangeOrder(restoredHandle, snapshots);
      return restoredHandle;
    } catch (RuntimeException | Error failure) {
      Native.closeOverAggregator(restoredHandle);
      throw failure;
    }
  }

  @Override
  protected byte[][] snapshotRawPartitions() {
    return Native.snapshotOverAggregatorPartitions(
        handle, maxParallelism(), keyTimestampPrecisions());
  }

  @Override
  protected void closeHandle() {
    processingCleanupArmed = false;
    processingCleanupGeneration++;
    if (processingCleanup != null) {
      processingCleanup.cancel(false);
      processingCleanup = null;
    }
    closeCapturedHandle(handle, directRocksDBState());
  }

  @Override
  protected void closeCapturedHandle(long handle, boolean persistent) {
    if (persistent) {
      Native.closeRocksDBOverAggregator(handle);
    } else {
      Native.closeOverAggregator(handle);
    }
  }

  @Override
  protected long stateBytesHandle() {
    return directRocksDBState()
        ? Native.rocksdbOverAggregatorStateBytes(handle)
        : Native.overAggregatorStateBytes(handle);
  }

  @Override
  public void open() throws Exception {
    super.open();
    if ((frameKind == 1 || frameKind == 2) && !directRocksDBState()) {
      boolean incrementalRange = false;
      for (int i = 0; i < aggregateKinds.length; i++) {
        int kind = aggregateKinds[i] % 100;
        if ((!proctime
                && frameKind == 2
                && ((valueTypes[i] >= 2000 && (kind == 0 || kind == 4 || kind == 22))
                    || ((valueTypes[i] == 1 || valueTypes[i] == 6)
                        && (kind == 0 || kind == 4 || kind == 22))))
            || (valueTypes[i] == 3
                && (aggregateKinds[i] == 1
                    || aggregateKinds[i] == 2
                    || aggregateKinds[i] == 5
                    || aggregateKinds[i] == 6))) {
          incrementalRange = true;
          break;
        }
      }
      if (incrementalRange)
        Native.setOverAggregatorHeapSnapshotProbe(
            handle,
            new tech.streamfusion.state.OverHeapSnapshotProbe(
                (org.apache.flink.runtime.state.CheckpointableKeyedStateBackend<?>)
                    getKeyedStateBackend()),
            maxParallelism());
    }
    if (!proctime) {
      numLateRecordsDropped = getMetricGroup().counter("numLateRecordsDropped");
    }
    if (processingCleanupEnabled()) {
      if (!eventCleanupTimers) Native.bindOverProcessingRowsCleanupTimers(handle);
      armProcessingCleanup();
    }
  }

  private boolean processingCleanupEnabled() {
    return eventCleanupTimers || proctime && frameKind == 1 && stateTtlMillis > 1;
  }

  private void armProcessingCleanup() {
    if (!processingCleanupEnabled()) return;
    long[] next =
        eventCleanupTimers
            ? Native.nextOverEventCleanup(handle)
            : Native.nextOverProcessingRowsCleanup(handle);
    if (processingCleanupArmed && next.length == 1 && processingCleanupDeadline == next[0]) return;
    if (processingCleanup != null) processingCleanup.cancel(false);
    processingCleanup = null;
    processingCleanupArmed = false;
    if (next.length == 1) {
      long generation = ++processingCleanupGeneration;
      processingCleanupDeadline = next[0];
      processingCleanupArmed = true;
      var future =
          getProcessingTimeService()
              .registerTimer(next[0], timestamp -> fireProcessingCleanup(timestamp, generation));
      if (processingCleanupArmed && processingCleanupGeneration == generation)
        processingCleanup = future;
      else future.cancel(false);
    }
  }

  private void fireProcessingCleanup(long timestamp, long generation) {
    if (!processingCleanupArmed
        || generation != processingCleanupGeneration
        || processingCleanupDeadline != timestamp) return;
    processingCleanupArmed = false;
    processingCleanup = null;
    if (eventCleanupTimers)
      Native.fireOverEventCleanup(
          handle, timestamp, getProcessingTimeService().getCurrentProcessingTime());
    else Native.fireOverProcessingRowsCleanup(handle, timestamp);
    publishStateBytes();
    armProcessingCleanup();
  }

  @Override
  public void processElement(StreamRecord<ArrowBatch> element) {
    ColumnarRecordMetrics.countIngested(getMetricGroup(), element.getValue().rowCount());
    VectorSchemaRoot in = element.getValue().root();
    BufferAllocator inAllocator =
        in.getFieldVectors().isEmpty() ? allocator : in.getFieldVectors().get(0).getAllocator();
    java.util.List<FieldVector> compactCopies = new java.util.ArrayList<>();
    VectorSchemaRoot constants = null;
    VectorSchemaRoot prepared = in;
    try {
      if (constantType != null) {
        constants = constantBatch(in.getRowCount(), inAllocator);
        int arity = rowType.getFieldCount();
        var fields = new java.util.ArrayList<>(in.getFieldVectors().subList(0, arity));
        fields.addAll(constants.getFieldVectors());
        fields.addAll(in.getFieldVectors().subList(arity, in.getFieldVectors().size()));
        prepared = new VectorSchemaRoot(fields);
        prepared.setRowCount(in.getRowCount());
      }
      java.util.List<FieldVector> columns = null;
      for (int a = 0; a < valueColumns.length; a++) {
        int code = valueTypes[a];
        int column = valueColumns[a];
        if (code < 1000 || code > 1003 || column < 0) continue;
        if (columns == null) columns = new java.util.ArrayList<>(prepared.getFieldVectors());
        if (columns.get(column) instanceof TimeStampMilliVector) continue;
        FieldVector source = columns.get(column);
        var target = new TimeStampMilliVector(source.getName(), inAllocator);
        compactCopies.add(target);
        target.allocateNew(in.getRowCount());
        var timestamps = new TimestampAccessor(source);
        for (int row = 0; row < in.getRowCount(); row++) {
          if (timestamps.isNull(row)) target.setNull(row);
          else target.set(row, timestamps.getMillis(row));
        }
        target.setValueCount(in.getRowCount());
        columns.set(column, target);
      }
      if (columns != null) {
        prepared = new VectorSchemaRoot(columns);
        prepared.setRowCount(in.getRowCount());
      }
      if (mapListInput) {
        var safeSchema = tech.streamfusion.arrow.MapListRepresentation.forIpc(prepared.getSchema());
        if (safeSchema != prepared.getSchema()) {
          // A borrowed schema view; C export retains the original buffers independently.
          prepared =
              new VectorSchemaRoot(safeSchema, prepared.getFieldVectors(), prepared.getRowCount());
        }
      }
      try (ArrowArray inArray = ArrowArray.allocateNew(inAllocator);
          ArrowSchema inSchema = ArrowSchema.allocateNew(inAllocator)) {
        Data.exportVectorSchemaRoot(inAllocator, prepared, dictionaries, inArray, inSchema);
        // Flink's retention clock: the processing-time service is System.currentTimeMillis in
        // production and harness-controlled in tests, so expiry is deterministic to test.
        long now = getProcessingTimeService().getCurrentProcessingTime();
        if (proctime) {
          // Proctime: fold in arrival order and emit this batch's rows immediately (no watermark).
          try (ArrowArray outArray = ArrowArray.allocateNew(allocator);
              ArrowSchema outSchema = ArrowSchema.allocateNew(allocator)) {
            Native.pushProctimeOverAggregator(
                handle,
                inArray.memoryAddress(),
                inSchema.memoryAddress(),
                now,
                outArray.memoryAddress(),
                outSchema.memoryAddress());
            VectorSchemaRoot out =
                withoutConstantArguments(
                    Data.importVectorSchemaRoot(allocator, outArray, outSchema, dictionaries));
            if (out.getRowCount() > 0) {
              ColumnarRecordMetrics.emit(output, getMetricGroup(), new ArrowBatch(out));
            } else {
              out.close();
            }
          }
        } else if (directRocksDBState()) {
          // Rowtime on direct RocksDB state: the rows append to the persistent pending table.
          Native.pushRocksDBOverAggregator(
              handle, inArray.memoryAddress(), inSchema.memoryAddress(), now);
        } else {
          // Rowtime: the native aggregator imports and keeps the batch (buffered until a watermark
          // completes these rows), so this side hands it off and closes its own view.
          Native.pushOverAggregator(handle, inArray.memoryAddress(), inSchema.memoryAddress(), now);
        }
      }
    } finally {
      for (FieldVector copy : compactCopies) copy.close();
      if (constants != null) constants.close();
      in.close();
    }
    publishStateBytes();
    armProcessingCleanup();
    if (!proctime) {
      long lateRecords = Native.overAggregatorLateDrops(handle);
      if (lateRecords > reportedLateRecords) {
        numLateRecordsDropped.inc(lateRecords - reportedLateRecords);
        reportedLateRecords = lateRecords;
      }
    }
  }

  @Override
  public void processWatermark(Watermark mark) throws Exception {
    if (proctime) {
      super.processWatermark(mark); // proctime emits eagerly in processElement; nothing to flush
      return;
    }
    try (ArrowArray array = ArrowArray.allocateNew(allocator);
        ArrowSchema schema = ArrowSchema.allocateNew(allocator)) {
      if (directRocksDBState()) {
        Native.flushRocksDBOverAggregator(
            handle,
            mark.getTimestamp(),
            getProcessingTimeService().getCurrentProcessingTime(),
            array.memoryAddress(),
            schema.memoryAddress());
      } else {
        Native.flushOverAggregator(
            handle,
            mark.getTimestamp(),
            getProcessingTimeService().getCurrentProcessingTime(),
            array.memoryAddress(),
            schema.memoryAddress());
      }
      VectorSchemaRoot out =
          withoutConstantArguments(
              Data.importVectorSchemaRoot(allocator, array, schema, dictionaries));
      if (out.getRowCount() > 0) {
        ColumnarRecordMetrics.emit(output, getMetricGroup(), new ArrowBatch(out));
      } else {
        out.close(); // nothing completed this watermark
      }
    }
    publishStateBytes();
    armProcessingCleanup();
    super.processWatermark(mark);
  }
}
