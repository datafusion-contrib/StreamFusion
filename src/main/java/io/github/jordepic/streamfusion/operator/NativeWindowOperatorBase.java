package io.github.jordepic.streamfusion.operator;

import io.github.jordepic.streamfusion.Native;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.CDataDictionaryProvider;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;

/**
 * Shared scaffolding for the native tumbling-window operators. It owns the native aggregator handle
 * and its checkpointed state, buffers incoming rows into batches, and fires on watermarks. Each
 * concrete operator only decides how a buffered batch is fed to native code and how the windows a
 * watermark closes are emitted.
 *
 * <p>Window state lives natively and is snapshotted into operator state; a snapshot first flushes
 * buffered rows so it reflects every record seen. The window bounds it produces are local
 * wall-clock timestamps in the session zone, matching how the host renders event-time windows.
 */
public abstract class NativeWindowOperatorBase extends AbstractStreamOperator<RowData>
    implements OneInputStreamOperator<RowData, RowData> {

  protected static final int TIMESTAMP_PRECISION = 3;

  /** Value-type codes matching the native side. */
  protected static final int TYPE_BIGINT = 0;
  protected static final int TYPE_DOUBLE = 1;
  protected static final int TYPE_INT = 2;
  protected static final int TYPE_STRING = 3;

  /** Aggregate kind code for COUNT, whose partial is always a bigint regardless of value type. */
  protected static final int KIND_COUNT = 3;

  private final String stateName;
  private final String timeZoneId;
  private final int batchSize;
  protected final long slideMillis;
  protected final int[] aggregateKinds;
  protected final int valueType;
  protected final long windowMillis;

  private transient ZoneId zone;
  private transient List<RowData> buffer;
  private transient ListState<byte[]> windowState;
  protected transient BufferAllocator allocator;
  protected transient CDataDictionaryProvider dictionaries;
  protected transient long handle;

  protected NativeWindowOperatorBase(
      String stateName,
      long windowMillis,
      long slideMillis,
      int valueType,
      int[] aggregateKinds,
      String timeZoneId,
      int batchSize) {
    this.stateName = stateName;
    this.windowMillis = windowMillis;
    this.slideMillis = slideMillis;
    this.valueType = valueType;
    this.aggregateKinds = aggregateKinds;
    this.timeZoneId = timeZoneId;
    this.batchSize = batchSize;
  }

  /** Number of aggregates this window computes (and the partial columns it carries). */
  protected final int aggregateCount() {
    return aggregateKinds.length;
  }

  /** Feeds the buffered rows to the native aggregator. Called when the batch fills or fires. */
  protected abstract void pushBatch(List<RowData> rows);

  /** Emits the windows the watermark has closed, fetched from the native aggregator. */
  protected abstract void emitClosedWindows(long watermark);

  // The native handle's lifecycle is delegated so operators can back themselves with a different
  // native aggregator (tumbling vs session) while sharing all the buffering, state, and emit logic.

  /** Creates a fresh native aggregator handle. */
  protected long createHandle() {
    return Native.createTumblingAggregator(windowMillis, slideMillis, valueType, aggregateKinds);
  }

  /** Restores a native aggregator handle from a checkpoint snapshot. */
  protected long restoreHandle(byte[] snapshot) {
    return Native.restoreTumblingAggregator(
        windowMillis, slideMillis, valueType, aggregateKinds, snapshot);
  }

  /** Folds an exported batch into the native aggregator. */
  protected void updateHandle(long arrayAddress, long schemaAddress) {
    Native.updateTumblingAggregator(handle, arrayAddress, schemaAddress);
  }

  /** Fetches the windows the watermark has closed from the native aggregator. */
  protected void flushHandle(long watermark, long arrayAddress, long schemaAddress) {
    Native.flushTumblingAggregator(handle, watermark, arrayAddress, schemaAddress);
  }

  /** Serializes the native aggregator's open state for a checkpoint. */
  protected byte[] snapshotHandle() {
    return Native.snapshotTumblingAggregator(handle);
  }

  /** Releases the native aggregator handle. */
  protected void closeHandle() {
    Native.closeTumblingAggregator(handle);
  }

  @Override
  public void initializeState(StateInitializationContext context) throws Exception {
    super.initializeState(context);
    windowState =
        context
            .getOperatorStateStore()
            .getListState(
                new ListStateDescriptor<>(
                    stateName, PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO));
    byte[] snapshot = null;
    for (byte[] entry : windowState.get()) {
      snapshot = entry;
    }
    handle = snapshot == null ? createHandle() : restoreHandle(snapshot);
  }

  @Override
  public void snapshotState(StateSnapshotContext context) throws Exception {
    super.snapshotState(context);
    flush();
    windowState.clear();
    windowState.add(snapshotHandle());
  }

  @Override
  public void open() throws Exception {
    super.open();
    zone = ZoneId.of(timeZoneId);
    allocator = new RootAllocator();
    dictionaries = new CDataDictionaryProvider();
    buffer = new ArrayList<>(batchSize);
  }

  @Override
  public void processElement(StreamRecord<RowData> element) {
    buffer.add(element.getValue());
    if (buffer.size() >= batchSize) {
      flush();
    }
  }

  @Override
  public void processWatermark(Watermark mark) throws Exception {
    flush();
    emitClosedWindows(mark.getTimestamp());
    super.processWatermark(mark);
  }

  @Override
  public void close() throws Exception {
    if (handle != 0) {
      closeHandle();
      handle = 0;
    }
    if (dictionaries != null) {
      dictionaries.close();
    }
    if (allocator != null) {
      allocator.close();
    }
    super.close();
  }

  /** Window start (epoch millis) rendered as a session-zone local timestamp, as the host does. */
  protected final LocalDateTime toLocal(long epochMillis) {
    return Instant.ofEpochMilli(epochMillis).atZone(zone).toLocalDateTime();
  }

  /**
   * Folds raw rows (event-time column, value, optional key) into the native aggregator. Shared by
   * the single-phase and local operators, which both consume raw input.
   */
  protected final void updateRaw(
      List<RowData> rows, int timeColumn, int valueColumn, int[] keyColumns, int[] keyTypes) {
    BigIntVector ts = new BigIntVector("ts", allocator);
    FieldVector value;
    if (valueType == TYPE_DOUBLE) {
      value = new Float8Vector("value", allocator);
    } else if (valueType == TYPE_INT) {
      value = new IntVector("value", allocator);
    } else {
      value = new BigIntVector("value", allocator);
    }
    FieldVector[] keys = new FieldVector[keyColumns.length];
    List<FieldVector> vectors = new ArrayList<>();
    vectors.add(ts);
    vectors.add(value);
    for (int j = 0; j < keyColumns.length; j++) {
      keys[j] = newKeyVector("key" + j, keyTypes[j]);
      vectors.add(keys[j]);
    }
    try (VectorSchemaRoot root = new VectorSchemaRoot(vectors);
        ArrowArray array = ArrowArray.allocateNew(allocator);
        ArrowSchema schema = ArrowSchema.allocateNew(allocator)) {
      for (int i = 0; i < rows.size(); i++) {
        RowData row = rows.get(i);
        ts.setSafe(i, row.getTimestamp(timeColumn, TIMESTAMP_PRECISION).getMillisecond());
        if (valueType == TYPE_DOUBLE) {
          ((Float8Vector) value).setSafe(i, row.getDouble(valueColumn));
        } else if (valueType == TYPE_INT) {
          ((IntVector) value).setSafe(i, row.getInt(valueColumn));
        } else {
          ((BigIntVector) value).setSafe(i, row.getLong(valueColumn));
        }
        for (int j = 0; j < keyColumns.length; j++) {
          setKey(keys[j], i, row, keyColumns[j], keyTypes[j]);
        }
      }
      root.setRowCount(rows.size());
      Data.exportVectorSchemaRoot(allocator, root, dictionaries, array, schema);
      updateHandle(array.memoryAddress(), schema.memoryAddress());
    }
  }

  /**
   * The grouping-key type codes for an aggregate's first {@code keyCount} output columns (the keys
   * lead the row in the host's order). Bigint and int keys are both carried as int64 natively; the
   * code records the type to read on input and emit on output.
   */
  public static int[] keyTypes(RowType outputType, int keyCount) {
    int[] types = new int[keyCount];
    for (int j = 0; j < keyCount; j++) {
      switch (outputType.getTypeAt(j).getTypeRoot()) {
        case INTEGER:
          types[j] = TYPE_INT;
          break;
        case VARCHAR:
        case CHAR:
          types[j] = TYPE_STRING;
          break;
        default:
          types[j] = TYPE_BIGINT;
      }
    }
    return types;
  }

  /** Creates the Arrow vector carrying a key column: varchar for a string key, else int64. */
  protected final FieldVector newKeyVector(String name, int keyType) {
    return keyType == TYPE_STRING
        ? new VarCharVector(name, allocator)
        : new BigIntVector(name, allocator);
  }

  /** Copies row {@code i}'s key from the input row into the key vector (int keys widen to int64). */
  protected static void setKey(FieldVector vector, int i, RowData row, int column, int keyType) {
    if (keyType == TYPE_STRING) {
      ((VarCharVector) vector).setSafe(i, row.getString(column).toBytes());
    } else {
      ((BigIntVector) vector).setSafe(i, keyType == TYPE_INT ? row.getInt(column) : row.getLong(column));
    }
  }

  /** Boxes a native-produced key cell back to the emitted column's type. */
  protected static Object boxKey(FieldVector vector, int i, int keyType) {
    if (keyType == TYPE_STRING) {
      return StringData.fromBytes(((VarCharVector) vector).get(i));
    }
    long value = ((BigIntVector) vector).get(i);
    if (keyType == TYPE_INT) {
      return (int) value;
    }
    return value;
  }

  /** Reads a result/partial cell, boxing by the column's type so RowData gets Long or Double. */
  protected static Object readScalar(FieldVector vector, int row) {
    if (vector instanceof Float8Vector) {
      return ((Float8Vector) vector).get(row);
    }
    if (vector instanceof IntVector) {
      return ((IntVector) vector).get(row);
    }
    return ((BigIntVector) vector).get(row);
  }

  /**
   * Emits the final per-window rows the watermark has closed:
   * {@code [key?, agg0..aggN-1, window_start, window_end]} — the host's column order. The flush
   * carries the window end explicitly (windows of the same start can differ by it), so every window
   * operator — single-phase, global, and session — shares this path.
   */
  protected final void emitFinal(long watermark, int[] keyTypes) {
    int keyCount = keyTypes.length;
    int aggregates = aggregateCount();
    try (ArrowArray array = ArrowArray.allocateNew(allocator);
        ArrowSchema schema = ArrowSchema.allocateNew(allocator)) {
      flushHandle(watermark, array.memoryAddress(), schema.memoryAddress());
      try (VectorSchemaRoot result =
          Data.importVectorSchemaRoot(allocator, array, schema, dictionaries)) {
        FieldVector[] keys = new FieldVector[keyCount];
        for (int j = 0; j < keyCount; j++) {
          keys[j] = (FieldVector) result.getVector("key" + j);
        }
        BigIntVector windowStart = (BigIntVector) result.getVector("window_start");
        BigIntVector windowEnd = (BigIntVector) result.getVector("window_end");
        FieldVector[] results = new FieldVector[aggregates];
        for (int a = 0; a < aggregates; a++) {
          results[a] = (FieldVector) result.getVector("result" + a);
        }
        for (int i = 0; i < result.getRowCount(); i++) {
          long start = windowStart.get(i);
          long end = windowEnd.get(i);
          GenericRowData row = new GenericRowData(keyCount + aggregates + 2);
          int field = 0;
          for (int j = 0; j < keyCount; j++) {
            row.setField(field++, boxKey(keys[j], i, keyTypes[j]));
          }
          for (int a = 0; a < aggregates; a++) {
            row.setField(field++, readScalar(results[a], i));
          }
          row.setField(field++, TimestampData.fromLocalDateTime(toLocal(start)));
          row.setField(field, TimestampData.fromLocalDateTime(toLocal(end)));
          output.collect(new StreamRecord<>(row, start));
        }
      }
    }
  }

  private void flush() {
    if (buffer.isEmpty()) {
      return;
    }
    pushBatch(buffer);
    buffer.clear();
  }
}
