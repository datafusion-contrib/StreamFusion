package io.github.jordepic.streamfusion.operator;

import io.github.jordepic.streamfusion.Native;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.List;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.CDataDictionaryProvider;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TimeStampNanoVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.RowType;

/**
 * Input-agnostic core of the native window operators: it owns the native aggregator handle and its
 * checkpointed state, fires on watermarks, and emits the closed windows — everything except how
 * input arrives. A row-fed subclass ({@link NativeWindowOperatorBase}) buffers {@link RowData}; a
 * columnar subclass feeds Arrow batches directly. Both share the handle lifecycle, state, and emit
 * paths here.
 *
 * <p>Window state lives natively and is snapshotted into operator state; a snapshot first flushes
 * any pending input so it reflects every record seen. The window bounds produced are local
 * wall-clock timestamps in the session zone, matching how the host renders event-time windows.
 */
public abstract class NativeWindowOperatorCore<OUT> extends AbstractStreamOperator<OUT> {

  protected static final int TIMESTAMP_PRECISION = 3;

  /**
   * Value-type codes matching the native side. 0-2 carry every aggregate; the narrow numeric types
   * 4-6 carry only MIN/MAX/COUNT (their SUM/AVG would diverge from the host's narrow-type arithmetic
   * — see docs/aggregate-type-support.md). 3 is a key-only string code (never a value type).
   */
  protected static final int TYPE_BIGINT = 0;
  protected static final int TYPE_DOUBLE = 1;
  protected static final int TYPE_INT = 2;
  protected static final int TYPE_STRING = 3;
  protected static final int TYPE_SMALLINT = 4;
  protected static final int TYPE_TINYINT = 5;
  protected static final int TYPE_FLOAT = 6;

  /** Aggregate kind code for COUNT, whose partial is always a bigint regardless of value type. */
  protected static final int KIND_COUNT = 3;

  private final String stateName;
  private final String timeZoneId;
  protected final long slideMillis;
  protected final int[] aggregateKinds;
  protected final int valueType;
  protected final long windowMillis;

  private transient ZoneId zone;
  private transient ListState<byte[]> windowState;
  protected transient BufferAllocator allocator;
  protected transient CDataDictionaryProvider dictionaries;
  protected transient long handle;

  protected NativeWindowOperatorCore(
      String stateName,
      long windowMillis,
      long slideMillis,
      int valueType,
      int[] aggregateKinds,
      String timeZoneId) {
    this.stateName = stateName;
    this.windowMillis = windowMillis;
    this.slideMillis = slideMillis;
    this.valueType = valueType;
    this.aggregateKinds = aggregateKinds;
    this.timeZoneId = timeZoneId;
  }

  /** Number of aggregates this window computes (and the partial columns it carries). */
  protected final int aggregateCount() {
    return aggregateKinds.length;
  }

  /** Flushes any input buffered by the subclass into the native aggregator. */
  protected abstract void flushPending();

  /** Emits the windows the watermark has closed, fetched from the native aggregator. */
  protected abstract void emitClosedWindows(long watermark);

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
    flushPending();
    windowState.clear();
    windowState.add(snapshotHandle());
  }

  @Override
  public void open() throws Exception {
    super.open();
    zone = ZoneId.of(timeZoneId);
    allocator = new RootAllocator();
    dictionaries = new CDataDictionaryProvider();
  }

  @Override
  public void processWatermark(Watermark mark) throws Exception {
    flushPending();
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
    FieldVector value = newValueVector();
    FieldVector[] keys = new FieldVector[keyColumns.length];
    List<FieldVector> vectors = new java.util.ArrayList<>();
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
        setValue(value, i, row, valueColumn);
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
   * Folds an Arrow batch into the native aggregator, reading the event-time, value, and key columns
   * positionally — the columnar analog of {@link #updateRaw}. The time column arrives as nanosecond
   * timestamps (how the row→Arrow transpose encodes them) and is reduced to the epoch millis the
   * native aggregator expects.
   */
  protected final void updateColumnar(
      VectorSchemaRoot in, int timeColumn, int valueColumn, int[] keyColumns, int[] keyTypes) {
    int rows = in.getRowCount();
    BigIntVector ts = new BigIntVector("ts", allocator);
    FieldVector value = newValueVector();
    FieldVector[] keys = new FieldVector[keyColumns.length];
    List<FieldVector> vectors = new java.util.ArrayList<>();
    vectors.add(ts);
    vectors.add(value);
    for (int j = 0; j < keyColumns.length; j++) {
      keys[j] = newKeyVector("key" + j, keyTypes[j]);
      vectors.add(keys[j]);
    }
    TimeStampNanoVector srcTs = (TimeStampNanoVector) in.getVector(timeColumn);
    FieldVector srcValue = in.getFieldVectors().get(valueColumn);
    try (VectorSchemaRoot root = new VectorSchemaRoot(vectors);
        ArrowArray array = ArrowArray.allocateNew(allocator);
        ArrowSchema schema = ArrowSchema.allocateNew(allocator)) {
      for (int i = 0; i < rows; i++) {
        ts.setSafe(i, srcTs.get(i) / 1_000_000L);
        copyValue(value, i, srcValue);
        for (int j = 0; j < keyColumns.length; j++) {
          setKeyFromVector(keys[j], i, in.getFieldVectors().get(keyColumns[j]), keyTypes[j]);
        }
      }
      root.setRowCount(rows);
      Data.exportVectorSchemaRoot(allocator, root, dictionaries, array, schema);
      updateHandle(array.memoryAddress(), schema.memoryAddress());
    }
  }

  /** Creates the Arrow vector carrying the value column, matching the native value-type code. */
  private FieldVector newValueVector() {
    switch (valueType) {
      case TYPE_DOUBLE:
        return new Float8Vector("value", allocator);
      case TYPE_INT:
        return new IntVector("value", allocator);
      case TYPE_SMALLINT:
        return new SmallIntVector("value", allocator);
      case TYPE_TINYINT:
        return new TinyIntVector("value", allocator);
      case TYPE_FLOAT:
        return new Float4Vector("value", allocator);
      default:
        return new BigIntVector("value", allocator);
    }
  }

  /** Copies row {@code i}'s value from the input row into the value vector. */
  private void setValue(FieldVector value, int i, RowData row, int column) {
    switch (valueType) {
      case TYPE_DOUBLE:
        ((Float8Vector) value).setSafe(i, row.getDouble(column));
        break;
      case TYPE_INT:
        ((IntVector) value).setSafe(i, row.getInt(column));
        break;
      case TYPE_SMALLINT:
        ((SmallIntVector) value).setSafe(i, row.getShort(column));
        break;
      case TYPE_TINYINT:
        ((TinyIntVector) value).setSafe(i, row.getByte(column));
        break;
      case TYPE_FLOAT:
        ((Float4Vector) value).setSafe(i, row.getFloat(column));
        break;
      default:
        ((BigIntVector) value).setSafe(i, row.getLong(column));
    }
  }

  /** Copies row {@code i}'s value from a source Arrow vector into the value vector. */
  private void copyValue(FieldVector value, int i, FieldVector source) {
    switch (valueType) {
      case TYPE_DOUBLE:
        ((Float8Vector) value).setSafe(i, ((Float8Vector) source).get(i));
        break;
      case TYPE_INT:
        ((IntVector) value).setSafe(i, ((IntVector) source).get(i));
        break;
      case TYPE_SMALLINT:
        ((SmallIntVector) value).setSafe(i, ((SmallIntVector) source).get(i));
        break;
      case TYPE_TINYINT:
        ((TinyIntVector) value).setSafe(i, ((TinyIntVector) source).get(i));
        break;
      case TYPE_FLOAT:
        ((Float4Vector) value).setSafe(i, ((Float4Vector) source).get(i));
        break;
      default:
        ((BigIntVector) value).setSafe(i, ((BigIntVector) source).get(i));
    }
  }

  /** Copies row {@code i}'s key from a source Arrow vector into the key vector (int widens to int64). */
  private static void setKeyFromVector(FieldVector target, int i, FieldVector source, int keyType) {
    if (keyType == TYPE_STRING) {
      ((VarCharVector) target).setSafe(i, ((VarCharVector) source).get(i));
    } else {
      long value = keyType == TYPE_INT ? ((IntVector) source).get(i) : ((BigIntVector) source).get(i);
      ((BigIntVector) target).setSafe(i, value);
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

  /** Reads a result/partial cell, boxing by the vector's type so RowData gets the column's type. */
  protected static Object readScalar(FieldVector vector, int row) {
    if (vector instanceof Float8Vector) {
      return ((Float8Vector) vector).get(row);
    }
    if (vector instanceof Float4Vector) {
      return ((Float4Vector) vector).get(row);
    }
    if (vector instanceof IntVector) {
      return ((IntVector) vector).get(row);
    }
    if (vector instanceof SmallIntVector) {
      return ((SmallIntVector) vector).get(row);
    }
    if (vector instanceof TinyIntVector) {
      return ((TinyIntVector) vector).get(row);
    }
    return ((BigIntVector) vector).get(row);
  }
}
