package io.github.jordepic.streamfusion.operator;

import io.github.jordepic.streamfusion.Native;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.List;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.CDataDictionaryProvider;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import java.math.BigDecimal;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DecimalVector;
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
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks;

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

  /** Key-only type codes (carried in their natural Arrow type; the native key path is type-general). */
  protected static final int TYPE_BOOLEAN = 7;
  protected static final int TYPE_DATE = 8;

  // Parameterized type codes pack precision/scale into the code so they thread through the existing
  // int[] without parallel arrays. Timestamp keys ride as int64 nanoseconds (lossless for any Flink
  // precision); decimal keys/values ride in an Arrow decimal vector of the given precision/scale.
  protected static final int TYPE_TIMESTAMP_BASE = 1000; // + precision (0..9)
  protected static final int TYPE_DECIMAL_BASE = 2000; // + precision * 100 + scale

  public static int decimalCode(int precision, int scale) {
    return TYPE_DECIMAL_BASE + precision * 100 + scale;
  }

  private static boolean isTimestamp(int code) {
    return code >= TYPE_TIMESTAMP_BASE && code < TYPE_DECIMAL_BASE;
  }

  private static int timestampPrecision(int code) {
    return code - TYPE_TIMESTAMP_BASE;
  }

  private static boolean isDecimal(int code) {
    return code >= TYPE_DECIMAL_BASE;
  }

  private static int decimalPrecision(int code) {
    return (code - TYPE_DECIMAL_BASE) / 100;
  }

  private static int decimalScale(int code) {
    return (code - TYPE_DECIMAL_BASE) % 100;
  }

  private static final long NANOS_PER_MILLI = 1_000_000L;

  /** Aggregate kind code for COUNT, whose partial is always a bigint regardless of value type. */
  protected static final int KIND_COUNT = 3;

  private final String stateName;
  private final String timeZoneId;
  protected final long slideMillis;
  protected final int[] aggregateKinds;
  // One value-column type per aggregate (positionally matching aggregateKinds), so a window can
  // aggregate over different value columns.
  protected final int[] valueTypes;
  protected final long windowMillis;

  private transient ZoneId zone;
  private transient ListState<byte[]> windowState;
  protected transient BufferAllocator allocator;
  protected transient CDataDictionaryProvider dictionaries;
  protected transient long handle;
  private transient ManagedMemoryBudget memoryBudget;

  protected NativeWindowOperatorCore(
      String stateName,
      long windowMillis,
      long slideMillis,
      int[] valueTypes,
      int[] aggregateKinds,
      String timeZoneId) {
    this.stateName = stateName;
    this.windowMillis = windowMillis;
    this.slideMillis = slideMillis;
    this.valueTypes = valueTypes;
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
    return Native.createTumblingAggregator(
        windowMillis, slideMillis, valueTypes, aggregateKinds, memoryBudgetBytes());
  }

  /** Restores a native aggregator handle from a checkpoint snapshot. */
  protected long restoreHandle(byte[] snapshot) {
    return Native.restoreTumblingAggregator(
        windowMillis, slideMillis, valueTypes, aggregateKinds, snapshot, memoryBudgetBytes());
  }

  /**
   * The managed-memory budget bounding the native state (see {@link ManagedMemoryBudget}), for this
   * class's handle creation and any subclass override's.
   */
  protected final long memoryBudgetBytes() {
    return memoryBudget == null ? ManagedMemoryBudget.UNBOUNDED : memoryBudget.bytes();
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

  /** The native aggregator's tracked state footprint in bytes (zero when unaccounted). */
  protected long stateBytesHandle() {
    return Native.tumblingAggregatorStateBytes(handle);
  }

  /**
   * Samples the native state size and publishes it to the operator's gauges; call after batches and
   * flushes on the task thread (the handle is not thread-safe). No-op without a budget — the native
   * side only tracks its footprint when accounted.
   */
  protected final void publishStateBytes() {
    if (memoryBudget.bounded()) {
      memoryBudget.publishStateBytes(stateBytesHandle());
    }
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
    memoryBudget = ManagedMemoryBudget.reserveFor(this);
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
    allocator = NativeAllocator.SHARED;
    dictionaries = NativeAllocator.DICTIONARIES;
  }

  @Override
  public void processWatermark(Watermark mark) throws Exception {
    flushPending();
    emitClosedWindows(mark.getTimestamp());
    publishStateBytes();
    super.processWatermark(mark);
  }

  @Override
  public void close() throws Exception {
    if (handle != 0) {
      closeHandle();
      handle = 0;
    }
    if (memoryBudget != null) {
      memoryBudget.close();
      memoryBudget = null;
    }
    super.close();
  }

  /** Window start (epoch millis) rendered as a session-zone local timestamp, as the host does. */
  protected final LocalDateTime toLocal(long epochMillis) {
    return Instant.ofEpochMilli(epochMillis).atZone(zone).toLocalDateTime();
  }

  /**
   * Inverse of the window-boundary local rendering (see {@code fillLocalTimestamps}): an upstream window
   * operator emits its boundaries as the session-zone wall-clock stored as if UTC, so a window-attached
   * re-aggregation must undo that shift back to epoch millis before folding — otherwise its own boundary
   * rendering would double-shift them (the boundaries would then miss an equi-join on the window bounds).
   */
  protected final long toEpochFromLocal(long localMillis) {
    return Instant.ofEpochMilli(localMillis)
        .atZone(ZoneOffset.UTC)
        .toLocalDateTime()
        .atZone(zone)
        .toInstant()
        .toEpochMilli();
  }

  /**
   * Folds raw rows (event-time column, value, optional key) into the native aggregator. Shared by
   * the single-phase and local operators, which both consume raw input.
   */
  protected final void updateRaw(
      List<RowData> rows, int timeColumn, int[] valueColumns, int[] keyColumns, int[] keyTypes) {
    BigIntVector ts = new BigIntVector("ts", allocator);
    FieldVector[] values = new FieldVector[valueColumns.length];
    FieldVector[] keys = new FieldVector[keyColumns.length];
    List<FieldVector> vectors = new java.util.ArrayList<>();
    vectors.add(ts);
    for (int a = 0; a < valueColumns.length; a++) {
      values[a] = newValueVector("value" + a, valueTypes[a]);
      vectors.add(values[a]);
    }
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
        for (int a = 0; a < valueColumns.length; a++) {
          if (valueColumns[a] < 0) {
            ((BigIntVector) values[a]).setSafe(i, 1L); // COUNT(*): a non-null constant counts rows
          } else {
            setValue(values[a], i, row, valueColumns[a], valueTypes[a]);
          }
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
   * Folds an Arrow batch into the native aggregator, reading the event-time, value, and key columns
   * positionally — the columnar analog of {@link #updateRaw}. The time column arrives as nanosecond
   * timestamps (how the row→Arrow transpose encodes them) and is reduced to the epoch millis the
   * native aggregator expects.
   */
  protected final void updateColumnar(
      VectorSchemaRoot in, int timeColumn, int[] valueColumns, int[] keyColumns, int[] keyTypes) {
    updateColumnarInternal(in, timeColumn, 0L, valueColumns, keyColumns, keyTypes);
  }

  /**
   * Proctime variant of {@link #updateColumnar}: every row's window-assignment time is the operator's
   * current processing time {@code nowMillis} (read once per batch by the caller), not a row column —
   * matching Flink's processing-time window assigner, which uses the clock rather than a row value.
   */
  protected final void updateColumnarProctime(
      VectorSchemaRoot in, long nowMillis, int[] valueColumns, int[] keyColumns, int[] keyTypes) {
    updateColumnarInternal(in, -1, nowMillis, valueColumns, keyColumns, keyTypes);
  }

  /**
   * Window-attached variant of {@link #updateColumnar}: the input rows already carry their window as
   * {@code windowStartColumn}/{@code windowEndColumn} (nanosecond timestamps, as the transpose encodes
   * them) rather than a rowtime to slice — an upstream window aggregate's output re-aggregated per
   * window (Nexmark q5). Emits an Arrow batch with the canonical {@code window_start}/{@code window_end}
   * (epoch millis), {@code value{i}}, and {@code key{j}} columns the native window-attached fold reads.
   */
  protected final void updateColumnarAttached(
      VectorSchemaRoot in,
      int windowStartColumn,
      int windowEndColumn,
      int[] valueColumns,
      int[] keyColumns,
      int[] keyTypes) {
    int rows = in.getRowCount();
    BigIntVector windowStart = new BigIntVector("window_start", allocator);
    BigIntVector windowEnd = new BigIntVector("window_end", allocator);
    FieldVector[] values = new FieldVector[valueColumns.length];
    FieldVector[] srcValues = new FieldVector[valueColumns.length];
    FieldVector[] keys = new FieldVector[keyColumns.length];
    List<FieldVector> vectors = new java.util.ArrayList<>();
    vectors.add(windowStart);
    vectors.add(windowEnd);
    for (int a = 0; a < valueColumns.length; a++) {
      values[a] = newValueVector("value" + a, valueTypes[a]);
      srcValues[a] = valueColumns[a] < 0 ? null : in.getFieldVectors().get(valueColumns[a]);
      vectors.add(values[a]);
    }
    for (int j = 0; j < keyColumns.length; j++) {
      keys[j] = newKeyVector("key" + j, keyTypes[j]);
      vectors.add(keys[j]);
    }
    TimeStampNanoVector srcStart = (TimeStampNanoVector) in.getVector(windowStartColumn);
    TimeStampNanoVector srcEnd = (TimeStampNanoVector) in.getVector(windowEndColumn);
    try (VectorSchemaRoot root = new VectorSchemaRoot(vectors);
        ArrowArray array = ArrowArray.allocateNew(allocator);
        ArrowSchema schema = ArrowSchema.allocateNew(allocator)) {
      for (int i = 0; i < rows; i++) {
        // The upstream boundaries are session-zone wall-clock (see toEpochFromLocal); fold on epoch
        // millis so this aggregate's own boundary rendering shifts them exactly once, not twice.
        windowStart.setSafe(i, toEpochFromLocal(srcStart.get(i) / 1_000_000L));
        windowEnd.setSafe(i, toEpochFromLocal(srcEnd.get(i) / 1_000_000L));
        for (int a = 0; a < valueColumns.length; a++) {
          if (valueColumns[a] < 0) {
            ((BigIntVector) values[a]).setSafe(i, 1L); // COUNT(*): a non-null constant counts rows
          } else {
            copyValue(values[a], i, srcValues[a], valueTypes[a]);
          }
        }
        for (int j = 0; j < keyColumns.length; j++) {
          setKeyFromVector(keys[j], i, in.getFieldVectors().get(keyColumns[j]), keyTypes[j]);
        }
      }
      root.setRowCount(rows);
      Data.exportVectorSchemaRoot(allocator, root, dictionaries, array, schema);
      Native.updateAttachedTumblingAggregator(handle, array.memoryAddress(), schema.memoryAddress());
    }
  }

  private void updateColumnarInternal(
      VectorSchemaRoot in,
      int timeColumn,
      long proctimeMillis,
      int[] valueColumns,
      int[] keyColumns,
      int[] keyTypes) {
    boolean proctime = timeColumn < 0;
    int rows = in.getRowCount();
    BigIntVector ts = new BigIntVector("ts", allocator);
    FieldVector[] values = new FieldVector[valueColumns.length];
    FieldVector[] srcValues = new FieldVector[valueColumns.length];
    FieldVector[] keys = new FieldVector[keyColumns.length];
    List<FieldVector> vectors = new java.util.ArrayList<>();
    vectors.add(ts);
    for (int a = 0; a < valueColumns.length; a++) {
      values[a] = newValueVector("value" + a, valueTypes[a]);
      srcValues[a] = valueColumns[a] < 0 ? null : in.getFieldVectors().get(valueColumns[a]);
      vectors.add(values[a]);
    }
    for (int j = 0; j < keyColumns.length; j++) {
      keys[j] = newKeyVector("key" + j, keyTypes[j]);
      vectors.add(keys[j]);
    }
    TimeStampNanoVector srcTs = proctime ? null : (TimeStampNanoVector) in.getVector(timeColumn);
    try (VectorSchemaRoot root = new VectorSchemaRoot(vectors);
        ArrowArray array = ArrowArray.allocateNew(allocator);
        ArrowSchema schema = ArrowSchema.allocateNew(allocator)) {
      for (int i = 0; i < rows; i++) {
        ts.setSafe(i, proctime ? proctimeMillis : srcTs.get(i) / 1_000_000L);
        for (int a = 0; a < valueColumns.length; a++) {
          if (valueColumns[a] < 0) {
            ((BigIntVector) values[a]).setSafe(i, 1L); // COUNT(*): a non-null constant counts rows
          } else {
            copyValue(values[a], i, srcValues[a], valueTypes[a]);
          }
        }
        for (int j = 0; j < keyColumns.length; j++) {
          setKeyFromVector(keys[j], i, in.getFieldVectors().get(keyColumns[j]), keyTypes[j]);
        }
      }
      root.setRowCount(rows);
      Data.exportVectorSchemaRoot(allocator, root, dictionaries, array, schema);
      updateHandle(array.memoryAddress(), schema.memoryAddress());
    }
  }

  /** Creates the Arrow vector carrying a value column, matching the native value-type code. */
  private FieldVector newValueVector(String name, int valueType) {
    if (isDecimal(valueType)) {
      return new DecimalVector(name, allocator, decimalPrecision(valueType), decimalScale(valueType));
    }
    switch (valueType) {
      case TYPE_DOUBLE:
        return new Float8Vector(name, allocator);
      case TYPE_INT:
        return new IntVector(name, allocator);
      case TYPE_SMALLINT:
        return new SmallIntVector(name, allocator);
      case TYPE_TINYINT:
        return new TinyIntVector(name, allocator);
      case TYPE_FLOAT:
        return new Float4Vector(name, allocator);
      default:
        return new BigIntVector(name, allocator);
    }
  }

  /** Copies row {@code i}'s value from the input row into the value vector. */
  private void setValue(FieldVector value, int i, RowData row, int column, int valueType) {
    if (isDecimal(valueType)) {
      ((DecimalVector) value)
          .setSafe(i, row.getDecimal(column, decimalPrecision(valueType), decimalScale(valueType)).toBigDecimal());
      return;
    }
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
  private void copyValue(FieldVector value, int i, FieldVector source, int valueType) {
    if (isDecimal(valueType)) {
      ((DecimalVector) value).setSafe(i, ((DecimalVector) source).getObject(i));
      return;
    }
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
    if (isTimestamp(keyType)) {
      // Columnar timestamps arrive as nanosecond timestamps; carry the nanos as int64.
      ((BigIntVector) target).setSafe(i, ((TimeStampNanoVector) source).get(i));
      return;
    }
    if (isDecimal(keyType)) {
      ((DecimalVector) target).setSafe(i, ((DecimalVector) source).getObject(i));
      return;
    }
    switch (keyType) {
      case TYPE_STRING:
        ((VarCharVector) target).setSafe(i, ((VarCharVector) source).get(i));
        break;
      case TYPE_BOOLEAN:
        ((BitVector) target).setSafe(i, ((BitVector) source).get(i));
        break;
      case TYPE_DATE:
        ((DateDayVector) target).setSafe(i, ((DateDayVector) source).get(i));
        break;
      default:
        long value =
            keyType == TYPE_INT ? ((IntVector) source).get(i) : ((BigIntVector) source).get(i);
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
      LogicalType type = outputType.getTypeAt(j);
      switch (type.getTypeRoot()) {
        case INTEGER:
          types[j] = TYPE_INT;
          break;
        case VARCHAR:
        case CHAR:
          types[j] = TYPE_STRING;
          break;
        case BOOLEAN:
          types[j] = TYPE_BOOLEAN;
          break;
        case DATE:
          types[j] = TYPE_DATE;
          break;
        case TIMESTAMP_WITHOUT_TIME_ZONE:
        case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
          types[j] = TYPE_TIMESTAMP_BASE + LogicalTypeChecks.getPrecision(type);
          break;
        case DECIMAL:
          types[j] = decimalCode(LogicalTypeChecks.getPrecision(type), LogicalTypeChecks.getScale(type));
          break;
        default:
          types[j] = TYPE_BIGINT;
      }
    }
    return types;
  }

  /** Creates the Arrow vector carrying a key column, in the key's natural type (int/bigint widen). */
  protected final FieldVector newKeyVector(String name, int keyType) {
    if (isTimestamp(keyType)) {
      return new BigIntVector(name, allocator); // carried as int64 nanoseconds
    }
    if (isDecimal(keyType)) {
      return new DecimalVector(name, allocator, decimalPrecision(keyType), decimalScale(keyType));
    }
    switch (keyType) {
      case TYPE_STRING:
        return new VarCharVector(name, allocator);
      case TYPE_BOOLEAN:
        return new BitVector(name, allocator);
      case TYPE_DATE:
        return new DateDayVector(name, allocator);
      default:
        return new BigIntVector(name, allocator);
    }
  }

  /** Copies row {@code i}'s key from the input row into the key vector (int keys widen to int64). */
  protected static void setKey(FieldVector vector, int i, RowData row, int column, int keyType) {
    if (isTimestamp(keyType)) {
      TimestampData t = row.getTimestamp(column, timestampPrecision(keyType));
      ((BigIntVector) vector).setSafe(i, t.getMillisecond() * NANOS_PER_MILLI + t.getNanoOfMillisecond());
      return;
    }
    if (isDecimal(keyType)) {
      ((DecimalVector) vector)
          .setSafe(i, row.getDecimal(column, decimalPrecision(keyType), decimalScale(keyType)).toBigDecimal());
      return;
    }
    switch (keyType) {
      case TYPE_STRING:
        ((VarCharVector) vector).setSafe(i, row.getString(column).toBytes());
        break;
      case TYPE_BOOLEAN:
        ((BitVector) vector).setSafe(i, row.getBoolean(column) ? 1 : 0);
        break;
      case TYPE_DATE:
        // Flink stores DATE as the epoch-day int; carry it in a Date32 (day) vector.
        ((DateDayVector) vector).setSafe(i, row.getInt(column));
        break;
      default:
        ((BigIntVector) vector)
            .setSafe(i, keyType == TYPE_INT ? row.getInt(column) : row.getLong(column));
    }
  }

  /** Boxes a native-produced key cell back to the emitted column's internal type. */
  protected static Object boxKey(FieldVector vector, int i, int keyType) {
    if (isTimestamp(keyType)) {
      long nanos = ((BigIntVector) vector).get(i);
      return TimestampData.fromEpochMillis(
          Math.floorDiv(nanos, NANOS_PER_MILLI), (int) Math.floorMod(nanos, NANOS_PER_MILLI));
    }
    if (isDecimal(keyType)) {
      return DecimalData.fromBigDecimal(
          ((DecimalVector) vector).getObject(i), decimalPrecision(keyType), decimalScale(keyType));
    }
    switch (keyType) {
      case TYPE_STRING:
        return StringData.fromBytes(((VarCharVector) vector).get(i));
      case TYPE_BOOLEAN:
        return ((BitVector) vector).get(i) != 0;
      case TYPE_DATE:
        return ((DateDayVector) vector).get(i);
      default:
        long value = ((BigIntVector) vector).get(i);
        if (keyType == TYPE_INT) {
          return (int) value; // separate return: a ternary would promote both arms to long
        }
        return value;
    }
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
    if (vector instanceof DecimalVector) {
      DecimalVector decimal = (DecimalVector) vector;
      return DecimalData.fromBigDecimal(
          decimal.getObject(row), decimal.getPrecision(), decimal.getScale());
    }
    return ((BigIntVector) vector).get(row);
  }
}
