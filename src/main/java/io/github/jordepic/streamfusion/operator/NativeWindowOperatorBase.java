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
import org.apache.flink.table.data.TimestampData;

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

  private final String stateName;
  private final String timeZoneId;
  private final int batchSize;
  private final int[] aggregateKinds;
  private final long slideMillis;
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
      int[] aggregateKinds,
      String timeZoneId,
      int batchSize) {
    this.stateName = stateName;
    this.windowMillis = windowMillis;
    this.slideMillis = slideMillis;
    this.aggregateKinds = aggregateKinds;
    this.timeZoneId = timeZoneId;
    this.batchSize = batchSize;
  }

  /** Number of aggregates this window computes. */
  protected final int aggregateCount() {
    return aggregateKinds.length;
  }

  /** Feeds the buffered rows to the native aggregator. Called when the batch fills or fires. */
  protected abstract void pushBatch(List<RowData> rows);

  /** Emits the windows the watermark has closed, fetched from the native aggregator. */
  protected abstract void emitClosedWindows(long watermark);

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
    handle =
        snapshot == null
            ? Native.createTumblingAggregator(windowMillis, slideMillis, aggregateKinds)
            : Native.restoreTumblingAggregator(windowMillis, slideMillis, aggregateKinds, snapshot);
  }

  @Override
  public void snapshotState(StateSnapshotContext context) throws Exception {
    super.snapshotState(context);
    flush();
    windowState.clear();
    windowState.add(Native.snapshotTumblingAggregator(handle));
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
      Native.closeTumblingAggregator(handle);
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
      List<RowData> rows, int timeColumn, int valueColumn, int keyColumn) {
    boolean keyed = keyColumn >= 0;
    BigIntVector ts = new BigIntVector("ts", allocator);
    BigIntVector value = new BigIntVector("value", allocator);
    BigIntVector key = keyed ? new BigIntVector("key", allocator) : null;
    List<FieldVector> vectors = keyed ? List.of(ts, value, key) : List.of(ts, value);
    try (VectorSchemaRoot root = new VectorSchemaRoot(vectors);
        ArrowArray array = ArrowArray.allocateNew(allocator);
        ArrowSchema schema = ArrowSchema.allocateNew(allocator)) {
      ts.allocateNew(rows.size());
      value.allocateNew(rows.size());
      if (keyed) {
        key.allocateNew(rows.size());
      }
      for (int i = 0; i < rows.size(); i++) {
        RowData row = rows.get(i);
        ts.set(i, row.getTimestamp(timeColumn, TIMESTAMP_PRECISION).getMillisecond());
        value.set(i, row.getLong(valueColumn));
        if (keyed) {
          key.set(i, row.getLong(keyColumn));
        }
      }
      root.setRowCount(rows.size());
      Data.exportVectorSchemaRoot(allocator, root, dictionaries, array, schema);
      Native.updateTumblingAggregator(handle, array.memoryAddress(), schema.memoryAddress());
    }
  }

  /**
   * Emits the final per-window rows the watermark has closed:
   * {@code [key?, agg0..aggN-1, window_start, window_end]} — the host's column order. Shared by the
   * single-phase and global operators, which both produce finals.
   */
  protected final void emitFinal(long watermark, int keyColumn) {
    boolean keyed = keyColumn >= 0;
    int aggregates = aggregateCount();
    try (ArrowArray array = ArrowArray.allocateNew(allocator);
        ArrowSchema schema = ArrowSchema.allocateNew(allocator)) {
      Native.flushTumblingAggregator(
          handle, watermark, array.memoryAddress(), schema.memoryAddress());
      try (VectorSchemaRoot result =
          Data.importVectorSchemaRoot(allocator, array, schema, dictionaries)) {
        BigIntVector key = keyed ? (BigIntVector) result.getVector("key") : null;
        BigIntVector windowStart = (BigIntVector) result.getVector("window_start");
        BigIntVector[] results = new BigIntVector[aggregates];
        for (int a = 0; a < aggregates; a++) {
          results[a] = (BigIntVector) result.getVector("result" + a);
        }
        for (int i = 0; i < result.getRowCount(); i++) {
          long start = windowStart.get(i);
          GenericRowData row = new GenericRowData((keyed ? 1 : 0) + aggregates + 2);
          int field = 0;
          if (keyed) {
            row.setField(field++, key.get(i));
          }
          for (int a = 0; a < aggregates; a++) {
            row.setField(field++, results[a].get(i));
          }
          row.setField(field++, TimestampData.fromLocalDateTime(toLocal(start)));
          row.setField(field, TimestampData.fromLocalDateTime(toLocal(start + windowMillis)));
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
