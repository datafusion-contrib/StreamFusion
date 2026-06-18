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
 * Runtime operator the planner substitutes for a tumbling-window {@code SUM} aggregation. It matches
 * the host engine's window-aggregate contract exactly: input rows carry an event-time column and
 * the summed value, and output rows carry the aggregate total alongside the window bounds, so the
 * surrounding plan is unaffected.
 *
 * <p>The native aggregator works in epoch millis, so the operator reads the event-time column as
 * millis on the way in and re-wraps the window bounds as timestamps on the way out. Window state is
 * held natively and snapshotted into the engine's checkpoints, and windows fire on watermarks.
 */
public class NativeWindowAggregateOperator extends AbstractStreamOperator<RowData>
    implements OneInputStreamOperator<RowData, RowData> {

  private static final String STATE_NAME = "streamfusion-window-aggregate-state";
  private static final int TIMESTAMP_PRECISION = 3;

  private final long windowMillis;
  private final int timeColumn;
  private final int valueColumn;
  private final int keyColumn;
  private final int aggregateKind;
  private final String timeZoneId;
  private final int batchSize;
  private transient ZoneId zone;
  private transient BufferAllocator allocator;
  private transient CDataDictionaryProvider dictionaries;
  private transient List<RowData> buffer;
  private transient long handle;
  private transient ListState<byte[]> windowState;

  public NativeWindowAggregateOperator(
      long windowMillis,
      int timeColumn,
      int valueColumn,
      int keyColumn,
      int aggregateKind,
      String timeZoneId,
      int batchSize) {
    this.windowMillis = windowMillis;
    this.timeColumn = timeColumn;
    this.valueColumn = valueColumn;
    this.keyColumn = keyColumn;
    this.aggregateKind = aggregateKind;
    this.timeZoneId = timeZoneId;
    this.batchSize = batchSize;
  }

  @Override
  public void initializeState(StateInitializationContext context) throws Exception {
    super.initializeState(context);
    windowState =
        context
            .getOperatorStateStore()
            .getListState(
                new ListStateDescriptor<>(
                    STATE_NAME, PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO));
    byte[] snapshot = null;
    for (byte[] entry : windowState.get()) {
      snapshot = entry;
    }
    handle =
        snapshot == null
            ? Native.createTumblingAggregator(windowMillis, aggregateKind)
            : Native.restoreTumblingAggregator(windowMillis, aggregateKind, snapshot);
  }

  @Override
  public void snapshotState(StateSnapshotContext context) throws Exception {
    super.snapshotState(context);
    pushBatch();
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
      pushBatch();
    }
  }

  @Override
  public void processWatermark(Watermark mark) throws Exception {
    pushBatch();
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

  private void pushBatch() {
    if (buffer.isEmpty()) {
      return;
    }
    boolean keyed = keyColumn >= 0;
    BigIntVector ts = new BigIntVector("ts", allocator);
    BigIntVector value = new BigIntVector("value", allocator);
    BigIntVector key = keyed ? new BigIntVector("key", allocator) : null;
    List<FieldVector> vectors = keyed ? List.of(ts, value, key) : List.of(ts, value);
    try (VectorSchemaRoot root = new VectorSchemaRoot(vectors);
        ArrowArray array = ArrowArray.allocateNew(allocator);
        ArrowSchema schema = ArrowSchema.allocateNew(allocator)) {
      ts.allocateNew(buffer.size());
      value.allocateNew(buffer.size());
      if (keyed) {
        key.allocateNew(buffer.size());
      }
      for (int i = 0; i < buffer.size(); i++) {
        RowData row = buffer.get(i);
        ts.set(i, row.getTimestamp(timeColumn, TIMESTAMP_PRECISION).getMillisecond());
        value.set(i, row.getLong(valueColumn));
        if (keyed) {
          key.set(i, row.getLong(keyColumn));
        }
      }
      root.setRowCount(buffer.size());
      Data.exportVectorSchemaRoot(allocator, root, dictionaries, array, schema);
      Native.updateTumblingAggregator(handle, array.memoryAddress(), schema.memoryAddress());
    }
    buffer.clear();
  }

  private LocalDateTime toLocal(long epochMillis) {
    return Instant.ofEpochMilli(epochMillis).atZone(zone).toLocalDateTime();
  }

  private void emitClosedWindows(long watermark) {
    try (ArrowArray array = ArrowArray.allocateNew(allocator);
        ArrowSchema schema = ArrowSchema.allocateNew(allocator)) {
      Native.flushTumblingAggregator(
          handle, watermark, array.memoryAddress(), schema.memoryAddress());
      try (VectorSchemaRoot result =
          Data.importVectorSchemaRoot(allocator, array, schema, dictionaries)) {
        boolean keyed = keyColumn >= 0;
        BigIntVector key = keyed ? (BigIntVector) result.getVector("key") : null;
        BigIntVector windowStart = (BigIntVector) result.getVector("window_start");
        BigIntVector total = (BigIntVector) result.getVector("total");
        for (int i = 0; i < result.getRowCount(); i++) {
          long start = windowStart.get(i);
          // Output column order follows the host: [grouping key, aggregate, window_start,
          // window_end]; the window bounds are local wall-clock timestamps in the session zone.
          TimestampData windowStartTs = TimestampData.fromLocalDateTime(toLocal(start));
          TimestampData windowEndTs = TimestampData.fromLocalDateTime(toLocal(start + windowMillis));
          GenericRowData row;
          if (keyed) {
            row = new GenericRowData(4);
            row.setField(0, key.get(i));
            row.setField(1, total.get(i));
            row.setField(2, windowStartTs);
            row.setField(3, windowEndTs);
          } else {
            row = new GenericRowData(3);
            row.setField(0, total.get(i));
            row.setField(1, windowStartTs);
            row.setField(2, windowEndTs);
          }
          output.collect(new StreamRecord<>(row, start));
        }
      }
    }
  }
}
