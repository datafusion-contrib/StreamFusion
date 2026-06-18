package io.github.jordepic.streamfusion.operator;

import io.github.jordepic.streamfusion.Native;
import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.CDataDictionaryProvider;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
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

/**
 * Hosts the native stateful tumbling-window sum in the engine runtime. Incoming rows ({@code ts},
 * {@code value}) are batched into the native aggregator, which holds open windows across batches. A
 * watermark flushes the windows it has closed, and their per-window totals are emitted downstream
 * before the watermark is forwarded.
 *
 * <p>The native handle owns the window state. Snapshotting that state into the engine's checkpoints
 * for fault tolerance is a separate concern; execution blocks the task thread on native calls for
 * now, as the other native operators do.
 */
public class NativeTumblingWindowOperator extends AbstractStreamOperator<RowData>
    implements OneInputStreamOperator<RowData, RowData> {

  private static final String STATE_NAME = "streamfusion-window-state";

  private final long windowMillis;
  private final int batchSize;
  private transient BufferAllocator allocator;
  private transient CDataDictionaryProvider dictionaries;
  private transient List<RowData> buffer;
  private transient long handle;
  private transient ListState<byte[]> windowState;

  public NativeTumblingWindowOperator(long windowMillis, int batchSize) {
    this.windowMillis = windowMillis;
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

    // The native handle owns the window state, so restore it from the checkpoint when present.
    byte[] snapshot = null;
    for (byte[] entry : windowState.get()) {
      snapshot = entry;
    }
    // This stepping-stone operator only ever sums.
    handle =
        snapshot == null
            ? Native.createTumblingAggregator(windowMillis, new int[] {0})
            : Native.restoreTumblingAggregator(windowMillis, new int[] {0}, snapshot);
  }

  @Override
  public void snapshotState(StateSnapshotContext context) throws Exception {
    super.snapshotState(context);
    // Fold buffered rows in first so the snapshot reflects every record seen so far.
    pushBatch();
    windowState.clear();
    windowState.add(Native.snapshotTumblingAggregator(handle));
  }

  @Override
  public void open() throws Exception {
    super.open();
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
    try (BigIntVector ts = new BigIntVector("ts", allocator);
        BigIntVector value = new BigIntVector("value", allocator);
        ArrowArray array = ArrowArray.allocateNew(allocator);
        ArrowSchema schema = ArrowSchema.allocateNew(allocator)) {
      ts.allocateNew(buffer.size());
      value.allocateNew(buffer.size());
      for (int i = 0; i < buffer.size(); i++) {
        RowData row = buffer.get(i);
        ts.set(i, row.getLong(0));
        value.set(i, row.getLong(1));
      }
      ts.setValueCount(buffer.size());
      value.setValueCount(buffer.size());
      try (VectorSchemaRoot root = new VectorSchemaRoot(List.of(ts, value))) {
        root.setRowCount(buffer.size());
        Data.exportVectorSchemaRoot(allocator, root, dictionaries, array, schema);
      }
      Native.updateTumblingAggregator(handle, array.memoryAddress(), schema.memoryAddress());
    }
    buffer.clear();
  }

  private void emitClosedWindows(long watermark) {
    try (ArrowArray array = ArrowArray.allocateNew(allocator);
        ArrowSchema schema = ArrowSchema.allocateNew(allocator)) {
      Native.flushTumblingAggregator(
          handle, watermark, array.memoryAddress(), schema.memoryAddress());
      try (VectorSchemaRoot result =
          Data.importVectorSchemaRoot(allocator, array, schema, dictionaries)) {
        BigIntVector windowStart = (BigIntVector) result.getVector("window_start");
        BigIntVector total = (BigIntVector) result.getVector("result0");
        for (int i = 0; i < result.getRowCount(); i++) {
          GenericRowData row = new GenericRowData(2);
          row.setField(0, windowStart.get(i));
          row.setField(1, total.get(i));
          output.collect(new StreamRecord<>(row, windowStart.get(i)));
        }
      }
    }
  }
}
