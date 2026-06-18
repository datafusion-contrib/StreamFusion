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

  private final long windowMillis;
  private final int batchSize;
  private transient BufferAllocator allocator;
  private transient CDataDictionaryProvider dictionaries;
  private transient List<RowData> buffer;
  private transient long handle;

  public NativeTumblingWindowOperator(long windowMillis, int batchSize) {
    this.windowMillis = windowMillis;
    this.batchSize = batchSize;
  }

  @Override
  public void open() throws Exception {
    super.open();
    allocator = new RootAllocator();
    dictionaries = new CDataDictionaryProvider();
    buffer = new ArrayList<>(batchSize);
    handle = Native.createTumblingAggregator(windowMillis);
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
        BigIntVector total = (BigIntVector) result.getVector("total");
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
