package io.github.jordepic.streamfusion.operator;

import io.github.jordepic.streamfusion.Native;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.CDataDictionaryProvider;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
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

/**
 * Columnar append-only keep-first deduplication (`ROW_NUMBER() OVER (PARTITION BY … ORDER BY rowtime
 * ASC) = 1`): Arrow in, Arrow out. The Arrow-batch analog of the host's insert-only {@code
 * RowTimeDeduplicateKeepFirstRowFunction}. Each input batch is buffered natively; on a watermark the
 * deduplicator emits each key's minimum-rowtime row once the watermark reaches that rowtime, and
 * drops every later row for the key. Insert-only — the watermark guarantees no smaller-rowtime row
 * can still arrive once a key's row fires. Keys are co-located by the columnar shuffle; the per-key
 * candidate state and the late-data drop live in the native deduplicator, and this layer moves
 * batches across the bridge and owns the handle's checkpointed state.
 */
public class NativeColumnarDeduplicateOperator extends AbstractStreamOperator<ArrowBatch>
    implements OneInputStreamOperator<ArrowBatch, ArrowBatch> {

  private final int[] partitionColumns;
  private final int rowtimeColumn;

  private transient BufferAllocator allocator;
  private transient CDataDictionaryProvider dictionaries;
  private transient long handle;
  private transient ListState<byte[]> handleState;

  public NativeColumnarDeduplicateOperator(int[] partitionColumns, int rowtimeColumn) {
    this.partitionColumns = partitionColumns;
    this.rowtimeColumn = rowtimeColumn;
  }

  @Override
  public void initializeState(StateInitializationContext context) throws Exception {
    super.initializeState(context);
    handleState =
        context
            .getOperatorStateStore()
            .getListState(
                new ListStateDescriptor<>(
                    "streamfusion-deduplicate-handle",
                    PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO));
    byte[] snapshot = null;
    for (byte[] entry : handleState.get()) {
      snapshot = entry;
    }
    handle =
        snapshot == null
            ? Native.createKeepFirstDeduplicator(partitionColumns, rowtimeColumn)
            : Native.restoreKeepFirstDeduplicator(partitionColumns, rowtimeColumn, snapshot);
  }

  @Override
  public void open() throws Exception {
    super.open();
    allocator = NativeAllocator.SHARED;
    dictionaries = NativeAllocator.DICTIONARIES;
  }

  @Override
  public void processElement(StreamRecord<ArrowBatch> element) {
    VectorSchemaRoot in = element.getValue().root();
    BufferAllocator inAllocator =
        in.getFieldVectors().isEmpty() ? allocator : in.getFieldVectors().get(0).getAllocator();
    try (ArrowArray array = ArrowArray.allocateNew(inAllocator);
        ArrowSchema schema = ArrowSchema.allocateNew(inAllocator)) {
      Data.exportVectorSchemaRoot(inAllocator, in, dictionaries, array, schema);
      Native.pushKeepFirstDeduplicator(handle, array.memoryAddress(), schema.memoryAddress());
    } finally {
      in.close();
    }
  }

  @Override
  public void processWatermark(Watermark mark) throws Exception {
    try (ArrowArray array = ArrowArray.allocateNew(allocator);
        ArrowSchema schema = ArrowSchema.allocateNew(allocator)) {
      Native.flushKeepFirstDeduplicator(
          handle, mark.getTimestamp(), array.memoryAddress(), schema.memoryAddress());
      VectorSchemaRoot out = Data.importVectorSchemaRoot(allocator, array, schema, dictionaries);
      if (out.getRowCount() > 0) {
        output.collect(new StreamRecord<>(new ArrowBatch(out)));
      } else {
        out.close(); // no key's first row was completed by this watermark
      }
    }
    super.processWatermark(mark);
  }

  @Override
  public void snapshotState(StateSnapshotContext context) throws Exception {
    super.snapshotState(context);
    handleState.clear();
    handleState.add(Native.snapshotKeepFirstDeduplicator(handle));
  }

  @Override
  public void close() throws Exception {
    if (handle != 0) {
      Native.closeKeepFirstDeduplicator(handle);
      handle = 0;
    }
    super.close();
  }
}
