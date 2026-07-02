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
 * Columnar event-time sort (`ORDER BY rowtime`): Arrow in, Arrow out. The Arrow-batch analog of the
 * host's {@code RowTimeSortOperator}. Each input batch is buffered natively; on a watermark the
 * native sorter emits the rows it has completed (rowtime at or before the watermark) in ascending
 * rowtime order and keeps the rest. Insert-only — the watermark guarantees no earlier-rowtime row
 * can still arrive, so the emitted order is final. The buffering, the sort, and the watermark-driven
 * release all live in the native sorter; this layer moves batches across the bridge and owns the
 * handle's checkpointed state.
 */
public class NativeColumnarTemporalSortOperator extends AbstractStreamOperator<ArrowBatch>
    implements OneInputStreamOperator<ArrowBatch, ArrowBatch> {

  private final int rowtimeColumn;

  private transient BufferAllocator allocator;
  private transient CDataDictionaryProvider dictionaries;
  private transient long handle;
  private transient ListState<byte[]> handleState;
  private transient ManagedMemoryBudget memoryBudget;

  public NativeColumnarTemporalSortOperator(int rowtimeColumn) {
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
                    "streamfusion-temporal-sort-handle",
                    PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO));
    byte[] snapshot = null;
    for (byte[] entry : handleState.get()) {
      snapshot = entry;
    }
    memoryBudget = ManagedMemoryBudget.reserveFor(this);
    handle =
        snapshot == null
            ? Native.createTemporalSorter(rowtimeColumn, memoryBudget.bytes())
            : Native.restoreTemporalSorter(rowtimeColumn, snapshot, memoryBudget.bytes());
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
      // The native sorter imports and keeps the batch (it buffers until a watermark releases the
      // rows), so this side hands it off and closes its own view.
      Native.pushTemporalSorter(handle, array.memoryAddress(), schema.memoryAddress());
    } finally {
      in.close();
    }
    publishStateBytes();
  }

  @Override
  public void processWatermark(Watermark mark) throws Exception {
    try (ArrowArray array = ArrowArray.allocateNew(allocator);
        ArrowSchema schema = ArrowSchema.allocateNew(allocator)) {
      Native.flushTemporalSorter(
          handle, mark.getTimestamp(), array.memoryAddress(), schema.memoryAddress());
      VectorSchemaRoot out = Data.importVectorSchemaRoot(allocator, array, schema, dictionaries);
      if (out.getRowCount() > 0) {
        output.collect(new StreamRecord<>(new ArrowBatch(out)));
      } else {
        out.close(); // nothing completed this watermark
      }
    }
    publishStateBytes();
    super.processWatermark(mark);
  }

  /** Samples the native state size for the operator's gauges; task-thread only. */
  private void publishStateBytes() {
    if (memoryBudget.bounded()) {
      memoryBudget.publishStateBytes(Native.temporalSorterStateBytes(handle));
    }
  }

  @Override
  public void snapshotState(StateSnapshotContext context) throws Exception {
    super.snapshotState(context);
    handleState.clear();
    handleState.add(Native.snapshotTemporalSorter(handle));
  }

  @Override
  public void close() throws Exception {
    if (handle != 0) {
      Native.closeTemporalSorter(handle);
      handle = 0;
    }
    if (memoryBudget != null) {
      memoryBudget.close();
      memoryBudget = null;
    }
    super.close();
  }
}
