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
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

/**
 * Non-windowed {@code GROUP BY} aggregation: rows in, a changelog of rows out. Input rows are
 * buffered and handed to the native aggregator in batches; each batch returns the changelog rows it
 * produces — an INSERT for a key's first appearance, then UPDATE_BEFORE/UPDATE_AFTER as the result
 * changes — with each row's {@link org.apache.flink.types.RowKind} carried back on the batch's
 * hidden kind column (see {@link RowDataArrowConverter}) and restored onto the emitted row.
 *
 * <p>The per-key running state and the changelog discipline live natively; this layer moves whole
 * rows across the bridge and owns the checkpointed handle. Because emitting a changelog during a
 * checkpoint would place records after the barrier (and so replay them on restore), the buffer is
 * drained before the barrier in {@link #prepareSnapshotPreBarrier}, leaving the snapshot to persist
 * only native state.
 */
public class NativeGroupAggregateOperator extends AbstractStreamOperator<RowData>
    implements OneInputStreamOperator<RowData, RowData>, BoundedOneInput {

  private final int[] aggregateKinds;
  private final int[] valueTypes;
  private final int[] valueColumns;
  private final int[] keyColumns;
  private final boolean generateUpdateBefore;
  private final RowType inputRowType;
  private final RowType outputRowType;
  private final int batchSize;

  private transient BufferAllocator allocator;
  private transient CDataDictionaryProvider dictionaries;
  private transient List<RowData> buffer;
  private transient long handle;
  private transient ListState<byte[]> handleState;

  public NativeGroupAggregateOperator(
      int[] aggregateKinds,
      int[] valueTypes,
      int[] valueColumns,
      int[] keyColumns,
      boolean generateUpdateBefore,
      RowType inputRowType,
      RowType outputRowType,
      int batchSize) {
    this.aggregateKinds = aggregateKinds;
    this.valueTypes = valueTypes;
    this.valueColumns = valueColumns;
    this.keyColumns = keyColumns;
    this.generateUpdateBefore = generateUpdateBefore;
    this.inputRowType = inputRowType;
    this.outputRowType = outputRowType;
    this.batchSize = batchSize;
  }

  @Override
  public void initializeState(StateInitializationContext context) throws Exception {
    super.initializeState(context);
    handleState =
        context
            .getOperatorStateStore()
            .getListState(
                new ListStateDescriptor<>(
                    "streamfusion-group-handle",
                    PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO));
    byte[] snapshot = null;
    for (byte[] entry : handleState.get()) {
      snapshot = entry;
    }
    handle =
        snapshot == null
            ? Native.createGroupAggregator(
                aggregateKinds, valueTypes, valueColumns, keyColumns, generateUpdateBefore)
            : Native.restoreGroupAggregator(
                aggregateKinds, valueTypes, valueColumns, keyColumns, generateUpdateBefore, snapshot);
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
      flush();
    }
  }

  @Override
  public void prepareSnapshotPreBarrier(long checkpointId) {
    // Emit any buffered changelog now, before the barrier, so it is not replayed after restore.
    flush();
  }

  @Override
  public void snapshotState(StateSnapshotContext context) throws Exception {
    super.snapshotState(context);
    handleState.clear();
    handleState.add(Native.snapshotGroupAggregator(handle));
  }

  @Override
  public void endInput() {
    flush();
  }

  /** Folds the buffered rows into native state and emits the changelog they produce. */
  private void flush() {
    if (buffer.isEmpty()) {
      return;
    }
    try (VectorSchemaRoot in = RowDataArrowConverter.write(buffer, inputRowType, allocator);
        ArrowArray inArray = ArrowArray.allocateNew(allocator);
        ArrowSchema inSchema = ArrowSchema.allocateNew(allocator);
        ArrowArray outArray = ArrowArray.allocateNew(allocator);
        ArrowSchema outSchema = ArrowSchema.allocateNew(allocator)) {
      Data.exportVectorSchemaRoot(allocator, in, dictionaries, inArray, inSchema);
      Native.updateGroupAggregator(
          handle,
          inArray.memoryAddress(),
          inSchema.memoryAddress(),
          outArray.memoryAddress(),
          outSchema.memoryAddress());
      try (VectorSchemaRoot out =
          Data.importVectorSchemaRoot(allocator, outArray, outSchema, dictionaries)) {
        for (RowData row : RowDataArrowConverter.read(out, outputRowType)) {
          output.collect(new StreamRecord<>(row));
        }
      }
    }
    buffer.clear();
  }

  @Override
  public void close() throws Exception {
    if (handle != 0) {
      Native.closeGroupAggregator(handle);
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
}
