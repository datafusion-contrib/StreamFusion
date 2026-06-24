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
 * Append-only streaming Top-N ({@code ROW_NUMBER() OVER (PARTITION BY … ORDER BY …) <= N}, rank
 * number not projected): rows in, a changelog of the top-N set out. Rows are buffered and handed to
 * the native ranker in batches; it keeps the best N per partition and returns the INSERTs/DELETEs as
 * that set changes, with each row's {@link org.apache.flink.types.RowKind} restored. The output
 * carries the input columns unchanged (no rank column). Buffers drain before a checkpoint barrier so
 * the changelog is not replayed on restore, leaving only the bounded buffers to snapshot.
 */
public class NativeTopNOperator extends AbstractStreamOperator<RowData>
    implements OneInputStreamOperator<RowData, RowData>, BoundedOneInput {

  private final int[] partitionColumns;
  private final int[] sortIndices;
  private final int[] sortAscending;
  private final int[] sortNullsFirst;
  private final long limit;
  private final RowType rowType;
  private final int batchSize;

  private transient BufferAllocator allocator;
  private transient CDataDictionaryProvider dictionaries;
  private transient List<RowData> buffer;
  private transient long handle;
  private transient ListState<byte[]> handleState;

  public NativeTopNOperator(
      int[] partitionColumns,
      int[] sortIndices,
      int[] sortAscending,
      int[] sortNullsFirst,
      long limit,
      RowType rowType,
      int batchSize) {
    this.partitionColumns = partitionColumns;
    this.sortIndices = sortIndices;
    this.sortAscending = sortAscending;
    this.sortNullsFirst = sortNullsFirst;
    this.limit = limit;
    this.rowType = rowType;
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
                    "streamfusion-topn-handle",
                    PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO));
    byte[] snapshot = null;
    for (byte[] entry : handleState.get()) {
      snapshot = entry;
    }
    handle =
        snapshot == null
            ? Native.createTopNRanker(
                partitionColumns, sortIndices, sortAscending, sortNullsFirst, limit)
            : Native.restoreTopNRanker(
                partitionColumns, sortIndices, sortAscending, sortNullsFirst, limit, snapshot);
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
    flush();
  }

  @Override
  public void snapshotState(StateSnapshotContext context) throws Exception {
    super.snapshotState(context);
    handleState.clear();
    handleState.add(Native.snapshotTopNRanker(handle));
  }

  @Override
  public void endInput() {
    flush();
  }

  private void flush() {
    if (buffer.isEmpty()) {
      return;
    }
    try (VectorSchemaRoot in = RowDataArrowConverter.write(buffer, rowType, allocator, true);
        ArrowArray inArray = ArrowArray.allocateNew(allocator);
        ArrowSchema inSchema = ArrowSchema.allocateNew(allocator);
        ArrowArray outArray = ArrowArray.allocateNew(allocator);
        ArrowSchema outSchema = ArrowSchema.allocateNew(allocator)) {
      Data.exportVectorSchemaRoot(allocator, in, dictionaries, inArray, inSchema);
      Native.pushTopNRanker(
          handle,
          inArray.memoryAddress(),
          inSchema.memoryAddress(),
          outArray.memoryAddress(),
          outSchema.memoryAddress());
      try (VectorSchemaRoot out =
          Data.importVectorSchemaRoot(allocator, outArray, outSchema, dictionaries)) {
        if (out.getRowCount() > 0) {
          for (RowData row : RowDataArrowConverter.read(out, rowType)) {
            output.collect(new StreamRecord<>(row));
          }
        }
      }
    }
    buffer.clear();
  }

  @Override
  public void close() throws Exception {
    if (handle != 0) {
      Native.closeTopNRanker(handle);
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
