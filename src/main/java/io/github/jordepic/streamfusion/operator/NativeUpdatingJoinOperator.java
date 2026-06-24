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
import org.apache.flink.streaming.api.operators.BoundedMultiInput;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

/**
 * Regular (non-windowed) INNER equi-join over a changelog — the updating join. Each input row's
 * {@link org.apache.flink.types.RowKind} is carried to the native joiner (which keeps a per-side
 * keyed multiset and emits the join changelog against the other side), and restored onto each
 * emitted row.
 *
 * <p>A row must join against the other side's state as of its arrival, so before buffering a row on
 * one side the opposite side's buffer is flushed — applying its pending rows (and emitting their
 * joins) first. This batches consecutive same-side rows while preserving the per-record arrival
 * order the host would see. As with the group aggregate, both buffers are drained before a
 * checkpoint barrier so the changelog is not replayed on restore, leaving only native state to
 * snapshot.
 */
public class NativeUpdatingJoinOperator extends AbstractStreamOperator<RowData>
    implements TwoInputStreamOperator<RowData, RowData, RowData>, BoundedMultiInput {

  private final int[] leftKeys;
  private final int[] rightKeys;
  private final RowType leftRowType;
  private final RowType rightRowType;
  private final RowType outputRowType;
  private final int batchSize;

  private transient BufferAllocator allocator;
  private transient CDataDictionaryProvider dictionaries;
  private transient List<RowData> leftBuffer;
  private transient List<RowData> rightBuffer;
  private transient long handle;
  private transient ListState<byte[]> handleState;

  public NativeUpdatingJoinOperator(
      int[] leftKeys,
      int[] rightKeys,
      RowType leftRowType,
      RowType rightRowType,
      RowType outputRowType,
      int batchSize) {
    this.leftKeys = leftKeys;
    this.rightKeys = rightKeys;
    this.leftRowType = leftRowType;
    this.rightRowType = rightRowType;
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
                    "streamfusion-updating-join-handle",
                    PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO));
    byte[] snapshot = null;
    for (byte[] entry : handleState.get()) {
      snapshot = entry;
    }
    handle =
        snapshot == null
            ? Native.createUpdatingJoiner(leftKeys, rightKeys)
            : Native.restoreUpdatingJoiner(leftKeys, rightKeys, snapshot);
  }

  @Override
  public void open() throws Exception {
    super.open();
    allocator = new RootAllocator();
    dictionaries = new CDataDictionaryProvider();
    leftBuffer = new ArrayList<>(batchSize);
    rightBuffer = new ArrayList<>(batchSize);
  }

  @Override
  public void processElement1(StreamRecord<RowData> element) {
    flushRight(); // apply pending right rows before this left row joins against right state
    leftBuffer.add(element.getValue());
    if (leftBuffer.size() >= batchSize) {
      flushLeft();
    }
  }

  @Override
  public void processElement2(StreamRecord<RowData> element) {
    flushLeft();
    rightBuffer.add(element.getValue());
    if (rightBuffer.size() >= batchSize) {
      flushRight();
    }
  }

  @Override
  public void prepareSnapshotPreBarrier(long checkpointId) {
    flushLeft();
    flushRight();
  }

  @Override
  public void snapshotState(StateSnapshotContext context) throws Exception {
    super.snapshotState(context);
    handleState.clear();
    handleState.add(Native.snapshotUpdatingJoiner(handle));
  }

  @Override
  public void endInput(int inputId) {
    flushLeft();
    flushRight();
  }

  private void flushLeft() {
    flush(leftBuffer, leftRowType, true);
  }

  private void flushRight() {
    flush(rightBuffer, rightRowType, false);
  }

  /** Folds the buffered rows of one side into native state and emits the join changelog produced. */
  private void flush(List<RowData> buffer, RowType rowType, boolean left) {
    if (buffer.isEmpty()) {
      return;
    }
    try (VectorSchemaRoot in = RowDataArrowConverter.write(buffer, rowType, allocator, true);
        ArrowArray inArray = ArrowArray.allocateNew(allocator);
        ArrowSchema inSchema = ArrowSchema.allocateNew(allocator);
        ArrowArray outArray = ArrowArray.allocateNew(allocator);
        ArrowSchema outSchema = ArrowSchema.allocateNew(allocator)) {
      Data.exportVectorSchemaRoot(allocator, in, dictionaries, inArray, inSchema);
      if (left) {
        Native.pushLeftUpdatingJoiner(
            handle, inArray.memoryAddress(), inSchema.memoryAddress(),
            outArray.memoryAddress(), outSchema.memoryAddress());
      } else {
        Native.pushRightUpdatingJoiner(
            handle, inArray.memoryAddress(), inSchema.memoryAddress(),
            outArray.memoryAddress(), outSchema.memoryAddress());
      }
      try (VectorSchemaRoot out =
          Data.importVectorSchemaRoot(allocator, outArray, outSchema, dictionaries)) {
        if (out.getRowCount() > 0) {
          for (RowData row : RowDataArrowConverter.read(out, outputRowType)) {
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
      Native.closeUpdatingJoiner(handle);
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
