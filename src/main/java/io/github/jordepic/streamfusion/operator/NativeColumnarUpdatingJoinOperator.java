package io.github.jordepic.streamfusion.operator;

import io.github.jordepic.streamfusion.Native;
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
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

/**
 * Regular (non-windowed) INNER updating join, fed Arrow batches on both inputs and emitting Arrow
 * batches. The changelog flows Arrow with no per-operator transpose; each keyed shuffle stays
 * columnar where the input is a columnar producer, and the row↔Arrow conversion happens only at the
 * host edges. Each input batch is folded into its side and
 * the join changelog it produces is emitted immediately (carrying the changelog kind on the output
 * batch's {@code $row_kind$} column); the other side's state is unaffected by the update, so no
 * cross-input buffering is needed.
 */
public class NativeColumnarUpdatingJoinOperator extends AbstractStreamOperator<ArrowBatch>
    implements TwoInputStreamOperator<ArrowBatch, ArrowBatch, ArrowBatch> {

  private final int[] leftKeys;
  private final int[] rightKeys;

  private transient BufferAllocator allocator;
  private transient CDataDictionaryProvider dictionaries;
  private transient long handle;
  private transient ListState<byte[]> handleState;

  public NativeColumnarUpdatingJoinOperator(int[] leftKeys, int[] rightKeys) {
    this.leftKeys = leftKeys;
    this.rightKeys = rightKeys;
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
  }

  @Override
  public void processElement1(StreamRecord<ArrowBatch> element) {
    join(element.getValue(), true);
  }

  @Override
  public void processElement2(StreamRecord<ArrowBatch> element) {
    join(element.getValue(), false);
  }

  private void join(ArrowBatch batch, boolean left) {
    VectorSchemaRoot in = batch.root();
    BufferAllocator inAllocator =
        in.getFieldVectors().isEmpty() ? allocator : in.getFieldVectors().get(0).getAllocator();
    try (ArrowArray inArray = ArrowArray.allocateNew(inAllocator);
        ArrowSchema inSchema = ArrowSchema.allocateNew(inAllocator);
        ArrowArray outArray = ArrowArray.allocateNew(allocator);
        ArrowSchema outSchema = ArrowSchema.allocateNew(allocator)) {
      Data.exportVectorSchemaRoot(inAllocator, in, dictionaries, inArray, inSchema);
      if (left) {
        Native.pushLeftUpdatingJoiner(
            handle, inArray.memoryAddress(), inSchema.memoryAddress(),
            outArray.memoryAddress(), outSchema.memoryAddress());
      } else {
        Native.pushRightUpdatingJoiner(
            handle, inArray.memoryAddress(), inSchema.memoryAddress(),
            outArray.memoryAddress(), outSchema.memoryAddress());
      }
      VectorSchemaRoot out =
          Data.importVectorSchemaRoot(allocator, outArray, outSchema, dictionaries);
      if (out.getRowCount() > 0) {
        output.collect(new StreamRecord<>(new ArrowBatch(out)));
      } else {
        out.close();
      }
    } finally {
      in.close();
    }
  }

  @Override
  public void snapshotState(StateSnapshotContext context) throws Exception {
    super.snapshotState(context);
    handleState.clear();
    handleState.add(Native.snapshotUpdatingJoiner(handle));
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
