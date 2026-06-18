package io.github.jordepic.streamfusion.operator;

import io.github.jordepic.streamfusion.Native;
import io.github.jordepic.streamfusion.bridge.IntColumnBridge;
import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.CDataDictionaryProvider;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.RowData;

/**
 * Hosts a native projection inside the engine's runtime. Incoming rows are buffered into a batch,
 * handed to native code as a column, and the produced rows are emitted downstream. Batching is what
 * makes the native crossing worthwhile: the per-call cost is amortized over many rows rather than
 * paid per record.
 *
 * <p>The batch is flushed when it fills and again when input ends, so no buffered rows are lost.
 * Execution blocks the task thread on the native call for now; integrating with the runtime's
 * non-blocking threading model is a separate concern.
 */
public class NativeProjectionOperator extends AbstractStreamOperator<RowData>
    implements OneInputStreamOperator<RowData, RowData>, BoundedOneInput {

  private final int batchSize;
  private transient BufferAllocator allocator;
  private transient CDataDictionaryProvider dictionaries;
  private transient List<RowData> buffer;

  public NativeProjectionOperator(int batchSize) {
    this.batchSize = batchSize;
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
  public void endInput() {
    flush();
  }

  @Override
  public void close() throws Exception {
    if (dictionaries != null) {
      dictionaries.close();
    }
    if (allocator != null) {
      allocator.close();
    }
    super.close();
  }

  private void flush() {
    if (buffer.isEmpty()) {
      return;
    }
    try (IntVector column = IntColumnBridge.toArrow(buffer, 0, allocator);
        ArrowArray inArray = ArrowArray.allocateNew(allocator);
        ArrowSchema inSchema = ArrowSchema.allocateNew(allocator);
        ArrowArray outArray = ArrowArray.allocateNew(allocator);
        ArrowSchema outSchema = ArrowSchema.allocateNew(allocator)) {

      Data.exportVector(allocator, column, dictionaries, inArray, inSchema);
      Native.doubleColumn(
          inArray.memoryAddress(),
          inSchema.memoryAddress(),
          outArray.memoryAddress(),
          outSchema.memoryAddress());

      try (IntVector result =
          (IntVector) Data.importVector(allocator, outArray, outSchema, dictionaries)) {
        for (RowData row : IntColumnBridge.fromArrow(result)) {
          output.collect(new StreamRecord<>(row));
        }
      }
    }
    buffer.clear();
  }
}
