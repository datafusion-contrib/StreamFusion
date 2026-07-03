package io.github.jordepic.streamfusion.operator;

import io.github.jordepic.streamfusion.Native;
import java.util.List;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.types.logical.RowType;

/**
 * The "shallow" ingest path's decode core: turns a stream of raw JSON message bodies (one {@code byte[]}
 * per record, as Flink's Kafka/other connectors deliver them) into typed Arrow batches, batched and
 * decoded natively by the shared {@code JsonDecoder}.
 *
 * <p>This is the alternative to a fully-native source: keep Flink's battle-tested connector for the
 * consume (offsets, checkpointing, auth) and only accelerate the decode — bytes are buffered into an
 * Arrow binary column and decoded to {@code outputType} in one native call per batch, replacing Flink's
 * per-record {@code byte[] -> tree -> RowData} materialization. Reusable by any source that emits raw
 * value bytes. Stateless across batches; flushes the partial batch at end of input.
 */
public class NativeJsonBytesDecodeOperator extends AbstractStreamOperator<ArrowBatch>
    implements OneInputStreamOperator<byte[], ArrowBatch>, BoundedOneInput {

  private final RowType outputType;
  private final int batchSize;

  private transient BufferAllocator allocator;
  private transient long handle;
  private transient VarBinaryVector body;
  private transient int count;

  public NativeJsonBytesDecodeOperator(RowType outputType, int batchSize) {
    this.outputType = outputType;
    this.batchSize = batchSize;
  }

  @Override
  public void open() throws Exception {
    super.open();
    allocator = NativeAllocator.SHARED;
    try (VectorSchemaRoot template = RowDataArrowConverter.write(List.of(), outputType, allocator);
        ArrowArray array = ArrowArray.allocateNew(allocator);
        ArrowSchema schema = ArrowSchema.allocateNew(allocator)) {
      Data.exportVectorSchemaRoot(allocator, template, NativeAllocator.DICTIONARIES, array, schema);
      handle = Native.createDecoder(0, array.memoryAddress(), schema.memoryAddress(), "", "", 0, false);
    }
    newBody();
  }

  private void newBody() {
    body = new VarBinaryVector("body", allocator);
    body.allocateNew(batchSize);
    count = 0;
  }

  @Override
  public void processElement(StreamRecord<byte[]> element) {
    body.setSafe(count++, element.getValue());
    if (count >= batchSize) {
      flush();
    }
  }

  @Override
  public void endInput() {
    if (count > 0) {
      flush();
    }
  }

  private void flush() {
    body.setValueCount(count);
    try (VectorSchemaRoot in = new VectorSchemaRoot(List.of(body));
        ArrowArray inArray = ArrowArray.allocateNew(allocator);
        ArrowSchema inSchema = ArrowSchema.allocateNew(allocator);
        ArrowArray outArray = ArrowArray.allocateNew(allocator);
        ArrowSchema outSchema = ArrowSchema.allocateNew(allocator)) {
      in.setRowCount(count);
      Data.exportVectorSchemaRoot(allocator, in, NativeAllocator.DICTIONARIES, inArray, inSchema);
      Native.decodeInto(
          handle,
          inArray.memoryAddress(),
          inSchema.memoryAddress(),
          outArray.memoryAddress(),
          outSchema.memoryAddress());
      VectorSchemaRoot out =
          Data.importVectorSchemaRoot(allocator, outArray, outSchema, NativeAllocator.DICTIONARIES);
      output.collect(new StreamRecord<>(new ArrowBatch(out)));
    }
    body.close();
    newBody();
  }

  @Override
  public void close() throws Exception {
    if (handle != 0) {
      Native.closeDecoder(handle);
      handle = 0;
    }
    if (body != null) {
      body.close();
    }
    super.close();
  }
}
