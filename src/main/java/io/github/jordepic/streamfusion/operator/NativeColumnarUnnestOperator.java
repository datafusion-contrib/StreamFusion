package io.github.jordepic.streamfusion.operator;

import io.github.jordepic.streamfusion.Native;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.CDataDictionaryProvider;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

/**
 * Stateless INNER UNNEST of an ARRAY column, columnar in and out: the Arrow-batch analog of Flink's
 * {@code Correlate} over {@code $UNNEST_ROWS$}. Each incoming batch is fanned out natively to one
 * output row per array element — the input columns repeated and the element appended — then forwarded.
 * It does no buffering, so watermarks pass straight through; carrying Arrow lets it chain with the
 * other native operators without a transpose.
 */
public class NativeColumnarUnnestOperator extends AbstractStreamOperator<ArrowBatch>
    implements OneInputStreamOperator<ArrowBatch, ArrowBatch> {

  private final int arrayColumn;

  private transient BufferAllocator allocator;
  private transient CDataDictionaryProvider dictionaries;

  public NativeColumnarUnnestOperator(int arrayColumn) {
    this.arrayColumn = arrayColumn;
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
    VectorSchemaRoot unnested;
    try (ArrowArray inArray = ArrowArray.allocateNew(inAllocator);
        ArrowSchema inSchema = ArrowSchema.allocateNew(inAllocator);
        ArrowArray outArray = ArrowArray.allocateNew(allocator);
        ArrowSchema outSchema = ArrowSchema.allocateNew(allocator)) {
      Data.exportVectorSchemaRoot(inAllocator, in, dictionaries, inArray, inSchema);
      Native.unnest(
          inArray.memoryAddress(),
          inSchema.memoryAddress(),
          outArray.memoryAddress(),
          outSchema.memoryAddress(),
          arrayColumn);
      unnested = Data.importVectorSchemaRoot(allocator, outArray, outSchema, dictionaries);
    } finally {
      in.close();
    }
    output.collect(new StreamRecord<>(new ArrowBatch(unnested)));
  }
}
