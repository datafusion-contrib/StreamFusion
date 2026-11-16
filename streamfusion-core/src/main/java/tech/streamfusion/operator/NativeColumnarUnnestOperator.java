package tech.streamfusion.operator;

import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.CDataDictionaryProvider;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import tech.streamfusion.Native;
import tech.streamfusion.compat.FlinkStreamOperator;

/**
 * Stateless INNER UNNEST of an ARRAY column, columnar in and out: the Arrow-batch analog of Flink's
 * {@code Correlate} over {@code $UNNEST_ROWS$}. Each incoming batch is fanned out natively to one
 * output row per array element — the input columns repeated and the element appended — then
 * forwarded. It does no buffering, so watermarks pass straight through; carrying Arrow lets it
 * chain with the other native operators without a transpose.
 */
public class NativeColumnarUnnestOperator extends FlinkStreamOperator<ArrowBatch>
    implements OneInputStreamOperator<ArrowBatch, ArrowBatch> {

  private final int arrayColumn;
  private final boolean withOrdinality;
  private final boolean isLeft;
  private final boolean isMultiset;
  private final int[] projection;

  private transient BufferAllocator allocator;
  private transient CDataDictionaryProvider dictionaries;

  public NativeColumnarUnnestOperator(
      int arrayColumn, boolean withOrdinality, boolean isLeft, boolean isMultiset) {
    this(arrayColumn, withOrdinality, isLeft, isMultiset, null);
  }

  public NativeColumnarUnnestOperator(
      int arrayColumn,
      boolean withOrdinality,
      boolean isLeft,
      boolean isMultiset,
      int[] projection) {
    this.arrayColumn = arrayColumn;
    this.withOrdinality = withOrdinality;
    this.isLeft = isLeft;
    this.isMultiset = isMultiset;
    this.projection = projection;
  }

  @Override
  public void open() throws Exception {
    super.open();
    NativeAllocator.initializeFor(this);
    allocator = NativeAllocator.SHARED;
    dictionaries = NativeAllocator.DICTIONARIES;
  }

  @Override
  public void processElement(StreamRecord<ArrowBatch> element) {
    ColumnarRecordMetrics.countIngested(getMetricGroup(), element.getValue().rowCount());
    VectorSchemaRoot in = element.getValue().root();
    BufferAllocator inAllocator =
        in.getFieldVectors().isEmpty() ? allocator : in.getFieldVectors().get(0).getAllocator();
    VectorSchemaRoot unnested;
    try (ArrowArray inArray = ArrowArray.allocateNew(inAllocator);
        ArrowSchema inSchema = ArrowSchema.allocateNew(inAllocator);
        ArrowArray outArray = ArrowArray.allocateNew(allocator);
        ArrowSchema outSchema = ArrowSchema.allocateNew(allocator)) {
      Data.exportVectorSchemaRoot(inAllocator, in, dictionaries, inArray, inSchema);
      if (projection == null)
        Native.unnest(
            inArray.memoryAddress(),
            inSchema.memoryAddress(),
            outArray.memoryAddress(),
            outSchema.memoryAddress(),
            arrayColumn,
            withOrdinality,
            isLeft,
            isMultiset);
      else
        Native.unnestProjected(
            inArray.memoryAddress(),
            inSchema.memoryAddress(),
            outArray.memoryAddress(),
            outSchema.memoryAddress(),
            arrayColumn,
            withOrdinality,
            isLeft,
            isMultiset,
            projection);
      unnested = Data.importVectorSchemaRoot(allocator, outArray, outSchema, dictionaries);
    } finally {
      in.close();
    }
    ColumnarRecordMetrics.emit(output, getMetricGroup(), new ArrowBatch(unnested));
  }
}
