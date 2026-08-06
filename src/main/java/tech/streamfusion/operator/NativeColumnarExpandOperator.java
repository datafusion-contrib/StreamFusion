package tech.streamfusion.operator;

import tech.streamfusion.Native;
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
 * Stateless GROUPING SETS / CUBE / ROLLUP expansion, columnar in and out: the Arrow-batch analog of
 * Flink's generated {@code ExpandFunction}. Each incoming batch is fanned out natively to one output
 * row per grouping set — copying the grouped-in columns, nulling the grouped-out ones, and stamping
 * the per-set expand id — then forwarded to the downstream native GROUP BY (over the keys plus the
 * expand-id column). It does no buffering, so it forwards watermarks unchanged (the default {@link
 * AbstractStreamOperator} behavior); carrying Arrow lets it chain with the native aggregate without a
 * transpose.
 */
public class NativeColumnarExpandOperator extends AbstractStreamOperator<ArrowBatch>
    implements OneInputStreamOperator<ArrowBatch, ArrowBatch> {

  private final int numExpandRows;
  private final int numOutCols;
  private final int expandIdIndex;
  private final boolean expandIdIsLong;
  private final int[] copyIndices;
  private final long[] expandIdValues;

  private transient BufferAllocator allocator;
  private transient CDataDictionaryProvider dictionaries;

  public NativeColumnarExpandOperator(
      int numExpandRows,
      int numOutCols,
      int expandIdIndex,
      boolean expandIdIsLong,
      int[] copyIndices,
      long[] expandIdValues) {
    this.numExpandRows = numExpandRows;
    this.numOutCols = numOutCols;
    this.expandIdIndex = expandIdIndex;
    this.expandIdIsLong = expandIdIsLong;
    this.copyIndices = copyIndices;
    this.expandIdValues = expandIdValues;
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
    // The input batch's buffers belong to the upstream operator's allocator; the C Data export must
    // use that same allocator. The operator's own allocator owns only the imported result.
    BufferAllocator inAllocator =
        in.getFieldVectors().isEmpty() ? allocator : in.getFieldVectors().get(0).getAllocator();
    VectorSchemaRoot expanded;
    try (ArrowArray inArray = ArrowArray.allocateNew(inAllocator);
        ArrowSchema inSchema = ArrowSchema.allocateNew(inAllocator);
        ArrowArray outArray = ArrowArray.allocateNew(allocator);
        ArrowSchema outSchema = ArrowSchema.allocateNew(allocator)) {
      Data.exportVectorSchemaRoot(inAllocator, in, dictionaries, inArray, inSchema);
      Native.expand(
          inArray.memoryAddress(),
          inSchema.memoryAddress(),
          outArray.memoryAddress(),
          outSchema.memoryAddress(),
          numExpandRows,
          numOutCols,
          expandIdIndex,
          expandIdIsLong,
          copyIndices,
          expandIdValues);
      expanded = Data.importVectorSchemaRoot(allocator, outArray, outSchema, dictionaries);
    } finally {
      in.close(); // the input batch is consumed
    }
    ColumnarRecordMetrics.emit(output, getMetricGroup(), new ArrowBatch(expanded));
  }
}
