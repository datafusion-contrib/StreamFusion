package tech.streamfusion.operator;

import java.util.HashMap;
import java.util.Map;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.CDataDictionaryProvider;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.functions.FunctionContext;
import tech.streamfusion.Native;
import tech.streamfusion.compat.FlinkStreamOperator;

/**
 * Stateless native Calc, columnar in and out: applies an encoded Calc — an optional condition then
 * the projection expressions — to each incoming Arrow batch natively, emitting the projected output
 * batch. The general form of the filter (it subsumes filter-plus-column-subset and handles computed
 * columns and constants). The Calc is compiled once into a native handle reused across batches;
 * carrying Arrow lets it chain with other native operators without converting to rows.
 */
public class NativeCalcOperator extends FlinkStreamOperator<ArrowBatch>
    implements OneInputStreamOperator<ArrowBatch, ArrowBatch> {

  private final int[] kinds;
  private final int[] payload;
  private final int[] childCounts;
  private final long[] longs;
  private final double[] doubles;
  private final String[] strings;
  private final int[] projectionRoots;
  private final int conditionRoot;
  private final String[] outputNames;
  private final NativeUdf.Binding udfBinding;

  private transient BufferAllocator allocator;
  private transient CDataDictionaryProvider dictionaries;
  private transient Map<Schema, Long> compiledSchemas;
  private transient long[] boundLongs;
  private transient long watermark;

  public NativeCalcOperator(
      int[] kinds,
      int[] payload,
      int[] childCounts,
      long[] longs,
      double[] doubles,
      String[] strings,
      int[] projectionRoots,
      int conditionRoot,
      String[] outputNames,
      NativeUdf.Binding udfBinding) {
    this.kinds = kinds;
    this.payload = payload;
    this.childCounts = childCounts;
    this.longs = longs;
    this.doubles = doubles;
    this.strings = strings;
    this.projectionRoots = projectionRoots;
    this.conditionRoot = conditionRoot;
    this.outputNames = outputNames;
    this.udfBinding = udfBinding;
  }

  @Override
  public void open() throws Exception {
    super.open();
    watermark = Long.MIN_VALUE;
    NativeAllocator.initializeFor(this);
    allocator = NativeAllocator.SHARED;
    dictionaries = NativeAllocator.DICTIONARIES;
    // Register any UDFs this Calc references into this JVM's registry (empty on a task manager) and
    // patch their ids into the encoded pool before compiling — so distributed tasks resolve them.
    boundLongs = udfBinding.bind(longs, new FunctionContext(getRuntimeContext()));
    compiledSchemas = new HashMap<>();
  }

  @Override
  public void close() throws Exception {
    if (compiledSchemas != null) {
      compiledSchemas.values().forEach(Native::closeCalcExpression);
      compiledSchemas.clear();
    }
    udfBinding.unbind();
    super.close();
  }

  @Override
  public void processWatermark(org.apache.flink.streaming.api.watermark.Watermark mark)
      throws Exception {
    watermark = Math.max(watermark, mark.getTimestamp());
    super.processWatermark(mark);
  }

  @Override
  public void processElement(StreamRecord<ArrowBatch> element) {
    ColumnarRecordMetrics.countIngested(getMetricGroup(), element.getValue().rowCount());
    // A Calc can sit immediately after a columnar key-group exchange (for example, the planner
    // appends OVER-frame constants there). The projection does not change the batch's routing, so
    // retain the key group used by the downstream keyed operator's Flink state-key context.
    int keyGroup = element.getValue().keyGroup();
    VectorSchemaRoot in = element.getValue().root();
    // The input batch's buffers belong to the upstream operator's allocator; the C Data export must
    // use that allocator (buffers associate only within one allocator root). The operator's own
    // allocator owns only the imported result.
    BufferAllocator inAllocator =
        in.getFieldVectors().isEmpty() ? allocator : in.getFieldVectors().get(0).getAllocator();
    VectorSchemaRoot out;
    try (ArrowArray inArray = ArrowArray.allocateNew(inAllocator);
        ArrowArray outArray = ArrowArray.allocateNew(allocator);
        ArrowSchema outSchema = ArrowSchema.allocateNew(allocator)) {
      try {
        // UNION ALL can mix insert-only batches with batches carrying the hidden row-kind
        // column. Each native handle caches its own immutable Arrow schema.
        Long cached = compiledSchemas.get(in.getSchema());
        long calc;
        if (cached != null) {
          calc = cached;
          Data.exportVectorSchemaRoot(inAllocator, in, dictionaries, inArray);
          Native.calcExpressionArrayAtWatermark(
              calc,
              inArray.memoryAddress(),
              outArray.memoryAddress(),
              outSchema.memoryAddress(),
              watermark);
        } else {
          calc =
              Native.createCalcExpression(
                  kinds,
                  payload,
                  childCounts,
                  boundLongs,
                  doubles,
                  strings,
                  projectionRoots,
                  conditionRoot,
                  outputNames);
          compiledSchemas.put(in.getSchema(), calc);
          try (ArrowSchema inSchema = ArrowSchema.allocateNew(inAllocator)) {
            Data.exportVectorSchemaRoot(inAllocator, in, dictionaries, inArray, inSchema);
            Native.calcExpressionAtWatermark(
                calc,
                inArray.memoryAddress(),
                inSchema.memoryAddress(),
                outArray.memoryAddress(),
                outSchema.memoryAddress(),
                watermark);
          }
        }
      } catch (tech.streamfusion.NativeException e) {
        throw NativeUdf.propagateUpcallFailure(e);
      }
      out = Data.importVectorSchemaRoot(allocator, outArray, outSchema, dictionaries);
    } finally {
      in.close(); // the input batch is consumed
    }
    ColumnarRecordMetrics.emit(output, getMetricGroup(), new ArrowBatch(out, keyGroup));
  }
}
