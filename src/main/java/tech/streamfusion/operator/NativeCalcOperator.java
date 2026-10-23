package tech.streamfusion.operator;

import java.util.HashMap;
import java.util.Map;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.CDataDictionaryProvider;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;
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
  private transient Map<Schema, CompiledCalc> compiledSchemas;
  private transient long[] boundLongs;
  private transient long watermark;
  private transient int directRowCalc;
  private transient int directPrimitiveCalc;
  private transient int[] directColumns;
  private transient org.apache.flink.metrics.Counter directRows;
  private transient org.apache.flink.metrics.Counter directPrimitiveRows;
  private transient org.apache.flink.metrics.Counter aliasedValues;

  private static final class CompiledCalc {
    final long handle;
    Schema outputSchema;

    CompiledCalc(long handle) {
      this.handle = handle;
    }
  }

  private static boolean hasDictionary(Field field) {
    return field.getDictionary() != null
        || field.getChildren().stream().anyMatch(NativeCalcOperator::hasDictionary);
  }

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
    boundLongs =
        udfBinding.bind(
            longs, new FunctionContext(getRuntimeContext()), getProcessingTimeService());
    compiledSchemas = new HashMap<>();
    directRowCalc = -1;
    directPrimitiveCalc = -1;
    directRows = getMetricGroup().counter("directTemporalRows");
    directPrimitiveRows = getMetricGroup().counter("directPrimitiveCalcRows");
    aliasedValues = getMetricGroup().counter("directTemporalAliasedValues");
    directColumns = udfBinding.rowInputColumns();
    if (directColumns != null) {
      int id = udfBinding.rowCalcId();
      if (NativeUdf.canEvaluatePrimitiveRowCalc(id)) directPrimitiveCalc = id;
    }
    if (Boolean.parseBoolean(
            System.getProperty("streamfusion.experimental.directTemporalCalc", "true"))
        && directColumns != null) {
      int id = udfBinding.rowCalcId();
      if (NativeUdf.canEvaluateTemporalRowCalc(id)) directRowCalc = id;
    }
  }

  @Override
  public void close() throws Exception {
    if (compiledSchemas != null) {
      compiledSchemas.values().forEach(calc -> Native.closeCalcExpression(calc.handle));
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
    // A complete JVM Calc has no Rust expressions left to execute. Read its borrowed input and
    // write flat Arrow results here, avoiding the C Data round trip and nullable struct envelope.
    int directCalc = directPrimitiveCalc >= 0 ? directPrimitiveCalc : directRowCalc;
    if (directCalc >= 0 && in.getVector(RowDataArrowConverter.ROW_KIND_COLUMN) == null) {
      VectorSchemaRoot result;
      try {
        result = NativeUdf.evaluateRowCalc(directCalc, in, directColumns);
        if (directPrimitiveCalc >= 0) directPrimitiveRows.inc(in.getRowCount());
        else directRows.inc(in.getRowCount());
        for (int column = 0;
            column < Math.min(result.getFieldVectors().size(), directColumns.length);
            column++) {
          var source = in.getVector(directColumns[column]);
          var target = result.getVector(column);
          if (source instanceof org.apache.arrow.vector.complex.StructVector
              && target instanceof org.apache.arrow.vector.complex.StructVector
              && tech.streamfusion.arrow.TimestampAccessor.isComponentTimestamp(target.getField())
              && ((org.apache.arrow.vector.complex.StructVector) source)
                      .getChild("millis")
                      .getDataBufferAddress()
                  == ((org.apache.arrow.vector.complex.StructVector) target)
                      .getChild("millis")
                      .getDataBufferAddress()) aliasedValues.inc(in.getRowCount());
        }
      } finally {
        in.close();
      }
      ColumnarRecordMetrics.emit(output, getMetricGroup(), new ArrowBatch(result, keyGroup));
      return;
    }
    // The input batch's buffers belong to the upstream operator's allocator; the C Data export must
    // use that allocator (buffers associate only within one allocator root). The operator's own
    // allocator owns only the imported result.
    BufferAllocator inAllocator =
        in.getFieldVectors().isEmpty() ? allocator : in.getFieldVectors().get(0).getAllocator();
    CompiledCalc cached = compiledSchemas.get(in.getSchema());
    VectorSchemaRoot out;
    try (ArrowArray inArray = ArrowArray.allocateNew(inAllocator);
        ArrowArray outArray = ArrowArray.allocateNew(allocator);
        ArrowSchema outSchema = cached == null || cached.outputSchema == null
            ? ArrowSchema.allocateNew(allocator) : null) {
      try {
        // UNION ALL can mix insert-only batches with batches carrying the hidden row-kind
        // column. Each native handle caches its own immutable Arrow schema.
        if (cached != null) {
          Data.exportVectorSchemaRoot(inAllocator, in, dictionaries, inArray);
          if (outSchema == null) {
            Native.calcExpressionWithCachedSchemasAtWatermark(
                cached.handle, inArray.memoryAddress(), outArray.memoryAddress(), watermark);
          } else {
            Native.calcExpressionArrayAtWatermark(
                cached.handle, inArray.memoryAddress(), outArray.memoryAddress(),
                outSchema.memoryAddress(), watermark);
          }
        } else {
          long calc =
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
          cached = new CompiledCalc(calc);
          compiledSchemas.put(in.getSchema(), cached);
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
      if (outSchema != null) {
        out = Data.importVectorSchemaRoot(allocator, outArray, outSchema, dictionaries);
        // Dictionary IDs refer to mutable provider entries. Preserve the existing full
        // schema import instead of caching those IDs across result batches.
        if (out.getSchema().getFields().stream().noneMatch(NativeCalcOperator::hasDictionary))
          cached.outputSchema = out.getSchema();
      } else {
        out = VectorSchemaRoot.create(cached.outputSchema, allocator);
        try {
          Data.importIntoVectorSchemaRoot(allocator, outArray, out, dictionaries);
        } catch (Throwable failure) {
          out.close();
          throw failure;
        }
      }
    } finally {
      in.close(); // the input batch is consumed
    }
    ColumnarRecordMetrics.emit(output, getMetricGroup(), new ArrowBatch(out, keyGroup));
  }
}
