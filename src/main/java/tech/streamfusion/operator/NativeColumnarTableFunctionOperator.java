package tech.streamfusion.operator;

import java.util.ArrayList;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.generated.GeneratedClass;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import tech.streamfusion.arrow.ArrowConversion;
import tech.streamfusion.arrow.ArrowWriter;
import tech.streamfusion.compat.FlinkOutput;
import tech.streamfusion.compat.FlinkStreamOperator;

/**
 * Borrows Arrow inputs for synchronous JVM evaluation and writes every emitted value immediately.
 */
public final class NativeColumnarTableFunctionOperator extends FlinkStreamOperator<ArrowBatch>
    implements OneInputStreamOperator<ArrowBatch, ArrowBatch> {
  private final RowType inputType;
  private final RowType outputType;
  private final GeneratedClass<? extends StreamOperator<RowData>> generated;
  private final int batchRows;
  private final int[] projection;
  private final boolean preserveCollections;
  private final int[] inputMapping;
  private final org.apache.flink.table.runtime.generated.GeneratedProjection generatedProjection;
  private transient org.apache.flink.table.runtime.generated.Projection<RowData, ? extends RowData>
      rowProjection;
  private transient org.apache.flink.table.data.utils.ProjectedRowData inputProjector;
  private transient org.apache.flink.table.data.utils.ProjectedRowData projected;
  private transient org.apache.arrow.vector.types.pojo.Schema outputSchema;
  private transient OneInputStreamOperator<RowData, RowData> evaluator;
  private transient StreamRecord<RowData> inputRecord;
  private transient VectorSchemaRoot result;
  private transient ArrowWriter<RowData> writer;
  private transient TinyIntVector resultKinds;
  private transient int count;
  private transient tech.streamfusion.arrow.StringCacheColumns.Writer caches;

  public NativeColumnarTableFunctionOperator(
      RowType inputType,
      RowType outputType,
      GeneratedClass<? extends StreamOperator<RowData>> generated,
      int[] projection,
      int batchRows) {
    this(inputType, outputType, generated, projection, batchRows, false);
  }

  public NativeColumnarTableFunctionOperator(
      RowType inputType,
      RowType outputType,
      GeneratedClass<? extends StreamOperator<RowData>> generated,
      int[] projection,
      int batchRows,
      boolean preserveCollections) {
    this(inputType, outputType, generated, projection, batchRows, preserveCollections, null);
  }

  public NativeColumnarTableFunctionOperator(
      RowType inputType,
      RowType outputType,
      GeneratedClass<? extends StreamOperator<RowData>> generated,
      int[] projection,
      int batchRows,
      boolean preserveCollections,
      int[] inputMapping) {
    this(
        inputType,
        outputType,
        generated,
        projection,
        batchRows,
        preserveCollections,
        inputMapping,
        null);
  }

  public NativeColumnarTableFunctionOperator(
      RowType inputType,
      RowType outputType,
      GeneratedClass<? extends StreamOperator<RowData>> generated,
      int[] projection,
      int batchRows,
      boolean preserveCollections,
      int[] inputMapping,
      org.apache.flink.table.runtime.generated.GeneratedProjection generatedProjection) {
    this.generatedProjection = generatedProjection;
    this.inputMapping = inputMapping;
    this.preserveCollections = preserveCollections;
    this.inputType = inputType;
    this.outputType = outputType;
    this.generated = generated;
    this.batchRows = batchRows;
    this.projection = projection;
  }

  @Override
  @SuppressWarnings("unchecked")
  public void open() throws Exception {
    super.open();
    NativeAllocator.initializeFor(this);
    inputRecord = new StreamRecord<>(null);
    inputProjector =
        inputMapping == null
            ? null
            : org.apache.flink.table.data.utils.ProjectedRowData.from(inputMapping);
    outputSchema = ArrowConversion.toArrowSchema(outputType, preserveCollections);
    if (preserveCollections)
      outputSchema = tech.streamfusion.arrow.NestedRepresentation.withBinaryRows(outputSchema);
    projected = org.apache.flink.table.data.utils.ProjectedRowData.from(projection);
    rowProjection =
        generatedProjection == null
            ? null
            : generatedProjection.newInstance(getUserCodeClassloader());
    var collector =
        new FlinkOutput<RowData>(output) {
          @Override
          public void collect(StreamRecord<RowData> record) {
            RowData input = record.getValue();
            if (rowProjection == null) append(projected.replaceRow(input));
            else {
              RowData row = rowProjection.apply(input);
              row.setRowKind(input.getRowKind());
              append(row);
            }
          }
        };
    evaluator =
        (OneInputStreamOperator<RowData, RowData>)
            generated.newInstance(
                getUserCodeClassloader(),
                generated.getReferences(),
                getContainingTask(),
                getOperatorConfig(),
                collector,
                getProcessingTimeService());
    evaluator.open();
  }

  @Override
  public void processElement(StreamRecord<ArrowBatch> element) throws Exception {
    ColumnarRecordMetrics.countIngested(getMetricGroup(), element.getValue().rowCount());
    try (var input = element.getValue().root()) {
      var reader = ArrowConversion.createArrowReader(input, inputType);
      var kinds = (TinyIntVector) input.getVector(RowDataArrowConverter.ROW_KIND_COLUMN);
      for (int index = 0; index < input.getRowCount(); index++) {
        var row = reader.read(index);
        row.setRowKind(kinds == null ? RowKind.INSERT : RowKind.fromByteValue(kinds.get(index)));
        evaluator.processElement(
            inputRecord.replace(inputProjector == null ? row : inputProjector.replaceRow(row)));
      }
      flush();
    } finally {
      discard();
    }
  }

  private void append(RowData row) {
    if (result == null) {
      result = VectorSchemaRoot.create(outputSchema, NativeAllocator.SHARED);
      writer = ArrowConversion.createRowDataArrowWriter(result, outputType);
      if (preserveCollections)
        caches =
            new tech.streamfusion.arrow.StringCacheColumns.Writer(
                outputType, NativeAllocator.SHARED);
      resultKinds =
          new TinyIntVector(RowDataArrowConverter.ROW_KIND_COLUMN, NativeAllocator.SHARED);
      resultKinds.allocateNew(batchRows);
    }
    if (caches != null) caches.write(row, count);
    writer.write(row);
    resultKinds.setSafe(count++, row.getRowKind().toByteValue());
    if (count == batchRows) flush();
  }

  private void flush() {
    if (result == null) return;
    writer.finish();
    resultKinds.setValueCount(count);
    var columns = new ArrayList<FieldVector>(result.getFieldVectors());
    columns.add(resultKinds);
    if (caches != null) columns.addAll(caches.finish(count));
    caches = null;
    var batch = new VectorSchemaRoot(columns);
    batch.setRowCount(count);
    result = null;
    resultKinds = null;
    writer = null;
    count = 0;
    ColumnarRecordMetrics.emit(output, getMetricGroup(), new ArrowBatch(batch));
  }

  private void discard() {
    try {
      if (result != null) result.close();
    } finally {
      if (resultKinds != null) resultKinds.close();
      if (caches != null) caches.close();
      caches = null;
      result = null;
      resultKinds = null;
      writer = null;
      count = 0;
    }
  }

  @Override
  public void finish() throws Exception {
    if (evaluator != null) evaluator.finish();
    flush();
    super.finish();
  }

  @Override
  public void close() throws Exception {
    try {
      if (evaluator != null) evaluator.close();
    } finally {
      discard();
      super.close();
    }
  }
}
