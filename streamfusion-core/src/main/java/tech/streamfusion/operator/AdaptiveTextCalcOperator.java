package tech.streamfusion.operator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.utils.ProjectedRowData;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.types.logical.RowType;
import tech.streamfusion.Native;
import tech.streamfusion.arrow.ArrowConversion;
import tech.streamfusion.arrow.ArrowSchemaViews;
import tech.streamfusion.arrow.ByteStringTransport;
import tech.streamfusion.arrow.NestedRepresentation;
import tech.streamfusion.arrow.StringCacheColumns;
import tech.streamfusion.planner.FlinkExpressionFunction;

/** Runs the existing Calc on validated text and its Flink evaluator on the remaining rows. */
public final class AdaptiveTextCalcOperator extends NativeCalcOperator {
  private static final long serialVersionUID = 1L;

  private final int[] textInputs;
  private final int[] stringOutputs;
  private final RowType inputType;
  private final RowType outputType;
  private final NativeUdf.Binding fallbackBinding;
  private final long[] fallbackLongs;
  private final boolean generatedOnly;
  private final boolean rowExit;

  private transient FlinkExpressionFunction fallback;
  private transient int[] fallbackColumns;
  private transient Schema fallbackSchema;
  private transient boolean fallbackStringCaches;
  private transient Counter nativeTextRows;
  private transient Counter flinkTextRows;

  public AdaptiveTextCalcOperator(
      int[] kinds, int[] payload, int[] childCounts, long[] longs, double[] doubles,
      String[] strings, int[] projectionRoots, int conditionRoot, String[] outputNames,
      NativeUdf.Binding binding, int[] textInputs, int[] stringOutputs, RowType inputType,
      RowType outputType, NativeUdf.Binding fallbackBinding, long[] fallbackLongs,
      boolean generatedOnly, boolean rowExit) {
    super(kinds, payload, childCounts, longs, doubles, strings, projectionRoots, conditionRoot,
        outputNames, generatedOnly ? NativeUdf.Binding.EMPTY : binding);
    this.textInputs = textInputs.clone();
    this.stringOutputs = stringOutputs.clone();
    this.inputType = inputType;
    this.outputType = outputType;
    this.fallbackBinding = fallbackBinding;
    this.fallbackLongs = fallbackLongs.clone();
    this.generatedOnly = generatedOnly;
    this.rowExit = rowExit;
  }

  @Override
  public void open() throws Exception {
    super.open();
    fallbackBinding.bind(fallbackLongs, new FunctionContext(getRuntimeContext()),
        getProcessingTimeService(), runtimeOptions());
    fallbackColumns = fallbackBinding.rowInputColumns();
    fallback = NativeUdf.generatedRowCalc(fallbackBinding.rowCalcId());
    var fields = new ArrayList<>(fallback.arrowResultSchema().getFields().get(0).getChildren());
    if (rowExit) {
      fallbackSchema = ByteStringTransport.resultSchema(new Schema(fields));
    } else {
      var scalarSchema = ArrowConversion.toArrowSchema(outputType);
      for (int index : stringOutputs) {
        if (NestedRepresentation.isString(fields.get(index))) {
          fields.set(index, scalarSchema.getFields().get(index));
        }
      }
      fallbackSchema = ByteStringTransport.inputSchema(new Schema(fields), stringOutputs);
    }
    fallbackStringCaches = fallbackSchema.getFields().stream().anyMatch(ByteStringTransport::isString);
    nativeTextRows = getMetricGroup().counter("validatedTextRows");
    flinkTextRows = getMetricGroup().counter("flinkTextRows");
  }

  @Override
  public void close() throws Exception {
    NativeResources.closeAll(fallbackBinding::unbind, super::close);
  }

  @Override
  public void processElement(StreamRecord<ArrowBatch> element) throws Exception {
    var batch = element.getValue();
    ColumnarRecordMetrics.countIngested(getMetricGroup(), batch.rowCount());
    try (var input = batch.root()) {
      int rows = input.getRowCount();
      if (rows == 0) return;
      if (generatedOnly) {
        emitResult(flinkRange(input, 0, rows), batch.keyGroup());
        flinkTextRows.inc(rows);
        return;
      }
      boolean[] exactRows = exactRows(input, rows);
      if (exactRows == null) {
        emitResult(nativeRange(input, 0, rows), batch.keyGroup());
        nativeTextRows.inc(rows);
        return;
      }
      // Consecutive ranges retain input order, including update pairs and rows dropped by filters.
      for (int start = 0; start < rows; ) {
        boolean exact = exactRows[start];
        int end = start + 1;
        while (end < rows && exactRows[end] == exact) end++;
        if (exact) {
          emitResult(flinkRange(input, start, end), batch.keyGroup());
          flinkTextRows.inc(end - start);
        } else {
          emitResult(nativeRange(input, start, end - start), batch.keyGroup());
          nativeTextRows.inc(end - start);
        }
        start = end;
      }
    }
  }

  private boolean[] exactRows(VectorSchemaRoot input, int rows) {
    var owner = input.getVector(textInputs[0]).getAllocator();
    VectorSchemaRoot validation = input;
    int[] columns = textInputs;
    if (rowExit && Arrays.stream(textInputs)
        .anyMatch(index -> NestedRepresentation.isString(input.getVector(index).getField()))) {
      var leaves = Arrays.stream(textInputs)
          .mapToObj(index -> ByteStringTransport.bytes(input.getVector(index)))
          .collect(Collectors.toList());
      validation = new VectorSchemaRoot(
          leaves.stream().map(FieldVector::getField).collect(Collectors.toList()), leaves, rows);
      columns = IntStream.range(0, leaves.size()).toArray();
    }
    int[] invalid;
    try (var array = ArrowArray.allocateNew(owner); var schema = ArrowSchema.allocateNew(owner)) {
      Data.exportVectorSchemaRoot(owner, validation, NativeAllocator.DICTIONARIES, array, schema);
      invalid = Native.invalidUtf8Rows(array.memoryAddress(), schema.memoryAddress(), columns);
    }
    boolean[] exact = invalid == null ? null : new boolean[rows];
    if (invalid != null) for (int row : invalid) exact[row] = true;
    var caches = StringCacheColumns.readers(input, inputType.getFieldCount());
    if (caches == null && validation == input) return exact;
    for (int column : textInputs) {
      var vector = input.getVector(column);
      var cache = NestedRepresentation.isString(vector.getField())
          ? (ListVector) ((StructVector) vector).getChild("java")
          : caches == null ? null : caches[column];
      if (cache == null) continue;
      for (int row = 0; row < rows; row++) {
        if (cache.isNull(row)) continue;
        if (exact == null) exact = new boolean[rows];
        exact[row] = true;
      }
    }
    return exact;
  }

  private VectorSchemaRoot nativeRange(VectorSchemaRoot input, int start, int length) throws Exception {
    var owner = input.getVector(textInputs[0]).getAllocator();
    try (var sliced = length == input.getRowCount() ? null : input.slice(start, length)) {
      var source = sliced == null ? input : sliced;
      var schema = ByteStringTransport.utf8Schema(source.getSchema(), textInputs);
      var valid = ArrowSchemaViews.retain(source.getFieldVectors(), schema, length, owner);
      var result = evaluateNative(valid);
      if (stringOutputs.length == 0) return result;
      int[] retagged = Arrays.stream(stringOutputs)
          .filter(index -> !ByteStringTransport.isString(result.getVector(index).getField()))
          .toArray();
      if (retagged.length == 0) return result;
      try (var owned = result) {
        return ArrowSchemaViews.retain(owned.getFieldVectors(),
            ByteStringTransport.inputSchema(owned.getSchema(), retagged),
            owned.getRowCount(), NativeAllocator.SHARED);
      }
    }
  }

  private VectorSchemaRoot flinkRange(VectorSchemaRoot input, int start, int end) throws Exception {
    try (var ignored = runtimeOptions().use()) {
      var reader = ArrowConversion.createArrowReader(input, inputType);
      var projected = ProjectedRowData.from(fallbackColumns);
      var originalKinds = (TinyIntVector) input.getVector(RowDataArrowConverter.ROW_KIND_COLUMN);
      var out = VectorSchemaRoot.create(fallbackSchema, NativeAllocator.SHARED);
      var caches = fallbackStringCaches
          ? new StringCacheColumns.Writer(outputType, NativeAllocator.SHARED)
          : null;
      TinyIntVector kinds = null;
      try {
        if (originalKinds != null) {
          kinds = new TinyIntVector(RowDataArrowConverter.ROW_KIND_COLUMN, NativeAllocator.SHARED);
          kinds.allocateNew(end - start);
        }
        var writer = ArrowConversion.createRowDataArrowWriter(out, outputType);
        int written = 0;
        for (int row = start; row < end; row++) {
          var value = (RowData) fallback.evalRow(projected.replaceRow(reader.read(row)));
          if (value == null) continue;
          if (caches != null) caches.write(value, written);
          writer.write(value);
          if (kinds != null) kinds.setSafe(written, originalKinds.get(row));
          written++;
        }
        writer.finish();
        var extra = new ArrayList<FieldVector>();
        if (kinds != null) {
          kinds.setValueCount(written);
          extra.add(kinds);
        }
        if (caches != null) extra.addAll(caches.finish(written));
        return RowDataArrowConverter.withExtraColumns(out, extra, written);
      } catch (Exception | Error failure) {
        try {
          org.apache.flink.util.IOUtils.closeAll(out, kinds, caches);
        } catch (Exception | Error closeFailure) {
          failure.addSuppressed(closeFailure);
        }
        throw failure;
      }
    }
  }
}
