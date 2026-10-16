package tech.streamfusion.operator;

import java.util.ArrayList;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.operators.aggregate.GroupAggFunction;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import tech.streamfusion.arrow.ArrowConversion;
import tech.streamfusion.arrow.ArrowWriter;
import tech.streamfusion.compat.FlinkStreamOperator;
import tech.streamfusion.compat.JvmAggregateCompat;

/** JVM accumulators use released Flink managed state behind Arrow batch boundaries. */
public final class NativeColumnarJvmGroupOperator extends FlinkStreamOperator<ArrowBatch>
    implements OneInputStreamOperator<ArrowBatch, ArrowBatch>,
        org.apache.flink.streaming.api.operators.BoundedOneInput {
  private final RowType inputType;
  private final RowType outputType;
  private final GroupAggFunction function;
  private final RowDataKeySelector keySelector;
  private final int batchRows;
  private final int cacheEntries;
  private final LogicalTypeRoot keyType;
  private final int keyColumn;
  private transient JvmAccumulatorCache accumulatorCache;
  private transient KeyedProcessFunction<RowData, RowData, RowData>.Context context;
  private transient Collector<RowData> collector;
  private transient VectorSchemaRoot result;
  private transient ArrowWriter<RowData> writer;
  private transient TinyIntVector kinds;
  private transient int count;
  private transient BatchCoalescer coalescer;
  private transient org.apache.arrow.vector.types.pojo.Schema outputSchema;

  public NativeColumnarJvmGroupOperator(
      RowType inputType,
      RowType outputType,
      GroupAggFunction function,
      RowDataKeySelector keySelector,
      int batchRows,
      int cacheEntries,
      LogicalTypeRoot keyType,
      int keyColumn) {
    this.inputType = inputType;
    this.outputType = outputType;
    this.function = function;
    this.keySelector = keySelector;
    this.batchRows = batchRows;
    this.cacheEntries = cacheEntries;
    this.keyType = keyType;
    this.keyColumn = keyColumn;
  }

  @Override
  public void open() throws Exception {
    super.open();
    NativeAllocator.initializeFor(this);
    outputSchema = ArrowConversion.toArrowSchema(outputType);
    collector =
        new Collector<RowData>() {
          @Override
          public void collect(RowData row) {
            append(row);
          }

          @Override
          public void close() {}
        };
    context =
        function.new Context() {
          @Override
          public RowData getCurrentKey() {
            return (RowData) NativeColumnarJvmGroupOperator.this.getCurrentKey();
          }

          @Override
          public Long timestamp() {
            return null;
          }

          @Override
          public TimerService timerService() {
            throw new UnsupportedOperationException(
                "released synchronous grouped aggregation has no explicit timers");
          }

          @Override
          public <X> void output(OutputTag<X> tag, X value) {
            output.collect(tag, new StreamRecord<>(value));
          }
        };
    var runtime = getRuntimeContext();
    if (cacheEntries > 0 && keyType != null) {
      accumulatorCache =
          new JvmAccumulatorCache(
              cacheEntries,
              keyType,
              this::getCurrentKey,
              this::setCurrentKey,
              getMetricGroup().counter("jvmAccumulatorCacheHits"),
              getMetricGroup().counter("jvmAccumulatorCacheBypasses"));
      JvmAggregateCompat.open(function, accumulatorCache.wrap(runtime));
    } else JvmAggregateCompat.open(function, runtime);
    coalescer = BatchCoalescer.create(getProcessingTimeService(), this::ingest);
  }

  @Override
  public void processElement(StreamRecord<ArrowBatch> record) throws Exception {
    ColumnarRecordMetrics.countIngested(getMetricGroup(), record.getValue().rowCount());
    var input = record.getValue().root();
    if (coalescer == null) ingest(input);
    else coalescer.add(input);
  }

  private void ingest(VectorSchemaRoot input) {
    try (input) {
      var reader = ArrowConversion.createArrowReader(input, inputType);
      var keyReader =
          keyType == LogicalTypeRoot.VARCHAR
              ? ArrowConversion.createBorrowedStringReader(input, inputType)
              : reader;
      var inputKinds = (TinyIntVector) input.getVector(RowDataArrowConverter.ROW_KIND_COLUMN);
      for (int index = 0; index < input.getRowCount(); index++) {
        var row = reader.read(index);
        var keyRow = keyReader == reader ? row : keyReader.read(index);
        row.setRowKind(
            inputKinds == null ? RowKind.INSERT : RowKind.fromByteValue(inputKinds.get(index)));
        setCurrentKey(
            accumulatorCache == null
                ? keySelector.getKey(keyRow)
                : accumulatorCache.keyFor(keyRow, keyColumn, keySelector));
        function.processElement(row, context, collector);
      }
      flush();
    } catch (Exception failure) {
      throw new org.apache.flink.util.FlinkRuntimeException(
          "JVM aggregate evaluation failed", failure);
    } finally {
      discard();
    }
  }

  @Override
  public void endInput() throws Exception {
    if (coalescer != null) coalescer.flush();
    flush();
    if (accumulatorCache != null) accumulatorCache.flush();
  }

  @Override
  public void finish() throws Exception {
    endInput();
    super.finish();
  }

  @Override
  public void processWatermark(Watermark mark) throws Exception {
    endInput();
    super.processWatermark(mark);
  }

  @Override
  public void prepareSnapshotPreBarrier(long checkpointId) throws Exception {
    endInput();
    if (accumulatorCache != null) accumulatorCache.invalidate();
    super.prepareSnapshotPreBarrier(checkpointId);
  }

  @Override
  public void snapshotState(org.apache.flink.runtime.state.StateSnapshotContext context)
      throws Exception {
    if (accumulatorCache != null) {
      accumulatorCache.flush();
      accumulatorCache.invalidate();
    }
    super.snapshotState(context);
  }

  private void append(RowData row) {
    if (result == null) {
      result = VectorSchemaRoot.create(outputSchema, NativeAllocator.SHARED);
      writer = ArrowConversion.createRowDataArrowWriter(result, outputType);
      kinds = new TinyIntVector(RowDataArrowConverter.ROW_KIND_COLUMN, NativeAllocator.SHARED);
      kinds.allocateNew(batchRows);
    }
    writer.write(row);
    kinds.setSafe(count++, row.getRowKind().toByteValue());
    if (count == batchRows) flush();
  }

  private void flush() {
    if (result == null) return;
    writer.finish();
    kinds.setValueCount(count);
    var columns = new ArrayList<FieldVector>(result.getFieldVectors());
    columns.add(kinds);
    var root = new VectorSchemaRoot(columns);
    root.setRowCount(count);
    result = null;
    kinds = null;
    writer = null;
    count = 0;
    ColumnarRecordMetrics.emit(output, getMetricGroup(), new ArrowBatch(root));
  }

  private void discard() {
    try {
      if (result != null) result.close();
    } finally {
      if (kinds != null) kinds.close();
      result = null;
      kinds = null;
      writer = null;
      count = 0;
    }
  }

  @Override
  public void close() throws Exception {
    if (accumulatorCache != null) accumulatorCache.invalidate();
    try {
      if (coalescer != null) coalescer.close();
    } finally {
      try {
        function.close();
      } finally {
        discard();
        super.close();
      }
    }
  }
}
