package tech.streamfusion.operator;

import java.util.ArrayList;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.metrics.Counter;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.streaming.api.SimpleTimerService;
import org.apache.flink.streaming.api.TimeDomain;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.InternalTimer;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.Triggerable;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import tech.streamfusion.arrow.ArrowConversion;
import tech.streamfusion.arrow.ArrowWriter;
import tech.streamfusion.compat.FlinkStreamOperator;
import tech.streamfusion.compat.JvmAggregateCompat;

/** Released OVER frames, managed accumulators and timers behind owned Arrow batch boundaries. */
public final class NativeColumnarJvmOverOperator extends FlinkStreamOperator<ArrowBatch>
    implements OneInputStreamOperator<ArrowBatch, ArrowBatch>,
        BoundedOneInput,
        Triggerable<RowData, VoidNamespace> {
  private final RowType inputType;
  private final RowType outputType;
  private final KeyedProcessFunction<RowData, RowData, RowData> function;
  private final RowDataKeySelector keySelector;
  private final int batchRows;
  private transient RowDataSerializer inputs;
  private transient Collector<RowData> collector;
  private transient KeyedProcessFunction<RowData, RowData, RowData>.Context context;
  private transient KeyedProcessFunction<RowData, RowData, RowData>.OnTimerContext timerContext;
  private transient InternalTimer<RowData, VoidNamespace> currentTimer;
  private transient TimeDomain domain;
  private transient Long recordTimestamp;
  private transient VectorSchemaRoot result;
  private transient ArrowWriter<RowData> writer;
  private transient TinyIntVector kinds;
  private transient tech.streamfusion.arrow.StringCacheColumns.Writer caches;
  private transient int count;
  private transient Counter inputRows;
  private transient Counter eventTimers;
  private transient Counter processingTimers;
  private transient org.apache.arrow.vector.types.pojo.Schema outputSchema;

  public NativeColumnarJvmOverOperator(
      RowType inputType,
      RowType outputType,
      KeyedProcessFunction<RowData, RowData, RowData> function,
      RowDataKeySelector keySelector,
      int batchRows) {
    this.inputType = inputType;
    this.outputType = outputType;
    this.function = function;
    this.keySelector = keySelector;
    this.batchRows = batchRows;
  }

  @Override
  public void open() throws Exception {
    super.open();
    NativeAllocator.initializeFor(this);
    inputs = new RowDataSerializer(inputType);
    outputSchema = ArrowConversion.toArrowSchema(outputType);
    inputRows = getMetricGroup().counter("jvmOverRows");
    eventTimers = getMetricGroup().counter("jvmOverEventTimers");
    processingTimers = getMetricGroup().counter("jvmOverProcessingTimers");
    var internalTimers =
        getInternalTimerService("user-timers", VoidNamespaceSerializer.INSTANCE, this);
    TimerService timers = new SimpleTimerService(internalTimers);
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
            return (RowData) NativeColumnarJvmOverOperator.this.getCurrentKey();
          }

          @Override
          public Long timestamp() {
            return recordTimestamp;
          }

          @Override
          public TimerService timerService() {
            return timers;
          }

          @Override
          public <X> void output(OutputTag<X> tag, X value) {
            output.collect(
                tag,
                recordTimestamp == null
                    ? new StreamRecord<>(value)
                    : new StreamRecord<>(value, recordTimestamp));
          }
        };
    timerContext =
        function.new OnTimerContext() {
          @Override
          public RowData getCurrentKey() {
            return currentTimer.getKey();
          }

          @Override
          public Long timestamp() {
            return currentTimer.getTimestamp();
          }

          @Override
          public TimerService timerService() {
            return timers;
          }

          @Override
          public TimeDomain timeDomain() {
            return domain;
          }

          @Override
          public <X> void output(OutputTag<X> tag, X value) {
            output.collect(tag, new StreamRecord<>(value, currentTimer.getTimestamp()));
          }
        };
    JvmAggregateCompat.openOver(function, getRuntimeContext());
  }

  @Override
  public void processElement(StreamRecord<ArrowBatch> record) throws Exception {
    ColumnarRecordMetrics.countIngested(getMetricGroup(), record.getValue().rowCount());
    recordTimestamp = record.hasTimestamp() ? record.getTimestamp() : null;
    try (var root = record.getValue().root()) {
      var reader = ArrowConversion.createArrowReader(root, inputType);
      var rowKinds = (TinyIntVector) root.getVector(RowDataArrowConverter.ROW_KIND_COLUMN);
      for (int index = 0; index < root.getRowCount(); index++) {
        // Released heap frame state can retain the entire row beyond this batch and watermark.
        RowData row = inputs.toBinaryRow(reader.read(index)).copy();
        row.setRowKind(
            rowKinds == null ? RowKind.INSERT : RowKind.fromByteValue(rowKinds.get(index)));
        setCurrentKey(keySelector.getKey(row));
        function.processElement(row, context, collector);
        inputRows.inc();
      }
      flush();
    } finally {
      recordTimestamp = null;
      discard();
    }
  }

  private void timer(InternalTimer<RowData, VoidNamespace> timer, TimeDomain timeDomain)
      throws Exception {
    currentTimer = timer;
    domain = timeDomain;
    try {
      function.onTimer(timer.getTimestamp(), timerContext, collector);
      (timeDomain == TimeDomain.EVENT_TIME ? eventTimers : processingTimers).inc();
      flush();
    } finally {
      currentTimer = null;
      domain = null;
      discard();
    }
  }

  @Override
  public void onEventTime(InternalTimer<RowData, VoidNamespace> timer) throws Exception {
    timer(timer, TimeDomain.EVENT_TIME);
  }

  @Override
  public void onProcessingTime(InternalTimer<RowData, VoidNamespace> timer) throws Exception {
    timer(timer, TimeDomain.PROCESSING_TIME);
  }

  @Override
  public void processWatermark(Watermark mark) throws Exception {
    flush();
    super.processWatermark(mark);
  }

  @Override
  public void prepareSnapshotPreBarrier(long checkpointId) throws Exception {
    flush();
    super.prepareSnapshotPreBarrier(checkpointId);
  }

  @Override
  public void endInput() {
    flush();
  }

  @Override
  public void finish() throws Exception {
    flush();
    super.finish();
  }

  private void append(RowData row) {
    if (result == null) {
      result = VectorSchemaRoot.create(outputSchema, NativeAllocator.SHARED);
      writer = ArrowConversion.createRowDataArrowWriter(result, outputType);
      caches =
          new tech.streamfusion.arrow.StringCacheColumns.Writer(outputType, NativeAllocator.SHARED);
      kinds = new TinyIntVector(RowDataArrowConverter.ROW_KIND_COLUMN, NativeAllocator.SHARED);
      kinds.allocateNew(batchRows);
    }
    caches.write(row, count);
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
    columns.addAll(caches.finish(count));
    caches = null;
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
      if (caches != null) caches.close();
      caches = null;
      result = null;
      kinds = null;
      writer = null;
      count = 0;
    }
  }

  @Override
  public void close() throws Exception {
    try {
      function.close();
    } finally {
      discard();
      super.close();
    }
  }
}
