package tech.streamfusion.operator;

import java.util.ArrayList;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import tech.streamfusion.arrow.ArrowConversion;
import tech.streamfusion.arrow.ArrowWriter;
import tech.streamfusion.compat.FlinkStreamOperator;
import tech.streamfusion.planner.NativeConfig;

/**
 * Transpose entering a columnar region: writes rows into owned Arrow buffers and emits them as
 * {@link ArrowBatch}es. Sits where a rowwise (host) operator feeds a native columnar one, so the
 * row→Arrow conversion happens once at the boundary rather than inside every native operator.
 *
 * <p>Ownership of an emitted batch passes to the downstream operator, which closes it once read (in
 * a chained task the downstream consumes it inline). Watermarks pass through after the buffer
 * flushes.
 */
public class RowDataToArrowOperator extends FlinkStreamOperator<ArrowBatch>
    implements OneInputStreamOperator<RowData, ArrowBatch>, BoundedOneInput {
  private static final int TIMING_SAMPLE_RATE = 64;

  private final RowType rowType;
  private final int batchSize;
  private final boolean carryRowKind;
  private final RowType sourceType;

  private transient BufferAllocator allocator;
  private transient VectorSchemaRoot pendingRoot;
  private transient ArrowWriter<RowData> writer;
  private transient TinyIntVector kinds;
  private transient int pendingRows;
  private transient PrunedRowData projector;
  private transient long flushLatencyMs;
  private transient long flushDeadline;
  private transient Counter numInputRows;
  private transient Counter numOutputBatches;
  private transient Counter conversionTime;

  public RowDataToArrowOperator(
      RowType rowType, int batchSize, boolean carryRowKind, RowType sourceType) {
    this.rowType = rowType;
    this.batchSize = batchSize;
    this.carryRowKind = carryRowKind;
    this.sourceType = sourceType;
  }

  @Override
  public void open() throws Exception {
    super.open();
    NativeAllocator.initializeFor(this);
    allocator = NativeAllocator.SHARED;
    flushLatencyMs = NativeConfig.transposeFlushLatencyMs();
    flushDeadline = Long.MIN_VALUE;
    // Prune before taking ownership so unused payload is neither copied nor buffered.
    projector = sourceType == null ? null : PrunedRowData.of(sourceType, rowType);
    numInputRows = getMetricGroup().counter("numInputRows");
    numOutputBatches = getMetricGroup().counter("numOutputBatches");
    conversionTime = getMetricGroup().counter("conversionTime");
  }

  @Override
  public void processElement(StreamRecord<RowData> element) {
    // Copy into owned Arrow buffers before upstream can mutate its reusable row or nested values.
    boolean wasEmpty = pendingRows == 0;
    numInputRows.inc();
    // Time allocation exactly; randomly sample other writes to avoid a clock call per row and
    // avoid aliasing periodic input shapes. Scale sampled durations to estimate conversion time.
    boolean allocating = pendingRoot == null;
    boolean timed = allocating || ThreadLocalRandom.current().nextInt(TIMING_SAMPLE_RATE) == 0;
    long started = timed ? System.nanoTime() : 0;
    try {
      if (pendingRoot == null) {
        pendingRoot = VectorSchemaRoot.create(ArrowConversion.toArrowSchema(rowType), allocator);
        writer = ArrowConversion.createRowDataArrowWriter(pendingRoot, rowType);
        if (carryRowKind) {
          kinds = new TinyIntVector(RowDataArrowConverter.ROW_KIND_COLUMN, allocator);
          kinds.allocateNew(batchSize);
        }
      }
      RowData row = projector == null ? element.getValue() : projector.replaceRow(element.getValue());
      writer.write(row);
      if (kinds != null) kinds.setSafe(pendingRows, row.getRowKind().toByteValue());
      pendingRows++;
    } catch (RuntimeException | Error failure) {
      try {
        releasePending();
      } catch (Exception | Error closeFailure) {
        failure.addSuppressed(closeFailure);
      }
      throw failure;
    } finally {
      if (projector != null) projector.clear();
      if (timed) {
        conversionTime.inc((System.nanoTime() - started) * (allocating ? 1 : TIMING_SAMPLE_RATE));
      }
    }
    if (pendingRows >= batchSize) {
      flush();
    } else if (wasEmpty && flushLatencyMs > 0) {
      armFlushTimer();
    }
  }

  private void armFlushTimer() {
    long deadline = getProcessingTimeService().getCurrentProcessingTime() + flushLatencyMs;
    flushDeadline = deadline;
    getProcessingTimeService()
        .registerTimer(
            deadline,
            timestamp -> {
              if (flushDeadline == timestamp) {
                flush();
              }
            });
  }

  @Override
  public void processWatermark(Watermark mark) throws Exception {
    flush();
    super.processWatermark(mark);
  }

  @Override
  public void endInput() {
    flush();
  }

  /**
   * Drains rows accepted before the checkpoint barrier. The upstream source will restore past those
   * rows from its checkpointed offset, while this boundary keeps no state of its own.
   */
  @Override
  public void prepareSnapshotPreBarrier(long checkpointId) throws Exception {
    flush();
    super.prepareSnapshotPreBarrier(checkpointId);
  }

  @Override
  public void close() throws Exception {
    try {
      releasePending();
    } finally {
      super.close();
    }
  }

  private void releasePending() throws Exception {
    VectorSchemaRoot root = pendingRoot;
    TinyIntVector rowKinds = kinds;
    clearPending();
    org.apache.flink.util.IOUtils.closeAll(root, rowKinds);
  }

  private void clearPending() {
    pendingRoot = null;
    writer = null;
    kinds = null;
    pendingRows = 0;
    flushDeadline = Long.MIN_VALUE;
  }

  private void flush() {
    flushDeadline = Long.MIN_VALUE;
    if (pendingRows == 0) return;
    long started = System.nanoTime();
    writer.finish();
    VectorSchemaRoot root = pendingRoot;
    if (kinds != null) {
      kinds.setValueCount(pendingRows);
      var columns = new ArrayList<FieldVector>(root.getFieldVectors());
      columns.add(kinds);
      root = new VectorSchemaRoot(columns);
      root.setRowCount(pendingRows);
    }
    ArrowBatch batch = new ArrowBatch(root);
    clearPending();
    conversionTime.inc(System.nanoTime() - started);
    numOutputBatches.inc();
    ColumnarRecordMetrics.emit(output, getMetricGroup(), batch);
  }
}
