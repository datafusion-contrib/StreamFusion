package tech.streamfusion.operator;

import tech.streamfusion.planner.NativeConfig;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.RowType;

/**
 * Transpose entering a columnar region: buffers rows and emits them as {@link ArrowBatch}es. Sits
 * where a rowwise (host) operator feeds a native columnar one, so the row→Arrow conversion happens
 * once at the boundary rather than inside every native operator.
 *
 * <p>Ownership of an emitted batch passes to the downstream operator, which closes it once read (in
 * a chained task the downstream consumes it inline). Watermarks pass through after the buffer flushes.
 */
public class RowDataToArrowOperator extends AbstractStreamOperator<ArrowBatch>
    implements OneInputStreamOperator<RowData, ArrowBatch>, BoundedOneInput {

  private final RowType rowType;
  private final int batchSize;
  private final boolean carryRowKind;
  private final RowType sourceType;

  private transient BufferAllocator allocator;
  private transient List<RowData> buffer;
  private transient PrunedRowData projector;
  private transient RowDataSerializer inputSerializer;
  private transient long flushLatencyMs;
  private transient long flushDeadline;

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
    allocator = NativeAllocator.SHARED;
    buffer = new ArrayList<>(batchSize);
    inputSerializer = new RowDataSerializer(sourceType == null ? rowType : sourceType);
    flushLatencyMs = NativeConfig.transposeFlushLatencyMs();
    flushDeadline = Long.MIN_VALUE;
    // When the planner pruned the transpose, present each wide source row as the narrowed schema so
    // the converter builds and fills only the read columns/sub-fields.
    projector = sourceType == null ? null : PrunedRowData.of(sourceType, rowType);
  }

  @Override
  public void processElement(StreamRecord<RowData> element) {
    // Chained Flink operators may reuse their RowData object immediately after collect(). This
    // boundary retains rows until a complete Arrow batch is ready, so it must take ownership with
    // a deep copy rather than retaining the caller's mutable view.
    boolean wasEmpty = buffer.isEmpty();
    buffer.add(inputSerializer.copy(element.getValue()));
    if (buffer.size() >= batchSize) {
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
    super.close();
  }

  private void flush() {
    flushDeadline = Long.MIN_VALUE;
    if (buffer.isEmpty()) {
      return;
    }
    List<RowData> rows = projector == null ? buffer : projected();
    VectorSchemaRoot root = RowDataArrowConverter.write(rows, rowType, allocator, carryRowKind);
    ColumnarRecordMetrics.emit(output, getMetricGroup(), new ArrowBatch(root));
    buffer.clear();
  }

  /**
   * The buffer presented through the pruning projector — a reused, zero-copy view repointed per row.
   * Safe because the converter reads each row inline (into the Arrow vectors) before requesting the
   * next, so the shared projector is never observed at two positions at once.
   */
  private List<RowData> projected() {
    return new AbstractList<RowData>() {
      @Override
      public RowData get(int index) {
        return projector.replaceRow(buffer.get(index));
      }

      @Override
      public int size() {
        return buffer.size();
      }
    };
  }
}
