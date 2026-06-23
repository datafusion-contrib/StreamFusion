package io.github.jordepic.streamfusion.operator;

import java.util.ArrayList;
import java.util.List;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.RowData;

/**
 * Row-fed native window operators: buffers incoming {@link RowData} into batches and folds each into
 * the native aggregator. The handle lifecycle, checkpointed state, watermark firing, and emit live
 * in {@link NativeWindowOperatorCore}; this layer only supplies the row input path. Each concrete
 * operator decides how a buffered batch is fed to native code and how closed windows are emitted.
 */
public abstract class NativeWindowOperatorBase extends NativeRowWindowOperatorCore
    implements OneInputStreamOperator<RowData, RowData> {

  private final int batchSize;
  private transient List<RowData> buffer;

  protected NativeWindowOperatorBase(
      String stateName,
      long windowMillis,
      long slideMillis,
      int[] valueTypes,
      int[] aggregateKinds,
      String timeZoneId,
      int batchSize) {
    super(stateName, windowMillis, slideMillis, valueTypes, aggregateKinds, timeZoneId);
    this.batchSize = batchSize;
  }

  /** Feeds the buffered rows to the native aggregator. Called when the batch fills or fires. */
  protected abstract void pushBatch(List<RowData> rows);

  @Override
  public void open() throws Exception {
    super.open();
    buffer = new ArrayList<>(batchSize);
  }

  @Override
  public void processElement(StreamRecord<RowData> element) {
    buffer.add(element.getValue());
    if (buffer.size() >= batchSize) {
      flushPending();
    }
  }

  @Override
  protected void flushPending() {
    if (buffer.isEmpty()) {
      return;
    }
    pushBatch(buffer);
    buffer.clear();
  }
}
