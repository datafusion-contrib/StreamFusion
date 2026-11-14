package tech.streamfusion.operator;

import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import tech.streamfusion.compat.FlinkStreamOperator;

/** A bounded unordered limit consumes all input and forwards only its selected Arrow ranges. */
public final class NativeBatchLimitOperator extends FlinkStreamOperator<ArrowBatch>
    implements OneInputStreamOperator<ArrowBatch, ArrowBatch> {
  private final long offset, count;
  private transient long skip, remaining;

  public NativeBatchLimitOperator(long offset, long end) {
    if (offset < 0 || end < offset) throw new IllegalArgumentException("Invalid batch limit");
    this.offset = offset;
    this.count = end - offset;
  }

  @Override
  public void open() throws Exception {
    super.open();
    skip = offset;
    remaining = count;
  }

  @Override
  public void processElement(StreamRecord<ArrowBatch> element) {
    var batch = element.getValue();
    var root = batch.root();
    int rows = root.getRowCount();
    ColumnarRecordMetrics.countIngested(getMetricGroup(), rows);
    int start = (int) Math.min(skip, rows);
    skip -= start;
    int length = (int) Math.min(remaining, rows - start);
    remaining -= length;
    if (length == 0) {
      root.close();
      return;
    }
    if (start == 0 && length == rows) {
      ColumnarRecordMetrics.emit(output, getMetricGroup(), new ArrowBatch(root, batch.keyGroup()));
    } else {
      org.apache.arrow.vector.VectorSchemaRoot selected;
      try {
        selected = root.slice(start, length);
      } finally {
        root.close();
      }
      ColumnarRecordMetrics.emit(
          output, getMetricGroup(), new ArrowBatch(selected, batch.keyGroup()));
    }
  }
}
