package tech.streamfusion.operator;

import tech.streamfusion.arrow.ArrowConversion;
import tech.streamfusion.arrow.ArrowReader;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.apache.flink.metrics.Counter;

/**
 * Transpose leaving a columnar region: reads each {@link ArrowBatch} back into rows. Sits where a
 * native columnar operator feeds a rowwise (host) one, so the Arrow→row conversion happens once at
 * the boundary. It consumes (and closes) each batch it receives.
 *
 * <p>The Arrow reader exposes a reusable view backed by the input batch. Chained Flink operators are
 * allowed to retain a collected {@code RowData}, and closing this batch invalidates every such view,
 * so the boundary deep-copies each row before handing it back to the rowwise runtime.
 */
public class ArrowToRowDataOperator extends AbstractStreamOperator<RowData>
    implements OneInputStreamOperator<ArrowBatch, RowData> {

  private final RowType rowType;
  private transient RowDataSerializer outputSerializer;
  private transient Counter numInputBatches;
  private transient Counter numOutputRows;
  private transient Counter convertTime;

  public ArrowToRowDataOperator(RowType rowType) {
    this.rowType = rowType;
  }

  @Override
  public void open() throws Exception {
    super.open();
    outputSerializer = new RowDataSerializer(rowType);
    numInputBatches = getMetricGroup().counter("numInputBatches");
    numOutputRows = getMetricGroup().counter("numOutputRows");
    convertTime = getMetricGroup().counter("convertTime");
  }

  @Override
  public void processElement(StreamRecord<ArrowBatch> element) {
    ColumnarRecordMetrics.countIngested(getMetricGroup(), element.getValue().rowCount());
    numInputBatches.inc();
    long started = System.nanoTime();
    try (VectorSchemaRoot root = element.getValue().root()) {
      ArrowReader reader = ArrowConversion.createArrowReader(root, rowType);
      TinyIntVector kinds = (TinyIntVector) root.getVector(RowDataArrowConverter.ROW_KIND_COLUMN);
      int rowCount = root.getRowCount();
      numOutputRows.inc(rowCount);
      for (int i = 0; i < rowCount; i++) {
        RowData row = reader.read(i);
        if (kinds != null) {
          row.setRowKind(RowKind.fromByteValue(kinds.get(i)));
        }
        output.collect(new StreamRecord<>(outputSerializer.copy(row)));
      }
    }
    convertTime.inc(System.nanoTime() - started);
  }
}
