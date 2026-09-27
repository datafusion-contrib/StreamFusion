package tech.streamfusion.operator;

import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import tech.streamfusion.arrow.ArrowConversion;
import tech.streamfusion.arrow.ArrowReader;
import tech.streamfusion.compat.FlinkStreamOperator;

/**
 * Transpose leaving a columnar region: reads each {@link ArrowBatch} back into rows. Sits where a
 * native columnar operator feeds a rowwise (host) one, so the Arrow→row conversion happens once at
 * the boundary. It consumes (and closes) each batch it receives.
 *
 * <p>The Arrow reader exposes a reusable view backed by the input batch. Chained Flink operators
 * are allowed to retain a collected {@code RowData}. With object reuse disabled Flink's chained
 * output copies the view synchronously; with reuse enabled this boundary supplies the owned copy.
 * Network outputs serialize synchronously before the batch closes.
 */
public class ArrowToRowDataOperator extends FlinkStreamOperator<RowData>
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
    outputSerializer = getExecutionConfig().isObjectReuseEnabled()
        ? new RowDataSerializer(rowType) : null;
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
        output.collect(new StreamRecord<>(outputSerializer == null ? row : outputSerializer.copy(row)));
      }
    }
    convertTime.inc(System.nanoTime() - started);
  }
}
