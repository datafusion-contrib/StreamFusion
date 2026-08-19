package tech.streamfusion.delta;

import io.delta.flink.sink.Conversions;
import io.delta.kernel.types.StructType;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import tech.streamfusion.operator.ArrowBatch;
import tech.streamfusion.operator.RowDataArrowConverter;

/** Emits retained RowData views for Delta bookkeeping without materializing Arrow rows. */
public final class ArrowToDeltaRowsOperator extends AbstractStreamOperator<RowData>
    implements OneInputStreamOperator<ArrowBatch, RowData> {

  private final RowType rowType;
  private transient StructType deltaSchema;

  public ArrowToDeltaRowsOperator(RowType rowType) {
    this.rowType = rowType;
  }

  @Override
  public void open() throws Exception {
    super.open();
    deltaSchema = Conversions.FlinkToDelta.schema(rowType);
  }

  @Override
  public void processElement(StreamRecord<ArrowBatch> element) {
    VectorSchemaRoot root = element.getValue().root();
    int rows = root.getRowCount();
    if (rows == 0) {
      root.close();
      return;
    }
    ArrowKernelRows views = new ArrowKernelRows(root, rowType, deltaSchema);
    TinyIntVector kinds = (TinyIntVector) root.getVector(RowDataArrowConverter.ROW_KIND_COLUMN);
    for (int row = 0; row < rows; row++) {
      RowKind kind = kinds == null ? RowKind.INSERT : RowKind.fromByteValue(kinds.get(row));
      StreamRecord<RowData> outputRow = new StreamRecord<>(views.row(row, kind));
      if (element.hasTimestamp()) {
        outputRow.setTimestamp(element.getTimestamp());
      }
      output.collect(outputRow);
    }
  }
}
