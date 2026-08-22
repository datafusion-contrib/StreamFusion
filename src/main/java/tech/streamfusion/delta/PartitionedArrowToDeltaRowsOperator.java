package tech.streamfusion.delta;

import io.delta.flink.sink.Conversions;
import io.delta.kernel.types.StructType;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import tech.streamfusion.operator.PartitionedArrowBatch;

/** Converts partition-routed Arrow batches into retained Delta row views. */
public final class PartitionedArrowToDeltaRowsOperator
    extends AbstractStreamOperator<RowData>
    implements OneInputStreamOperator<PartitionedArrowBatch, RowData> {
  private final RowType rowType;
  private transient StructType deltaSchema;

  public PartitionedArrowToDeltaRowsOperator(RowType rowType) {
    this.rowType = rowType;
  }

  @Override
  public void open() throws Exception {
    super.open();
    deltaSchema = Conversions.FlinkToDelta.schema(rowType);
  }

  @Override
  public void processElement(StreamRecord<PartitionedArrowBatch> element) {
    VectorSchemaRoot root = element.getValue().root();
    int rows = root.getRowCount();
    if (rows == 0) {
      root.close();
      return;
    }
    ArrowKernelRows views = new ArrowKernelRows(root, rowType, deltaSchema);
    StreamRecord<RowData> outputBatch = new StreamRecord<>(views);
    if (element.hasTimestamp()) {
      outputBatch.setTimestamp(element.getTimestamp());
    }
    output.collect(outputBatch);
  }
}
